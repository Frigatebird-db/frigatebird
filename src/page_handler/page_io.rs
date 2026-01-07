use crate::cache::page_cache::PageCacheEntryCompressed;
#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};
#[cfg(target_os = "linux")]
use libc;
use rkyv::ser::{Serializer, serializers::AllocSerializer};
use rkyv::{Archive, Deserialize, Serialize};
use std::alloc::{Layout, alloc, dealloc};
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io;
#[cfg(not(target_os = "linux"))]
use std::io::Read;
use std::io::{Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::ptr;

pub struct IOHandler {}

const PREFIX_META_SIZE: usize = 64;
const ALIGNMENT: usize = 4096;

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
struct Metadata {
    read_size: usize,
}

struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    capacity: usize,
}

impl AlignedBuffer {
    fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 { ALIGNMENT } else { capacity };
        // Ensure capacity is a multiple of alignment for O_DIRECT length requirements
        let aligned_capacity = (capacity + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;

        let layout = Layout::from_size_align(aligned_capacity, ALIGNMENT).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Memory allocation failed");
        }
        // Zero initialize to avoid leaking data in padding
        unsafe { ptr::write_bytes(ptr, 0, aligned_capacity) };

        Self {
            ptr,
            layout,
            capacity: aligned_capacity,
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.capacity) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

pub struct PageIO {}

impl PageIO {
    pub fn read_from_path(&self, path: &str, offset: u64) -> io::Result<PageCacheEntryCompressed> {
        self.read_batch_from_path(path, &[offset])?
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing page data"))
    }

    #[cfg(target_os = "linux")]
    pub fn read_batch_from_path(
        &self,
        path: &str,
        offsets: &[u64],
    ) -> io::Result<Vec<PageCacheEntryCompressed>> {
        if offsets.is_empty() {
            return Ok(Vec::new());
        }

        let file = open_direct_reader(path)?;
        let fd = types::Fd(file.as_raw_fd());
        let queue_entries = (offsets.len() * 2).max(1) as u32;
        let mut ring = IoUring::new(queue_entries)?;

        // Phase 1: Read first block (contains metadata + start of data)
        let mut first_blocks = Vec::with_capacity(offsets.len());

        for (idx, offset) in offsets.iter().enumerate() {
            let mut buffer = AlignedBuffer::new(ALIGNMENT);
            let entry = opcode::Read::new(fd, buffer.ptr, ALIGNMENT as u32)
                .offset(*offset)
                .build()
                .user_data(idx as u64);
            submit_entry(&mut ring, entry)?;
            first_blocks.push(buffer);
        }

        let mut read_sizes = vec![0usize; offsets.len()];

        wait_for(&mut ring, offsets.len(), |idx, res| {
            ensure_result(res)?;
            read_sizes[idx] = read_size_from_meta(first_blocks[idx].as_slice());
            Ok(())
        })?;

        // Phase 2: Read remaining data if any
        let mut extra_buffers: Vec<Option<AlignedBuffer>> = Vec::with_capacity(offsets.len());
        for _ in 0..offsets.len() {
            extra_buffers.push(None);
        }

        let mut submitted_extra = 0usize;

        for (idx, &data_len) in read_sizes.iter().enumerate() {
            if data_len == 0 {
                continue;
            }

            let total_len = PREFIX_META_SIZE + data_len;
            if total_len > ALIGNMENT {
                // Need to read more
                let remaining_len = total_len - ALIGNMENT;
                let mut buffer = AlignedBuffer::new(remaining_len);
                let read_offset = offsets[idx] + ALIGNMENT as u64;

                let entry = opcode::Read::new(fd, buffer.ptr, buffer.capacity as u32)
                    .offset(read_offset)
                    .build()
                    .user_data(idx as u64);
                submit_entry(&mut ring, entry)?;
                extra_buffers[idx] = Some(buffer);
                submitted_extra += 1;
            }
        }

        if submitted_extra > 0 {
            wait_for(&mut ring, submitted_extra, |_idx, res| {
                ensure_result(res)?;
                Ok(())
            })?;
        }

        // Assemble results
        let mut results = Vec::with_capacity(offsets.len());
        for (idx, &data_len) in read_sizes.iter().enumerate() {
            if data_len == 0 {
                results.push(PageCacheEntryCompressed { page: Vec::new() });
                continue;
            }

            let mut page_data = Vec::with_capacity(data_len);

            // Copy data from first block (skipping metadata)
            let first_block_slice = first_blocks[idx].as_slice();
            let available_in_first = (ALIGNMENT - PREFIX_META_SIZE).min(data_len);
            page_data.extend_from_slice(
                &first_block_slice[PREFIX_META_SIZE..PREFIX_META_SIZE + available_in_first],
            );

            // Copy from extra buffer if needed
            if let Some(extra_buf) = &extra_buffers[idx] {
                let remaining = data_len - available_in_first;
                page_data.extend_from_slice(&extra_buf.as_slice()[..remaining]);
            }

            results.push(PageCacheEntryCompressed { page: page_data });
        }

        Ok(results)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn read_batch_from_path(
        &self,
        path: &str,
        offsets: &[u64],
    ) -> io::Result<Vec<PageCacheEntryCompressed>> {
        let mut fd = File::open(path)?;
        offsets
            .iter()
            .map(|offset| read_single_sync(&mut fd, *offset))
            .collect()
    }

    pub fn write_to_path(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), io::Error> {
        ensure_parent_dir(path)?;
        let mut fd = open_direct_writer(path)?;

        fd.seek(SeekFrom::Start(offset))?;

        // Create metadata
        let new_meta = Metadata {
            read_size: data.len(),
        };

        // Serialize metadata with rkyv
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&new_meta).unwrap();
        let meta_bytes = serializer.into_serializer().into_inner();

        let total_size = PREFIX_META_SIZE + data.len();
        let mut buffer = AlignedBuffer::new(total_size);
        let slice = buffer.as_slice_mut();

        // Write metadata
        slice[..meta_bytes.len()].copy_from_slice(&meta_bytes);
        // Write data
        slice[PREFIX_META_SIZE..PREFIX_META_SIZE + data.len()].copy_from_slice(&data);

        // Write the aligned buffer
        // Note: as_slice_mut().len() is aligned to 4096, which satisfies O_DIRECT
        fd.write_all(buffer.as_slice())?;
        fd.sync_all()?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub fn write_batch_to_path(&self, path: &str, writes: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if writes.is_empty() {
            return Ok(());
        }

        ensure_parent_dir(path)?;
        let file = open_direct_writer(path)?;
        let fd = types::Fd(file.as_raw_fd());
        let mut ring = IoUring::new(writes.len() as u32)?;

        // Prepare all buffers upfront
        let mut buffers = Vec::with_capacity(writes.len());
        for (_, data) in writes.iter() {
            let new_meta = Metadata {
                read_size: data.len(),
            };

            let mut serializer = AllocSerializer::<256>::default();
            serializer.serialize_value(&new_meta).unwrap();
            let meta_bytes = serializer.into_serializer().into_inner();

            let total_size = PREFIX_META_SIZE + data.len();
            let mut buffer = AlignedBuffer::new(total_size);
            let slice = buffer.as_slice_mut();

            slice[..meta_bytes.len()].copy_from_slice(&meta_bytes);
            slice[PREFIX_META_SIZE..PREFIX_META_SIZE + data.len()].copy_from_slice(data);

            buffers.push(buffer);
        }

        // Submit all writes
        for (idx, (offset, _)) in writes.iter().enumerate() {
            let buffer = &buffers[idx];
            let entry = opcode::Write::new(fd, buffer.ptr, buffer.capacity as u32)
                .offset(*offset)
                .build()
                .user_data(idx as u64);
            submit_entry(&mut ring, entry)?;
        }

        // Wait for all completions
        wait_for(&mut ring, writes.len(), |_idx, res| {
            ensure_result(res)?;
            Ok(())
        })?;

        // Single fsync after all writes
        let sync_entry = opcode::Fsync::new(fd).build().user_data(u64::MAX);
        submit_entry(&mut ring, sync_entry)?;
        ring.submit_and_wait(1)?;
        let mut cq = ring.completion();
        if let Some(cqe) = cq.next() {
            ensure_result(cqe.result())?;
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn write_batch_to_path(&self, path: &str, writes: &[(u64, Vec<u8>)]) -> io::Result<()> {
        ensure_parent_dir(path)?;
        for (offset, data) in writes.iter() {
            self.write_to_path(path, *offset, data.clone())?;
        }
        Ok(())
    }
}

fn ensure_parent_dir(path: &str) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn read_single_sync(fd: &mut File, offset: u64) -> io::Result<PageCacheEntryCompressed> {
    fd.seek(SeekFrom::Start(offset))?;

    let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
    fd.read_exact(&mut meta_buffer)?;
    let actual_size = read_size_from_meta(&meta_buffer);

    let mut ret_buffer = vec![0; actual_size];
    fd.read_exact(&mut ret_buffer)?;

    Ok(PageCacheEntryCompressed { page: ret_buffer })
}

fn read_size_from_meta(buf: &[u8]) -> usize {
    const SIZE_BYTES: usize = std::mem::size_of::<usize>();
    if buf.len() < SIZE_BYTES {
        return 0;
    }
    let mut raw = [0u8; SIZE_BYTES];
    raw.copy_from_slice(&buf[..SIZE_BYTES]);
    usize::from_le_bytes(raw)
}

#[cfg(target_os = "linux")]
fn ensure_result(res: i32) -> io::Result<()> {
    if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn submit_entry(ring: &mut IoUring, entry: io_uring::squeue::Entry) -> io::Result<()> {
    loop {
        match unsafe { ring.submission().push(&entry) } {
            Ok(()) => return Ok(()),
            Err(_) => {
                ring.submit()?;
                continue;
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn wait_for<F>(ring: &mut IoUring, expected: usize, mut on_complete: F) -> io::Result<()>
where
    F: FnMut(usize, i32) -> io::Result<()>,
{
    let mut processed = 0usize;
    while processed < expected {
        ring.submit_and_wait(1)?;
        let mut cq = ring.completion();
        while let Some(cqe) = cq.next() {
            processed += 1;
            let idx = cqe.user_data() as usize;
            on_complete(idx, cqe.result())?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_direct_reader(path: &str) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

#[cfg(target_os = "linux")]
fn open_direct_writer(path: &str) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

#[cfg(not(target_os = "linux"))]
fn open_direct_reader(path: &str) -> io::Result<File> {
    File::open(path)
}

#[cfg(not(target_os = "linux"))]
fn open_direct_writer(path: &str) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{open_direct_reader, open_direct_writer};
    use std::env;
    use std::fs::{File, remove_file};
    use std::os::fd::AsRawFd;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file(name: &str) -> PathBuf {
        let mut path = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("pageio-{name}-{nanos}"));
        path
    }

    #[test]
    fn reader_uses_o_direct_flag() {
        let path = temp_file("read");
        File::create(&path).unwrap();
        let file = open_direct_reader(path.to_str().unwrap()).unwrap();
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags & libc::O_DIRECT, 0, "reader must set O_DIRECT");
        drop(file);
        let _ = remove_file(&path);
    }

    #[test]
    fn writer_uses_o_direct_flag() {
        let path = temp_file("write");
        let file = open_direct_writer(path.to_str().unwrap()).unwrap();
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags & libc::O_DIRECT, 0, "writer must set O_DIRECT");
        drop(file);
        let _ = remove_file(&path);
    }
}
