use crate::cache::page_cache::PageCacheEntryCompressed;
#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};
#[cfg(target_os = "linux")]
use libc;
use rkyv::ser::{Serializer, serializers::AllocSerializer};
use rkyv::{Archive, Deserialize, Serialize};
use std::fs::File;
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::io;
#[cfg(not(target_os = "linux"))]
use std::io::Read;
use std::io::{Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

pub struct IOHandler {}

const PREFIX_META_SIZE: usize = 64;

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive(check_bytes)]
struct Metadata {
    read_size: usize,
}

pub struct PageIO {}

impl PageIO {
    pub fn read_from_path(&self, path: &str, offset: u64) -> PageCacheEntryCompressed {
        self.read_batch_from_path(path, &[offset])
            .expect("batch read must succeed")
            .into_iter()
            .next()
            .expect("batch must return exactly one page")
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

        let mut meta_buffers = Vec::with_capacity(offsets.len());
        let mut lengths = vec![0usize; offsets.len()];

        for (idx, offset) in offsets.iter().enumerate() {
            let mut buffer = vec![0u8; PREFIX_META_SIZE];
            let entry = opcode::Read::new(fd, buffer.as_mut_ptr(), PREFIX_META_SIZE as u32)
                .offset(*offset)
                .build()
                .user_data(idx as u64);
            submit_entry(&mut ring, entry)?;
            meta_buffers.push(buffer);
        }

        wait_for(&mut ring, offsets.len(), |idx, res| {
            ensure_result(res)?;
            lengths[idx] = read_size_from_meta(&meta_buffers[idx]);
            Ok(())
        })?;

        let mut results: Vec<Option<PageCacheEntryCompressed>> = vec![None; offsets.len()];
        let mut data_buffers = Vec::with_capacity(offsets.len());
        let mut submitted = 0usize;

        for (idx, len) in lengths.iter().enumerate() {
            if *len == 0 {
                results[idx] = Some(PageCacheEntryCompressed { page: Vec::new() });
                data_buffers.push(Vec::new());
                continue;
            }

            let mut buffer = vec![0u8; *len];
            let data_offset = offsets[idx] + PREFIX_META_SIZE as u64;
            let entry = opcode::Read::new(fd, buffer.as_mut_ptr(), *len as u32)
                .offset(data_offset)
                .build()
                .user_data(idx as u64);
            submit_entry(&mut ring, entry)?;
            data_buffers.push(buffer);
            submitted += 1;
        }

        if submitted > 0 {
            wait_for(&mut ring, submitted, |idx, res| {
                ensure_result(res)?;
                let page = std::mem::take(&mut data_buffers[idx]);
                results[idx] = Some(PageCacheEntryCompressed { page });
                Ok(())
            })?;
        }

        Ok(results
            .into_iter()
            .map(|entry| entry.unwrap_or(PageCacheEntryCompressed { page: Vec::new() }))
            .collect())
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
        let mut fd = open_direct_writer(path)?;

        fd.seek(SeekFrom::Start(offset))?;

        // Create metadata
        let new_meta = Metadata {
            read_size: data.len(),
        };

        // Serialize metadata with rkyv - MUCH faster than JSON
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&new_meta).unwrap();
        let meta_bytes = serializer.into_serializer().into_inner();

        // Pad to exact size
        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
        meta_buffer[..meta_bytes.len()].copy_from_slice(&meta_bytes);

        // Combine and write in one syscall
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(&data);

        fd.write_all(&combined)?;
        fd.sync_all()?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub fn write_batch_to_path(&self, path: &str, writes: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if writes.is_empty() {
            return Ok(());
        }

        let file = open_direct_writer(path)?;
        let fd = types::Fd(file.as_raw_fd());
        let mut ring = IoUring::new(writes.len() as u32)?;

        // Prepare all buffers upfront (io_uring needs stable pointers)
        let mut buffers = Vec::with_capacity(writes.len());
        for (_, data) in writes.iter() {
            let new_meta = Metadata {
                read_size: data.len(),
            };

            let mut serializer = AllocSerializer::<256>::default();
            serializer.serialize_value(&new_meta).unwrap();
            let meta_bytes = serializer.into_serializer().into_inner();

            let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];
            meta_buffer[..meta_bytes.len()].copy_from_slice(&meta_bytes);

            let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len());
            combined.extend_from_slice(&meta_buffer);
            combined.extend_from_slice(data);

            buffers.push(combined);
        }

        // Submit all writes
        for (idx, (offset, _)) in writes.iter().enumerate() {
            let buffer = &buffers[idx];
            let entry = opcode::Write::new(fd, buffer.as_ptr(), buffer.len() as u32)
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
        for (offset, data) in writes.iter() {
            self.write_to_path(path, *offset, data.clone())?;
        }
        Ok(())
    }
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
    let archived = unsafe { rkyv::archived_root::<Metadata>(buf) };
    archived.read_size as usize
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
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

#[cfg(not(target_os = "linux"))]
fn open_direct_reader(path: &str) -> io::Result<File> {
    File::open(path)
}

#[cfg(not(target_os = "linux"))]
fn open_direct_writer(path: &str) -> io::Result<File> {
    File::create(path)
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
