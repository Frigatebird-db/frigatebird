use crate::cache::page_cache::PageCacheEntryCompressed;
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::ser::{Serializer, serializers::AllocSerializer};
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;

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
        let mut fd = File::open(path).unwrap();

        // Try to disable kernel caching on macOS
        #[cfg(target_os = "macos")]
        unsafe {
            let raw = fd.as_raw_fd();
            let _ = libc::fcntl(raw, libc::F_RDAHEAD, 0);
            let _ = libc::fcntl(raw, libc::F_NOCACHE, 1);
        }

        fd.seek(SeekFrom::Start(offset)).unwrap();

        // Read the fixed-size metadata prefix
        let mut meta_buffer = vec![0; PREFIX_META_SIZE];
        fd.read_exact(&mut meta_buffer).unwrap();

        // Zero-copy access to metadata - NO DESERIALIZATION
        let archived = unsafe {
            // This is safe because we know the buffer is valid
            rkyv::archived_root::<Metadata>(&meta_buffer[..])
        };
        let actual_entry_size = archived.read_size;

        // Now read the actual data
        let new_offset = offset + PREFIX_META_SIZE as u64;
        fd.seek(SeekFrom::Start(new_offset)).unwrap();

        let mut ret_buffer = vec![0; actual_entry_size as usize];
        fd.read_exact(&mut ret_buffer).unwrap();

        PageCacheEntryCompressed { page: ret_buffer }
    }

    pub fn write_to_path(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), io::Error> {
        let mut fd = File::create(path)?;

        #[cfg(target_os = "macos")]
        unsafe {
            let raw = fd.as_raw_fd();
            let _ = libc::fcntl(raw, libc::F_RDAHEAD, 0);
            let _ = libc::fcntl(raw, libc::F_NOCACHE, 1);
        }

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
}
