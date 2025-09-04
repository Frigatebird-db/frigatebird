use std::io;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use serde::Deserialize;
use serde::Serialize;

/*
so on disk, our Entries are just compressed pages


we use atmax 1mb compressed pages

we never get kernel optmizations involved for IO, we have our own user space page cache
*/

pub struct IOHandler {}

const PREFIX_META_SIZE: usize = 2048;

// impl IOHandler {
//     fn new() -> Self {
//         IOHandler {}
//     }

//     fn read_from_path(&self, path: String, offset: u64) -> Vec<u8> {
//         // Read data from the specified path and offset and returns raw bytes
//         let fd = File::open(path);
//         fd.seek(SeekFrom::Start(offset));

//         // read the prefix meta from the offset, get the exact size to read from the offset, then read that whole thing and return
//         let mut buffer = vec![0; PREFIX_META_SIZE as usize];
//         fd.read_exact(&mut buffer);
//         buffer
//     }

//     fn write_to_path(&self, path: String, offset: u64, data: Vec<u8>) -> bool {
//         // just dumbly write to that path and offset
//         let fd = File::create(path);
//         fd.seek(SeekFrom::Start(offset));
//         fd.write_all(&data);
//         true
//     }
// }

/*
this thing is responsible for flushing and fetching pages from disk
*/

#[derive(Deserialize)]
#[derive(Serialize)]
struct Metadata {
    read_size: usize
}

pub struct PageIO {}

fn deserialize_metadata(buffer: Vec<u8>) -> Metadata {
    // Deserialize the buffer into a Metadata struct
    let json = String::from_utf8_lossy(&buffer);
    serde_json::from_str(&json).unwrap()
}

impl PageIO {
    pub fn read_from_path(&self, path: &str, offset: u64) -> Vec<u8> {
        // Read data from the specified path and offset and returns raw bytes

        // we are opening new FDs here btw, try to keep an FD pool and stuff...
        let mut fd = File::open(path).unwrap();
        fd.seek(SeekFrom::Start(offset)).unwrap();

        // read the prefix meta from the offset, get the exact size to read from the offset, then read that whole thing and return
        let mut buffer = vec![0; PREFIX_META_SIZE as usize];
        fd.read_exact(&mut buffer).unwrap();

        let new_offset = offset + PREFIX_META_SIZE as u64;
        fd.seek(SeekFrom::Start(new_offset)).unwrap();
        let meta = deserialize_metadata(buffer);
        let actual_entry_size = meta.read_size;

        let mut ret_buffer = vec![0; actual_entry_size as usize];
        fd.read_exact(&mut ret_buffer).unwrap();

        ret_buffer
        // now, that buffer is just the raw bytes containing the serialized metadata for the entry
        // we need to deserialize it and read it, for now, let's just keep it human readable json
    }

    pub fn write_to_path(&self, path: &str, offset: u64, data: Vec<u8>) -> Result<(), io::Error> {
        let mut fd = File::create(path)?;
        fd.seek(SeekFrom::Start(offset))?;
        
        // Create metadata
        let data_size = data.len();
        let new_meta = Metadata { read_size: data_size };
        
        // Serialize and pad metadata to exactly PREFIX_META_SIZE
        let meta_json = serde_json::to_string(&new_meta).unwrap();
        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE as usize];
        let meta_bytes = meta_json.as_bytes();
        meta_buffer[..meta_bytes.len()].copy_from_slice(meta_bytes);
        
        // Combine metadata + data in memory
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE as usize + data.len());
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(&data);
        
        // ONE write syscall for everything
        fd.write_all(&combined)?;
        
        Ok(())
    }
}