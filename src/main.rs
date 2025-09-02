use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
use crate::metadata_store::TableMetaStoreWrapper;
use crate::page_cache::{PageCacheEntryCompressed,PageCacheEntryUncompressed};
mod entry;
mod page_io;
mod metadata_store;
mod ops_handler;
mod page_cache;
use page_cache::PageCacheWrapper;
mod page;
mod context;
mod compressor;

// makes a new page and returns its offset
fn make_new_page_at_EOF() -> io::Result<(u64)>{
    // make a new page at EOF
    
    // return the offset
    Ok((69))
}


fn do_shit_to_file(data: &[u8], fd: &mut File) -> io::Result<String> {
    // append data to that file dumbly
    
    // seek to the bottom of the file
    fd.seek(SeekFrom::End(0))?;
    fd.write_all(data)?;

    // append data to it
    Ok("UwU".to_string())

}

fn main() -> io::Result<()> {
    let compressed_page_cache = PageCacheWrapper::<PageCacheEntryCompressed>::new();
    let uncompressed_page_cache = PageCacheWrapper::<PageCacheEntryUncompressed>::new();
    let metadata_store: TableMetaStoreWrapper = TableMetaStoreWrapper::new();

    // so the above stuffs are our contexts, and we'll use dependency injection to pass them around
    Ok(())
}

/*
just write stuff to a file

just make a function which does this:

takes a file descriptor
appends shit to the file it belongs to, that's it


*/






























