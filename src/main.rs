use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
use crate::page_cache::{PageCache,PageCacheEntryCompressed,PageCacheEntryUncompressed};
use crate::page_handler::PageHandler;
mod entry;
mod page_io;
mod metadata_store;
mod ops_handler;
mod page_cache;
mod page_handler;
use page_cache::PageCacheEntry;
mod page;
mod compressor;
use metadata_store::TableMetaStore;
use std::sync::{Arc,RwLock};

fn main() -> io::Result<()> {
    let compressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let uncompressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let metadata_store = Arc::new(RwLock::new(TableMetaStore::new())) ;

    let page_handler = PageHandler{uncompressed_page_cache: Arc::clone(&uncompressed_page_cache),compressed_page_cache: Arc::clone(&compressed_page_cache) };
    
    // cleanup
    drop(uncompressed_page_cache);
    drop(compressed_page_cache);

    Ok(())
}


























