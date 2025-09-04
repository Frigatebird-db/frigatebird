use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
use crate::page_cache::{PageCache,PageCacheEntryCompressed,PageCacheEntryUncompressed};
use crate::page_handler::PageHandler;
mod entry;
mod page_io;
use page_io::PageIO;
mod metadata_store;
mod ops_handler;
mod page_cache;
mod page_handler;
use page_cache::PageCacheEntry;
mod page;
mod compressor;
use metadata_store::TableMetaStore;
use std::sync::{Arc,RwLock};
use compressor::Compressor;
fn main() {
    let compressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let uncompressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let page_io = Arc::new(PageIO{});
    let metadata_store = Arc::new(RwLock::new(TableMetaStore::new())) ;
    let compressor = Arc::new(Compressor::new());
    let page_handler = PageHandler{page_io: Arc::clone(&page_io),uncompressed_page_cache: Arc::clone(&uncompressed_page_cache),compressed_page_cache: Arc::clone(&compressed_page_cache), compressor: Arc::clone(&compressor)};




    // cleanup
    drop(uncompressed_page_cache);
    drop(compressed_page_cache);
    drop(page_io);
    drop(compressor);
}


























