use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::page_handler::{page_io::PageIO, PageFetcher, PageHandler, PageLocator, PageMaterializer};
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
mod cache;
mod entry;
mod helpers;
mod metadata_store;
mod ops_handler;
mod page;
mod page_handler;
mod pool;
use cache::lifecycle::{CompressedToDiskLifecycle, UncompressedToCompressedLifecycle};
use helpers::compressor::Compressor;
use metadata_store::{PageDirectory, TableMetaStore};
use std::sync::{Arc, RwLock};

fn main() {
    let compressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let uncompressed_page_cache =
        Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let page_io = Arc::new(PageIO {});
    let metadata_store = Arc::new(RwLock::new(TableMetaStore::new()));
    let page_directory = Arc::new(PageDirectory::new(Arc::clone(&metadata_store)));
    let compressor = Arc::new(Compressor::new());

    {
        let lifecycle = Arc::new(UncompressedToCompressedLifecycle::new(
            Arc::clone(&compressor),
            Arc::clone(&compressed_page_cache),
        ));
        let mut upc = uncompressed_page_cache.write().unwrap();
        upc.set_lifecycle(Some(lifecycle));
    }

    {
        let lifecycle = Arc::new(CompressedToDiskLifecycle::new(
            Arc::clone(&page_io),
            Arc::clone(&page_directory),
        ));
        let mut cpc = compressed_page_cache.write().unwrap();
        cpc.set_lifecycle(Some(lifecycle));
    }

    let locator = Arc::new(PageLocator::new(Arc::clone(&page_directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_page_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_page_cache),
        Arc::clone(&compressor),
    ));
    let page_handler = PageHandler::new(locator, fetcher, materializer);

    // cleanup
    drop(uncompressed_page_cache);
    drop(compressed_page_cache);
    drop(page_io);
    drop(compressor);
}
