use std::sync::{Arc, RwLock};

use crate::cache::page_cache::{
    CacheLifecycle, PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use crate::helpers::compressor::Compressor;
use crate::metadata_store::PageDirectory;
use crate::page_handler::page_io::PageIO;

pub struct UncompressedToCompressedLifecycle {
    compressor: Arc<Compressor>,
    compressed_cache: Arc<RwLock<PageCache<PageCacheEntryCompressed>>>,
}

impl UncompressedToCompressedLifecycle {
    pub fn new(
        compressor: Arc<Compressor>,
        compressed_cache: Arc<RwLock<PageCache<PageCacheEntryCompressed>>>,
    ) -> Self {
        Self {
            compressor,
            compressed_cache,
        }
    }
}

impl CacheLifecycle<PageCacheEntryUncompressed> for UncompressedToCompressedLifecycle {
    fn on_evict(&self, id: &str, data: Arc<PageCacheEntryUncompressed>) {
        let compressed = self.compressor.compress(Arc::clone(&data));
        if let Ok(mut cache) = self.compressed_cache.write() {
            cache.add(id, compressed);
        }
    }
}

pub struct CompressedToDiskLifecycle {
    page_io: Arc<PageIO>,
    directory: Arc<PageDirectory>,
}

impl CompressedToDiskLifecycle {
    pub fn new(page_io: Arc<PageIO>, directory: Arc<PageDirectory>) -> Self {
        Self { page_io, directory }
    }
}

impl CacheLifecycle<PageCacheEntryCompressed> for CompressedToDiskLifecycle {
    fn on_evict(&self, id: &str, data: Arc<PageCacheEntryCompressed>) {
        if let Some(meta) = self.directory.lookup(id) {
            let _ = self
                .page_io
                .write_to_path(&meta.disk_path, meta.offset, data.page.clone());
        }
    }
}
