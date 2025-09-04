// an abstraction for all page fetching thingies for ops
use std::sync::{Arc,RwLock};
use crate::page_cache::{PageCache,PageCacheEntryCompressed,PageCacheEntryUncompressed};

// so this thingy gets a bunch of page metas and we just have to serve them
pub struct PageHandler {
    pub uncompressed_page_cache: Arc<RwLock<PageCache::<PageCacheEntryUncompressed>>>, // UPC
    pub compressed_page_cache: Arc<RwLock<PageCache::<PageCacheEntryCompressed>>>, // CPC
}

impl PageHandler {
    fn fetch_from_UPC(&self) {

    }

    fn decompress_from_CPC(&self) {

    }

    fn fetch_from_fs(&self) {

    }

    pub fn get_page(&self) {
        // check in UPC

        // check in CPC
    }
}