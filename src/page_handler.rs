// an abstraction for all page fetching thingies for ops
use std::sync::{Arc,RwLock};
use crate::page_cache::{PageCache,PageCacheEntryCompressed,PageCacheEntryUncompressed};
use crate::page_io::PageIO;
use crate::compressor::Compressor;
use crate::metadata_store::PageMetadata;

// so this thingy gets a bunch of page metas and we just have to serve them
pub struct PageHandler {
    pub page_io: Arc<PageIO>,
    pub uncompressed_page_cache: Arc<RwLock<PageCache::<PageCacheEntryUncompressed>>>, // UPC
    pub compressed_page_cache: Arc<RwLock<PageCache::<PageCacheEntryCompressed>>>, // CPC
    pub compressor: Arc<Compressor>,
}

impl PageHandler {
    fn fetch_from_upc(&self, id: &str) -> Result<Arc<PageCacheEntryUncompressed>, &'static str> {
        // get a read lock on UPC, return early if missing
        let read_lock = self.uncompressed_page_cache.read().unwrap();
        if let Some(entry) = read_lock.store.get(id) {
            return Ok(Arc::clone(&entry.page));
        }
        Err("not found in UPC")
    }

    // decompresses and puts into UPC
    fn decompress_from_cpc(&self, id: &str) -> Result<(), &'static str>{
        let read_lock = self.compressed_page_cache.read().unwrap();
        let dat = match read_lock.store.get(id) {
            Some(e) => Arc::clone(&e.page),
            None => return Err("not found in CPC"),
        };
        drop(read_lock);

        let uncompressed = self.compressor.decompress(dat);

        let mut write_lock = self.uncompressed_page_cache.write().unwrap();
        write_lock.add(id, uncompressed);
        Ok(())
    }

    fn fetch_from_fs(&self, id: &str, path: &str, offset: u64) -> Result<(), &'static str> {
        // uses page_io to fetch from fs
        let compressed: PageCacheEntryCompressed = self.page_io.read_from_path(path, offset);
        let mut cpc = self.compressed_page_cache.write().unwrap();
        cpc.add(id, compressed);
        Ok(())
    }

    pub fn get_page(&self, page_meta: PageMetadata) -> Option<Arc<PageCacheEntryUncompressed>> {
        // uses the above internal stuffs
        let id = page_meta.id.clone();

        // try UPC first
        if let Ok(hit) = self.fetch_from_upc(&id) {
            return Some(hit);
        }

        // try CPC then decompress
        if self.decompress_from_cpc(&id).is_ok() {
            if let Ok(hit) = self.fetch_from_upc(&id) {
                return Some(hit);
            }
        }

        // fetch from FS, insert into CPC, then decompress and return
        if self.fetch_from_fs(&id, &page_meta.disk_path, page_meta.offset).is_ok() {
            if self.decompress_from_cpc(&id).is_ok() {
                if let Ok(hit) = self.fetch_from_upc(&id) {
                    return Some(hit);
                }
            }
        }
        None
    }
}