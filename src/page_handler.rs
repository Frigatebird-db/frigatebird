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

    pub fn get_pages(&self, page_metas: Vec<PageMetadata>) -> Vec<Arc<PageCacheEntryUncompressed>> {
        // so the thing is, here we get a bunch of page metas, and we gotta fetch stuff real quick with minimal lock contention as possible
        
        // holy fuck man, in the current state all of those would be an individual call, :skull: , why can't we..

        // okay, lets try be greedy

        /*
        we clone the darn page_metas for easier use , call it needed_page_metas

        first we raid the UPC, grab all the pages we can and remove them from needed_page_metas list

        then we raid CPC, grab all from there

        then the darn fs, grab the left ones from there

        holy fuck, this is a lot of ops

        we need something, something such that we can very quickly figure out what is in the darn cache and what is not

        wait, wait, its just fast, we just need read lock huh, yeah, yeah, fast


        so I think we can and should do it in one read lock, remember the p50 vs p99 thingy huh, yeah, one lock, read lock
        */

        use std::collections::{HashMap, HashSet};

        // Preserve original order
        let order: Vec<String> = page_metas.iter().map(|m| m.id.clone()).collect();

        // id -> meta
        let mut meta_map: HashMap<String, PageMetadata> = HashMap::new();
        for m in page_metas.into_iter() {
            meta_map.insert(m.id.clone(), m);
        }

        let mut result: Vec<Arc<PageCacheEntryUncompressed>> = Vec::new();
        let mut already_pushed: HashSet<String> = HashSet::new();

        // 1) UPC raid in a single read lock, push immediately in order
        {
            let upc_read = self.uncompressed_page_cache.read().unwrap();
            for id in order.iter() {
                if let Some(entry) = upc_read.store.get(id) {
                    result.push(Arc::clone(&entry.page));
                    already_pushed.insert(id.clone());
                    meta_map.remove(id);
                }
            }
        }
        if meta_map.is_empty() { return result; }

        // 2) CPC raid: collect compressed hits to decompress outside lock
        let mut to_decompress: Vec<(String, Arc<PageCacheEntryCompressed>)> = Vec::new();
        {
            let cpc_read = self.compressed_page_cache.read().unwrap();
            for id in order.iter() {
                if already_pushed.contains(id) { continue; }
                if let Some(entry) = cpc_read.store.get(id) {
                    to_decompress.push((id.clone(), Arc::clone(&entry.page)));
                    meta_map.remove(id);
                }
            }
        }

        // Decompress CPC hits and add to UPC
        for (id, comp_arc) in to_decompress.into_iter() {
            let uncompressed = self.compressor.decompress(comp_arc);
            let mut upc_write = self.uncompressed_page_cache.write().unwrap();
            upc_write.add(&id, uncompressed);
        }

        // 3) Fetch remaining from FS, then decompress from CPC
        for (_id, meta) in meta_map.iter() {
            if self.fetch_from_fs(&meta.id, &meta.disk_path, meta.offset).is_ok() {
                let _ = self.decompress_from_cpc(&meta.id);
            }
        }

        // 4) Final UPC read to collect all pages in original order that weren't pushed yet
        let upc_read_final = self.uncompressed_page_cache.read().unwrap();
        for id in order.iter() {
            if already_pushed.contains(id) { continue; }
            if let Some(entry) = upc_read_final.store.get(id) {
                result.push(Arc::clone(&entry.page));
            }
        }

        result
    }
}