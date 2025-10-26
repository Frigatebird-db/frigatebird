pub mod page_io;

use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::helpers::compressor::Compressor;
use crate::metadata_store::{PageDescriptor, PageDirectory};
use crate::page_handler::page_io::PageIO;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct PageLocator {
    directory: Arc<PageDirectory>,
}

impl PageLocator {
    pub fn new(directory: Arc<PageDirectory>) -> Self {
        Self { directory }
    }

    pub fn latest_for_column(&self, column: &str) -> Option<PageDescriptor> {
        self.directory.latest(column)
    }

    pub fn range_for_column(
        &self,
        column: &str,
        l_bound: u64,
        r_bound: u64,
        commit_time_upper_bound: u64,
    ) -> Vec<PageDescriptor> {
        self.directory
            .range(column, l_bound, r_bound, commit_time_upper_bound)
    }

    pub fn lookup(&self, id: &str) -> Option<PageDescriptor> {
        self.directory.lookup(id)
    }
}

pub struct PageFetcher {
    compressed_page_cache: Arc<RwLock<PageCache<PageCacheEntryCompressed>>>,
    page_io: Arc<PageIO>,
}

impl PageFetcher {
    pub fn new(
        compressed_page_cache: Arc<RwLock<PageCache<PageCacheEntryCompressed>>>,
        page_io: Arc<PageIO>,
    ) -> Self {
        Self {
            compressed_page_cache,
            page_io,
        }
    }

    pub fn get_cached(&self, id: &str) -> Option<Arc<PageCacheEntryCompressed>> {
        let cache = self.compressed_page_cache.read().ok()?;
        cache.store.get(id).map(|entry| Arc::clone(&entry.page))
    }

    pub fn collect_cached(&self, order: &[String]) -> Vec<(String, Arc<PageCacheEntryCompressed>)> {
        let cache = match self.compressed_page_cache.read() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        order
            .iter()
            .filter_map(|id| {
                cache
                    .store
                    .get(id)
                    .map(|entry| (id.clone(), Arc::clone(&entry.page)))
            })
            .collect()
    }

    pub fn fetch_and_insert(&self, meta: &PageDescriptor) -> Arc<PageCacheEntryCompressed> {
        if let Some(hit) = self.get_cached(&meta.id) {
            return hit;
        }

        let compressed = self.page_io.read_from_path(&meta.disk_path, meta.offset);
        {
            let mut cache = self.compressed_page_cache.write().unwrap();
            cache.add(&meta.id, compressed);
        }

        self.get_cached(&meta.id)
            .expect("compressed page inserted but not found")
    }
}

pub struct PageMaterializer {
    uncompressed_page_cache: Arc<RwLock<PageCache<PageCacheEntryUncompressed>>>,
    compressor: Arc<Compressor>,
}

impl PageMaterializer {
    pub fn new(
        uncompressed_page_cache: Arc<RwLock<PageCache<PageCacheEntryUncompressed>>>,
        compressor: Arc<Compressor>,
    ) -> Self {
        Self {
            uncompressed_page_cache,
            compressor,
        }
    }

    pub fn get_cached(&self, id: &str) -> Option<Arc<PageCacheEntryUncompressed>> {
        let cache = self.uncompressed_page_cache.read().ok()?;
        cache.store.get(id).map(|entry| Arc::clone(&entry.page))
    }

    pub fn collect_cached(
        &self,
        order: &[String],
    ) -> Vec<(String, Arc<PageCacheEntryUncompressed>)> {
        let cache = match self.uncompressed_page_cache.read() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        order
            .iter()
            .filter_map(|id| {
                cache
                    .store
                    .get(id)
                    .map(|entry| (id.clone(), Arc::clone(&entry.page)))
            })
            .collect()
    }

    pub fn write_back(&self, id: &str, page: PageCacheEntryUncompressed) {
        let mut cache = self.uncompressed_page_cache.write().unwrap();
        cache.add(id, page);
    }

    pub fn materialize_one(
        &self,
        id: &str,
        compressed: Arc<PageCacheEntryCompressed>,
    ) -> Option<Arc<PageCacheEntryUncompressed>> {
        let page = self.compressor.decompress(compressed);
        {
            let mut cache = self.uncompressed_page_cache.write().unwrap();
            cache.add(id, page);
        }
        self.get_cached(id)
    }

    pub fn materialize_many(
        &self,
        items: Vec<(String, Arc<PageCacheEntryCompressed>)>,
    ) -> Vec<(String, Arc<PageCacheEntryUncompressed>)> {
        if items.is_empty() {
            return Vec::new();
        }

        let mut materialized: Vec<(String, PageCacheEntryUncompressed)> = Vec::new();
        for (id, comp) in items.into_iter() {
            let page = self.compressor.decompress(comp);
            materialized.push((id, page));
        }

        let mut cache = self.uncompressed_page_cache.write().unwrap();
        let mut result = Vec::new();
        for (id, page) in materialized.into_iter() {
            cache.add(&id, page);
            if let Some(entry) = cache.store.get(&id) {
                result.push((id.clone(), Arc::clone(&entry.page)));
            }
        }
        result
    }
}

pub struct PageHandler {
    locator: Arc<PageLocator>,
    fetcher: Arc<PageFetcher>,
    materializer: Arc<PageMaterializer>,
}

impl PageHandler {
    pub fn new(
        locator: Arc<PageLocator>,
        fetcher: Arc<PageFetcher>,
        materializer: Arc<PageMaterializer>,
    ) -> Self {
        Self {
            locator,
            fetcher,
            materializer,
        }
    }

    pub fn locate_latest(&self, column: &str) -> Option<PageDescriptor> {
        self.locator.latest_for_column(column)
    }

    pub fn locate_range(
        &self,
        column: &str,
        l_bound: u64,
        r_bound: u64,
        commit_time_upper_bound: u64,
    ) -> Vec<PageDescriptor> {
        self.locator
            .range_for_column(column, l_bound, r_bound, commit_time_upper_bound)
    }

    pub fn get_page(&self, page_meta: PageDescriptor) -> Option<Arc<PageCacheEntryUncompressed>> {
        let id = page_meta.id.clone();

        if let Some(hit) = self.materializer.get_cached(&id) {
            return Some(hit);
        }

        if let Some(comp_hit) = self.fetcher.get_cached(&id) {
            return self.materializer.materialize_one(&id, comp_hit);
        }

        let comp = self.fetcher.fetch_and_insert(&page_meta);
        self.materializer.materialize_one(&id, comp)
    }

    pub fn get_pages(
        &self,
        page_metas: Vec<PageDescriptor>,
    ) -> Vec<Arc<PageCacheEntryUncompressed>> {
        if page_metas.is_empty() {
            return Vec::new();
        }

        let order: Vec<String> = page_metas.iter().map(|m| m.id.clone()).collect();
        let mut meta_map: HashMap<String, PageDescriptor> =
            page_metas.into_iter().map(|m| (m.id.clone(), m)).collect();

        let mut result: Vec<Arc<PageCacheEntryUncompressed>> = Vec::new();
        let mut already_pushed: HashSet<String> = HashSet::new();

        for (id, entry) in self.materializer.collect_cached(&order) {
            result.push(entry);
            already_pushed.insert(id.clone());
            meta_map.remove(&id);
        }

        if meta_map.is_empty() {
            return result;
        }

        let mut to_materialize: Vec<(String, Arc<PageCacheEntryCompressed>)> = Vec::new();
        for (id, comp) in self.fetcher.collect_cached(&order) {
            if already_pushed.contains(&id) {
                continue;
            }
            to_materialize.push((id.clone(), comp));
            meta_map.remove(&id);
        }
        self.materializer.materialize_many(to_materialize);

        if !meta_map.is_empty() {
            let metas: Vec<PageDescriptor> = meta_map.values().cloned().collect();
            let mut fetched: Vec<(String, Arc<PageCacheEntryCompressed>)> = Vec::new();
            for meta in metas.iter() {
                let comp = self.fetcher.fetch_and_insert(meta);
                fetched.push((meta.id.clone(), comp));
            }
            self.materializer.materialize_many(fetched);
        }

        for (id, entry) in self.materializer.collect_cached(&order) {
            if already_pushed.insert(id.clone()) {
                result.push(entry);
            }
        }

        result
    }

    pub fn write_back_uncompressed(&self, id: &str, page: PageCacheEntryUncompressed) {
        self.materializer.write_back(id, page);
    }
}
