pub mod page_io;

use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::helpers::compressor::Compressor;
use crate::metadata_store::{PageDescriptor, PageDirectory};
use crate::page_handler::page_io::PageIO;
use crossbeam::channel::{self, Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

const PREFETCH_POLL_INTERVAL_MS: u64 = 1;

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
    pub(crate) page_io: Arc<PageIO>,
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

    pub fn fetch_and_insert_batch(
        &self,
        metas: &[PageDescriptor],
    ) -> Vec<(String, Arc<PageCacheEntryCompressed>)> {
        if metas.is_empty() {
            return Vec::new();
        }

        // Group by disk path for batch reads
        let mut by_path: HashMap<String, Vec<(usize, &PageDescriptor)>> = HashMap::new();
        for (idx, meta) in metas.iter().enumerate() {
            by_path
                .entry(meta.disk_path.clone())
                .or_insert_with(Vec::new)
                .push((idx, meta));
        }

        let mut results: Vec<Option<(String, PageCacheEntryCompressed)>> = vec![None; metas.len()];

        // Batch read per unique path
        for (path, items) in by_path.into_iter() {
            let offsets: Vec<u64> = items.iter().map(|(_, m)| m.offset).collect();
            let pages = self
                .page_io
                .read_batch_from_path(&path, &offsets)
                .expect("batch read must succeed");

            for ((idx, meta), page) in items.into_iter().zip(pages.into_iter()) {
                results[idx] = Some((meta.id.clone(), page));
            }
        }

        // Single write lock to insert all
        {
            let mut cache = self.compressed_page_cache.write().unwrap();
            for result in results.iter().flatten() {
                cache.add(&result.0, result.1.clone());
            }
        }

        // Collect Arc references
        results
            .into_iter()
            .filter_map(|r| r.and_then(|(id, _)| self.get_cached(&id).map(|arc| (id, arc))))
            .collect()
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
    prefetch_tx: Sender<Vec<String>>,
    _prefetch_thread: thread::JoinHandle<()>,
}

impl PageHandler {
    pub fn new(
        locator: Arc<PageLocator>,
        fetcher: Arc<PageFetcher>,
        materializer: Arc<PageMaterializer>,
    ) -> Self {
        let (prefetch_tx, prefetch_rx) = channel::unbounded::<Vec<String>>();

        let locator_clone = Arc::clone(&locator);
        let fetcher_clone = Arc::clone(&fetcher);
        let materializer_clone = Arc::clone(&materializer);

        let prefetch_thread = thread::spawn(move || {
            run_prefetch_loop(
                prefetch_rx,
                locator_clone,
                fetcher_clone,
                materializer_clone,
            );
        });

        Self {
            locator,
            fetcher,
            materializer,
            prefetch_tx,
            _prefetch_thread: prefetch_thread,
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
            let fetched = self.fetcher.fetch_and_insert_batch(&metas);
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

    pub fn flush_to_disk(&self, ids: &[String]) -> Result<(), std::io::Error> {
        if ids.is_empty() {
            return Ok(());
        }

        // Collect compressed pages from CPC
        let cached = self.fetcher.collect_cached(ids);
        if cached.is_empty() {
            return Ok(());
        }

        // Group by disk path and lookup metadata
        let mut by_path: HashMap<String, Vec<(u64, Vec<u8>)>> = HashMap::new();
        for (id, compressed_page) in cached.into_iter() {
            if let Some(meta) = self.locator.lookup(&id) {
                by_path
                    .entry(meta.disk_path)
                    .or_insert_with(Vec::new)
                    .push((meta.offset, compressed_page.page.to_vec()));
            }
        }

        // Batch write per unique path
        for (path, writes) in by_path.into_iter() {
            self.fetcher.page_io.write_batch_to_path(&path, &writes)?;
        }

        Ok(())
    }

    pub fn ensure_pages_cached(&self, ids: &[String]) {
        if ids.is_empty() {
            return;
        }

        let mut missing = Vec::new();
        for id in ids {
            if self.materializer.get_cached(id).is_none() && self.fetcher.get_cached(id).is_none() {
                missing.push(id.clone());
            }
        }

        if missing.is_empty() {
            return;
        }

        let descriptors: Vec<PageDescriptor> = missing
            .iter()
            .filter_map(|id| self.locator.lookup(id))
            .collect();

        if !descriptors.is_empty() {
            let _ = self.get_pages(descriptors);
        }
    }

    pub fn get_pages_with_prefetch(
        &self,
        page_ids: &[String],
        k: usize,
    ) -> Vec<Arc<PageCacheEntryUncompressed>> {
        if page_ids.is_empty() {
            return Vec::new();
        }

        let split = k.min(page_ids.len());

        // Non-blocking prefetch send for remaining pages
        if split < page_ids.len() {
            let _ = self.prefetch_tx.send(page_ids[split..].to_vec());
        }

        // Blocking immediate fetch for first k pages
        let descriptors: Vec<PageDescriptor> = page_ids[..split]
            .iter()
            .filter_map(|id| self.locator.lookup(id))
            .collect();

        self.get_pages(descriptors)
    }
}

fn run_prefetch_loop(
    rx: Receiver<Vec<String>>,
    locator: Arc<PageLocator>,
    fetcher: Arc<PageFetcher>,
    materializer: Arc<PageMaterializer>,
) {
    let poll_interval = Duration::from_millis(PREFETCH_POLL_INTERVAL_MS);

    loop {
        match rx.recv_timeout(poll_interval) {
            Ok(batch) => {
                let mut accumulated = batch;

                // Drain channel immediately to batch pending requests
                while let Ok(more) = rx.try_recv() {
                    accumulated.extend(more);
                }

                process_prefetch_batch(&accumulated, &locator, &fetcher, &materializer);
            }
            Err(channel::RecvTimeoutError::Timeout) => continue,
            Err(channel::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn process_prefetch_batch(
    ids: &[String],
    locator: &Arc<PageLocator>,
    fetcher: &Arc<PageFetcher>,
    materializer: &Arc<PageMaterializer>,
) {
    if ids.is_empty() {
        return;
    }

    // Dedupe
    let unique: HashSet<String> = ids.iter().cloned().collect();

    // Filter already cached (UPC first, then CPC)
    let mut missing = Vec::new();
    for id in unique {
        if materializer.get_cached(&id).is_none() && fetcher.get_cached(&id).is_none() {
            missing.push(id);
        }
    }

    if missing.is_empty() {
        return;
    }

    // Lookup metadata
    let descriptors: Vec<PageDescriptor> =
        missing.iter().filter_map(|id| locator.lookup(id)).collect();

    if descriptors.is_empty() {
        return;
    }

    // Batch fetch compressed pages
    let compressed = fetcher.fetch_and_insert_batch(&descriptors);

    // Batch decompress into UPC
    if !compressed.is_empty() {
        materializer.materialize_many(compressed);
    }
}
