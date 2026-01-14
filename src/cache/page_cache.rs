use crate::entry::current_epoch_millis;
use crate::page::Page;
use crate::sql::runtime::batch::ColumnarPage;
use crate::sql::types::DataType;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

// user space page cache

// a dequeue of Pages(which are nothing but a set of entries)
// fixed sized because of limits
// we should be able to access a page from its ID or something

/*

wait a second, do we need to flush a page to disk that we know is an old version ??

set with (used_id,id)
---

store[id] -> PageCacheEntry
Set((used_time,id),()...)

create when adding, remove when removing
*/
// Keep enough pages resident while the persistence pipeline is still evolving.
const LRU_SIZE: usize = 1024;

pub trait CacheLifecycle<T>: Send + Sync {
    fn on_evict(&self, id: &str, data: Arc<T>);
}

#[derive(Clone)]
pub struct PageCacheEntry<T> {
    pub page: Arc<T>,
    pub used_time: u64,
}

#[derive(Clone)]
pub struct PageCacheEntryUncompressed {
    pub page: ColumnarPage,
}

impl PageCacheEntryUncompressed {
    pub fn from_disk_page(page: Page, dtype: DataType) -> Self {
        Self {
            page: ColumnarPage::load(page, dtype),
        }
    }

    pub fn as_disk_page(&self) -> Page {
        self.page.as_disk_page()
    }

    pub fn replace_with_disk_page(&mut self, page: Page, dtype: DataType) {
        self.page.replace_with_disk_page(page, dtype);
    }

    pub fn mutate_disk_page<F>(&mut self, mutator: F, dtype: DataType)
    where
        F: FnOnce(&mut Page),
    {
        let mut page = self.as_disk_page();
        mutator(&mut page);
        self.replace_with_disk_page(page, dtype);
    }
}

#[derive(Clone)]
pub struct PageCacheEntryCompressed {
    pub page: Vec<u8>, // a bunch of raw bytes that we read from the disk
}

impl Drop for PageCacheEntryUncompressed {
    fn drop(&mut self) {
        // okay, so we need to compress it and insert into Compressed page cache ?

        // how ? ownership and stuff ?

        // how to get Compressed page cache context over here ??
    }
}

impl Drop for PageCacheEntryCompressed {
    fn drop(&mut self) {
        // when this goes out of scope, we just flush it to disk with direct IO
    }
}

pub struct PageCache<T> {
    pub store: HashMap<String, PageCacheEntry<T>>,
    pub lru_queue: BTreeSet<(u64, String)>,
    lifecycle: Option<Arc<dyn CacheLifecycle<T>>>,
}

impl<T> PageCache<T> {
    pub fn new() -> Self {
        PageCache {
            store: HashMap::new(),
            lru_queue: BTreeSet::new(),
            lifecycle: None,
        }
    }

    pub fn capacity_limit() -> usize {
        LRU_SIZE
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn with_lifecycle(lifecycle: Option<Arc<dyn CacheLifecycle<T>>>) -> Self {
        PageCache {
            store: HashMap::new(),
            lru_queue: BTreeSet::new(),
            lifecycle,
        }
    }

    pub fn set_lifecycle(&mut self, lifecycle: Option<Arc<dyn CacheLifecycle<T>>>) {
        self.lifecycle = lifecycle;
    }

    pub fn add(&mut self, id: &str, page: T) {
        if let Some(entry) = self.store.get(id) {
            self.lru_queue.remove(&(entry.used_time, String::from(id)));
        }

        let used_time = current_epoch_millis();

        let entry = PageCacheEntry {
            page: Arc::new(page),
            used_time: used_time,
        };

        self.store.insert(id.to_string(), entry);

        self.lru_queue.insert((used_time, id.to_string()));
        if self.lru_queue.len() > LRU_SIZE {
            let (_oldest_time, oldest_id) = self.lru_queue.iter().next().unwrap().clone();
            self.evict(&oldest_id);
        }
    }

    pub fn has(&self, id: &str) -> bool {
        self.store.contains_key(id)
    }

    pub fn get(&self, id: &str) -> Option<Arc<T>> {
        self.store.get(id).map(|entry| Arc::clone(&entry.page))
    }

    pub fn evict(&mut self, id: &str) {
        if let Some(entry) = self.store.remove(id) {
            self.lru_queue.remove(&(entry.used_time, id.to_string()));
            if let Some(lifecycle) = &self.lifecycle {
                lifecycle.on_evict(id, Arc::clone(&entry.page));
            }
        }
    }
}
