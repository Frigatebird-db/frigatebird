use crate::entry::current_epoch_millis;
use crate::page::Page;
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
const LRU_SIZE: usize = 10;

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
    pub page: Page,
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
        if self.store.contains_key(id) {
            let entry = self.store.get(id);
            self.lru_queue
                .remove(&((entry.unwrap()).used_time, String::from(id)));
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
