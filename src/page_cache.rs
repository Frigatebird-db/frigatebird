use std::collections::BTreeSet;
use std::collections::HashMap;
use crate::page;
use crate::entry_keeper::current_epoch_millis;
// user space page cache

// a dequeue of Pages(which are nothing but a set of entries)
// fixed sized because of limits 
// we should be able to access a page from its ID or something 

/*

set with (used_id,id)
---

store[id] -> PageCacheEntry
Set((used_time,id),()...)

create when adding, remove when removing
*/
const LRUsize:usize = 10;

struct PageCacheEntry<T> {
    page: T,
    used_time: u64
}

pub struct PageCacheEntryUncompressed {
    page: page::Page,
}

pub struct PageCacheEntryCompressed {
    page: Vec<u8>, // a bunch of raw bytes that we read from the disk
}


pub struct PageCache<T>{
    pub store: HashMap<String,PageCacheEntry<T>>,
    pub lru_queue: BTreeSet<(u64,String)>
}

impl<T> PageCache<T> {
    pub fn new() -> Self {
        PageCache{ store: HashMap::new() , lru_queue: BTreeSet::new()}
    }

    // adds a certain Page to mem
    pub fn add(&mut self,id: &str, page: T) {
        // check if an entry already exists
        if self.store.contains_key(id) {
            // remove it from set
            let entry = self.store.get(id);
            // let wut = entry.unwrap().used_time;
            self.lru_queue.remove(&((entry.unwrap()).used_time,String::from(id)));
        }

        // make an new entry
        let used_time = current_epoch_millis();
        
        let entry = PageCacheEntry{page: page,used_time: used_time};

        self.store.insert(id.to_string(),entry);

        self.lru_queue.insert((used_time,id.to_string()));
        if self.lru_queue.len() > LRUsize {
            let (oldest_time, oldest_id) = self.lru_queue.iter().next().unwrap().clone();
            self.evict(&oldest_id);
        }
    }

    pub fn has(&self, id: &str) -> bool {
        self.store.contains_key(id)
    }

    // so this returns a reference to the entry
    pub fn get(&self, id: &str) -> Option<&PageCacheEntry<T>> {
        // this is more complex btw
        self.store.get(id)
    }

    pub fn evict(&mut self, id: &str) {
        // remove from lru_queue
        self.lru_queue.remove(&(self.store.get(id).unwrap().used_time, id.to_string()));
        self.store.remove(id);
    }

}

pub struct CombinedCache {
    pub compressed_pages: PageCache<PageCacheEntryCompressed>,
    pub uncompressed_pages: PageCache<PageCacheEntryUncompressed>,
}

impl CombinedCache {
    pub fn new() -> Self {
        Self {compressed_pages: PageCache::new() , uncompressed_pages: PageCache::new()}
    }
}