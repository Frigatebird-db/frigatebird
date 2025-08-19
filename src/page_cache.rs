use std::collections::BTreeSet;

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
struct PageCacheEntry {
    page: Page,
    used_time: u64
}

pub struct PageCache{
    store: HashMap<String,PageCacheEntry>,
    lru_queue: BTreeSet<(u64,String)>
}

impl PageCache {
    fn new() -> Self {
        PageCache{ store: HashMap.new() , lru_queue: BTreeSet.new()}
    }

    // adds a certain Page to mem
    fn add(&mut self,id: String, page: Page) {
        // check if an entry already exists
        if self.store.contains_key(id) {
            // remove it from set
            let entry = self.store.get(id);
            self.lru_queue.remove(&(entry.used_time,id));
        }

        // make an new entry
        let entry = PageCacheEntry{page: page,used_time: current_epoch_millis()};
        self.store[id].set(entry);
        self.lru_queue.insert((entry.used_time,id));
    }
    
}