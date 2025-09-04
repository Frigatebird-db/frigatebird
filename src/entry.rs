/*
It only matters for the start of the query/txn to the end of it
*/
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

pub fn current_epoch_millis() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .expect("Time went backwards")
       .as_millis() as u64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    prefix_meta: String,
    data: String,
    suffix_meta: String,
}

impl Entry {
    pub fn new(data: &str) -> Self {
        Entry {
            prefix_meta: "".to_string(),
            data: data.to_string(),
            suffix_meta: "".to_string(),
        }
    }
}

struct Link {
    commit_time: u64,
    entry: Entry,
    locked_by: u64,
}

impl Link {
    fn new(entry: Entry) -> Self {
        Link { commit_time: 69696969, entry: entry, locked_by: 0 }
    }

    fn lock() {
        // increase the locked_by count in a threadsafe way
    }

    fn unlock() {
        // decrease count
    }
}

// stores and deals with the Entry Chains
// an Entry id is nothing but: (col_name::row_idx)
// Entry chains are nothing but: (col_name::row_idx::commit_time)
pub struct EntryKeeper{
    store: VecDeque<Link>
}

impl EntryKeeper{
    fn new() -> Self {
        EntryKeeper { store: VecDeque::new() }
    }

    // adds a new link to the chain
    fn add_link(&mut self, entry: Entry) {
        self.store.push_back(Link::new(entry));

        // we need update the latest state of this stuff without blocking any reads for it there
    }

    /*
    remove a link when:
    - there is another link right after it
    - when the time has passed so you can be certain with the fact that none of the upcoming queries would be considering it
    - the link isnt locked by any running query
    
    note that links are always sorted by created_at
    */

    // tries to remove the first link
    fn rem_link(&mut self) {
        if self.store.len() < 2 {
            return
        }
        if current_epoch_millis() >  self.store[0].commit_time && self.store[0].locked_by == 0 {
            self.store.pop_front();
        }
    }

}