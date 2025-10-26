use crate::entry;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Page {
    pub page_metadata: String,
    pub entries: Vec<entry::Entry>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            page_metadata: "".to_string(),
            entries: vec![],
        }
    }

    pub fn add_entry(&mut self, entry: entry::Entry) {
        // what does it means to add an entry in a page
        self.entries.push(entry);

        // now its done in memory, we need to do it on disk as well
    }
}
