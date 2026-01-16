use crate::entry;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Page {
    pub page_metadata: String,
    pub entries: Vec<entry::Entry>,
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

impl Page {
    pub fn new() -> Self {
        Self {
            page_metadata: "".to_string(),
            entries: vec![],
        }
    }

    pub fn add_entry(&mut self, entry: entry::Entry) {
        self.entries.push(entry);
    }
}
