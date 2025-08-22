use crate::entry_keeper;

pub struct Page {
    page_metadata: String,
    entries: Vec<entry_keeper::Entry>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            page_metadata: "".to_string(),
            entries: vec![],
        }
    }

    pub fn add_entry(&mut self, entry: entry_keeper::Entry) {
        // what does it means to add an entry in a page
        self.entries.push(entry);

        // now its done in memory, we need to do it on disk as well
    }
}
