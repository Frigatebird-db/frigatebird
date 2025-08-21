/*
this things keeps track of 'where on disk' the compressed pages of a certain table lies

we would also need to keep track of MVCC stuff here

lets kinda accept the fact that: 'contagious pages cant be kept together every single time' , atleast
not without giving away write performance and worst case massive disk movements

I think the best we can do for the columnar compressed pages updates is to just do the 'best effort' of just storing them durably(wherever we can on the disk at that time) when they come
and just round them up together(on disk) during compactions

okay, so what should the metadata store structure look like, it needs to:
    - keep track of where the compressed Pages are for a particular column
    - should support keeping track of multiple version of them, if there are a lot of versions of a certain page, we must prioritize that
    after compaction - atleast the latest versions of pages are physically kept close sequentially
*/
use std::collections::HashMap;

struct PageMetadata {
    locked_by: u8,
    commit_time: u64, // when it came
    disk_path: String,
    offset: u64, // where to find the compressed page in that path
}

struct TableMetaStoreEntry {
    start_idx: u64,
    end_idx: u64,
    page_metas: Vec<PageMetadata>
}

impl TableMetaStoreEntry {
    fn new(start_idx: u64, end_idx: u64) -> Self {
        Self {
            start_idx: start_idx, end_idx: end_idx, page_metas: vec![]
        }
    }

    fn copied(&self) {
        // returns a copy ??? hoe ?
        // TODO
    }
}
struct TableMetaStore {
    data: HashMap<String,Vec<TableMetaStoreEntry>>
}

impl TableMetaStore {
    fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    fn get_latest_page_meta(&self, column: &str) -> Option<&TableMetaStoreEntry> {
        self.data.get(column)?.last()
    }
}

fn append_to_column(tableMetaStore: TableMetaStore, column: &str, data: &str) -> Option<()>{
    // find out the current page from table meta store
    let pageOffset = tableMetaStore.get_latest_page_meta(column);
    
    // create entry


    // check if that page has enough space to accomodate the entry

    // if not, create a new page

    if true {
        // creating a new page
        // add an empty entry
    }

    None
}
