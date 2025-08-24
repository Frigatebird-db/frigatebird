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


currently we store stuff as:

# Table metadata store

M[col_name] -> [(),()...]
                 |
                 ~->{start_row_idx,end_row_idx,PageMetadata: [(),()...]}
                                                              | // we store multiple versions of Pages for MVCC stuff
                                                              ~-> {id,locked_by_cnt,commit_time,disk_path,offset} 

we should also do:
M[page_id] -> I mean, we should just do this shit like: {id,locked_by_cnt,commit_time,disk_path,offset}

and just keep page_id like a foreign key in M[col_name] shit instead of keeping it all there, would also be faster to just get it in just O(x)
once than to go through a lot of O(x) + O(x).... nested stuff every single time
*/
use std::collections::HashMap;

use crate::page::Page;
use crate::context::Context;
use crate::page_cache::CombinedCache;

pub struct PageMetadata {
    pub id: String, // this is page id btw
    pub locked_by: u8,
    pub commit_time: u64, // when it came
    pub disk_path: String,
    pub offset: u64, // where to find the compressed page in that path
}

pub struct TableMetaStoreEntry {
    pub start_idx: u64,
    pub end_idx: u64,
    pub page_metas: Vec<String> // todo: change this shit to just Vec<String> as we are storing page metadata separately in meta store now
}

pub struct TableMetaStore {
    // M[col_name] -> [(),()..]
    col_data: HashMap<String,Vec<TableMetaStoreEntry>>,
    page_data: HashMap<String,PageMetadata>
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


impl TableMetaStore {
    fn new() -> Self {
        Self {
            col_data: HashMap::new(),
            page_data: HashMap::new()
        }
    }


    pub fn get_page_path_and_offset(&self, id: &str) -> Option<(String,u64)>{
        let entry: &PageMetadata = self.page_data.get(id).unwrap();
        let path = &entry.disk_path;
        let offset = &entry.offset;
        
        Some((path.to_string(),*offset))
    }

    pub fn get_latest_page_meta(&self, column: &str) -> Option<&PageMetadata> {
        let whatever = self.col_data.get(column)?.last().unwrap();
        let bruh = whatever.page_metas.last().unwrap();
        self.page_data.get(bruh)
    }
}

// // how the hell do I get the PageCache context here lmao
// fn append_to_column(context: Context, tableMetaStore: TableMetaStore, column: &str, data: &str) -> Option<()>{
//     // find out the current page from table meta store
//     let latest_page_meta = tableMetaStore.get_latest_page_meta(column).unwrap().page_metas.last().unwrap();


//     // most probably need to add some abstraction for below stuff

//     if context.cache.uncompressed_pages.has(&latest_page_meta) {
//         // todo
//         // update
//     } else if context.cache.compressed_pages.has(&latest_page_meta){
//         // todo
//         // decompress page
//         // pull into uncompressed pages
//         // update
//     } else {
//         // do IO shit
//         // pull into compressed pages
//         // pull into decompressed pages
//     }

//     if true {
//         let new_page = Page::new();

//         // creating a new page
//         // add an empty entry
//     }

//     None
// }
