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
use std::sync::{Arc,RwLock};

use crate::entry::current_epoch_millis;


// this will be immutable throughout its lifetime btw
#[derive(Clone)]
pub struct PageMetadata {
    pub id: String, // this is page id btw
    pub disk_path: String,
    pub offset: u64, // where to find the compressed page in that path
}

pub struct MVCCKeeperEntry {
    pub page_id: String,
    pub locked_by: u8, // this needs to be atomic counter btw, todo I guess
    pub commit_time: u64,
}

pub struct TableMetaStoreEntry {
    pub start_idx: u64,
    pub end_idx: u64,
    pub page_metas: Vec<MVCCKeeperEntry>
}

pub struct RangeScanMetaResponse{
    pub page_metas: Vec<Arc<PageMetadata>>
}

/* 

M[page_id]  ->      [  () , ()... , |~~>(disk_path,offset) , () , ()...  ]
                                    |
M[col_name] -> [                    |
                                    | 
                    (l0,r0) -> [Page_id_at_x < Page_id_at_y < Page_id_at_z....],
                    (l1,r1) -> ...
                    (l2,r2)
                    (l3,r3)
                    ...
                    ...
                ] 


should I introduce another map or... wait, I wanted that huh, we can do that now I think..

its all so fragmented man, ughh

*/

// a lot can read multiple column metas and page metas at once with RLock on wrapper

/*
with Rlock we want something like:
- Rlock the TableMetaStoreWrapper, get the column/page meta you wish from Map, clone it and RELEASE LOCK ASAP

okay, so what exactly are we cloning here ? also note that TableMetaStore is a wrapper in itself, just a gateway, just grab a clone and get out, check later if you can do something with it
*/

// we query this thing to grab page and column metas, THATS IT
pub struct TableMetaStore {
    col_data: HashMap<String,Arc<RwLock<Vec<TableMetaStoreEntry>>>>, // this just keeps absolute minimal page meta references
    page_data: HashMap<String,Arc<PageMetadata>> // this keeps the actual page metadata, and owns the ARCs
}

impl PageMetadata {
    // also returns an id maybe ??
    fn new(disk_path: String,offset: u64) -> Self {
        Self {
            id: "1111111".to_string(), // todo: use a real rand id gen here good ser
            disk_path: disk_path,
            offset: offset,
        }
    }
}

impl MVCCKeeperEntry {
    fn new(id: String) -> Self {
        Self {
            page_id: id,
            commit_time: current_epoch_millis(),
            locked_by: 0,
        }
    }
}

impl Drop for PageMetadata {
    fn drop(&mut self) {
        // so we do a bunch of stuff here

        // so if a page meta is being dropped, we don't want to:
        // - have it on disk
        // - have it in any cache(both compressed and uncompressed)
        // so yeah, todo I guess AHAHAHHAH
    }
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


// just returns ARCS, nothing else concerns it, try not to take locks in there
impl TableMetaStore {
    pub fn new() -> Self {
        Self {
            col_data: HashMap::new(),
            page_data: HashMap::new()
        }
    }

    pub fn get_page_path_and_offset(&self, id: &str) -> Option<Arc<PageMetadata>>{
        let entry_arc = Arc::clone(&self.page_data.get(id).unwrap());
        // let path = &entry.disk_path;
        // let offset = &entry.offset;
        // just make a clone and return huh
        Some(entry_arc)
    }

    // its here, but try to use it btw
    pub fn get_latest_page_meta(&self, column: &str) -> Option<&Arc<PageMetadata>> {
        self.col_data
            .get(column)?
            .read()
            .ok()?
            .last()?
            .page_metas
            .last()
            .and_then(|mvcc_entry| self.page_data.get(&mvcc_entry.page_id))
    }

    // returns the new generated page id
    fn add_new_page_meta(&mut self,disk_path: String, offset: u64) -> String {
        let meta_object = PageMetadata::new(disk_path,offset);
        let id = meta_object.id.clone();
        self.page_data.insert(id.clone(),Arc::new(meta_object));
        // now meta object is stored in clouds meta store AHAHAH, no ownership bullos from here, will be freed whenever it gets freed by the laws of physics and inevitability of flowing time
        id
    }

    fn add_new_page_to_col(&mut self, col: String, disk_path: String, offset: u64) {
        let page_id = self.add_new_page_meta(disk_path, offset);
        
        if !self.col_data.contains_key(&col) {
            self.col_data.insert(col.clone(), Arc::new(RwLock::new(Vec::new())));
        }
        
        let mut col_guard = self.col_data.get(&col).unwrap().write().unwrap();
        
        if col_guard.is_empty() {
            col_guard.push(TableMetaStoreEntry {
                start_idx: 0,
                end_idx: 1,
                page_metas: vec![MVCCKeeperEntry::new(page_id)],
            });
        } else {
            let last_entry = col_guard.last_mut().unwrap();
            last_entry.end_idx += 1;
            last_entry.page_metas.push(MVCCKeeperEntry::new(page_id));
        }
    }

    pub fn get_ranged_pages_meta(&self, col: &str, l_bound: u64, r_bound: u64, commit_time_upper_bound: u64) -> Option<RangeScanMetaResponse> {
        // okay, so we gotta either binary search or just get it linearly if not too many elements ? oh wait, do we need locks ? we dont ig
        if l_bound > r_bound {
            return Some(RangeScanMetaResponse { page_metas: Vec::new() });
        }

        // Acquire read lock for the column entries, binary search for overlap window, copy page_ids, and drop the lock ASAP
        // this is the longest place where we are holding column meta read guard btw
        let col_guard = self.col_data.get(col)?.read().ok()?;

        let entries = &*col_guard;
        let len = entries.len();
        let mut page_ids: Vec<String> = Vec::new();

        if len > 0 {
            let mut lo: usize = 0;
            let mut hi: usize = len;
            while lo < hi {
                let mid = (lo + hi) / 2;
                if entries[mid].end_idx <= l_bound {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let mut i = lo;

            // Linear scan forward until start_idx >= r_bound
            while i < len {
                let entry = &entries[i];
                if entry.start_idx >= r_bound { break; }
                // pick MVCC version with commit_time just before/at the upper bound
                if !entry.page_metas.is_empty() {
                    let versions = &entry.page_metas;
                    // binary search for first commit_time > commit_time_upper_bound
                    let mut vlo: usize = 0;
                    let mut vhi: usize = versions.len();
                    while vlo < vhi {
                        let vmid = (vlo + vhi) / 2;
                        if versions[vmid].commit_time <= commit_time_upper_bound {
                            vlo = vmid + 1;
                        } else {
                            vhi = vmid;
                        }
                    }
                    if vlo > 0 { // there exists a version with commit_time <= commit_time_upper_bound
                        let chosen = &versions[vlo - 1];
                        page_ids.push(chosen.page_id.clone());
                    }
                }
                i += 1;
            }
        }
        drop(col_guard);

        let mut metas: Vec<Arc<PageMetadata>> = Vec::new();
        for page_id in page_ids {
            if let Some(meta_arc) = self.page_data.get(&page_id) {
                metas.push(Arc::clone(meta_arc));
            }
        }

        Some(RangeScanMetaResponse { page_metas: metas })
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
