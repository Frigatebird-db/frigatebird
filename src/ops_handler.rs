// these are our external API contracts and shouldnt change btw, whatever you change internally, these should just work out of the box

use std::sync::{Arc, RwLock};
use crate::entry::Entry;
use crate::page_handler::PageHandler;
use crate::metadata_store::{TableMetaStore, PageMetadata};

// TODO: we also have to update the (l,r) ranges whenever we upsert something into it
pub fn upsert_data_into_column(meta_store: &Arc<RwLock<TableMetaStore>>, handler: &PageHandler, col: &str, data: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let page_meta: PageMetadata = {
        let guard = meta_store.read().unwrap();
        guard.get_latest_page_meta(col).unwrap().as_ref().clone()
    };

    let page_arc = handler.get_page(page_meta.clone()).unwrap();

    let mut updated = (*page_arc).clone();
    updated.page.add_entry(Entry::new(data));

    let mut upc_write = handler.uncompressed_page_cache.write().unwrap();
    upc_write.add(&page_meta.id, updated);

    Ok(true)
}

pub fn update_column_entry(meta_store: &Arc<RwLock<TableMetaStore>>, handler: &PageHandler, col: &str, data: &str, row: u64) -> Result<bool, Box<dyn std::error::Error>> {
    let page_meta: PageMetadata = {
        let guard = meta_store.read().unwrap();
        guard.get_latest_page_meta(col).unwrap().as_ref().clone()
    };

    let page_arc = handler.get_page(page_meta.clone()).unwrap();

    let mut updated = (*page_arc).clone();
    if (row as usize) >= updated.page.entries.len() { return Ok(false); }
    updated.page.entries[row as usize] = Entry::new(data);

    let mut upc_write = handler.uncompressed_page_cache.write().unwrap();
    upc_write.add(&page_meta.id, updated);

    Ok(true)
}


pub fn range_scan_column_entry(meta_store: &Arc<RwLock<TableMetaStore>>, handler: &PageHandler, col: &str, l_row: u64, r_row: u64, commit_time_upper_bound: u64) -> Vec<Entry> {
    let response = {
        let guard = meta_store.read().unwrap();
        guard.get_ranged_pages_meta(col, l_row, r_row, commit_time_upper_bound).unwrap()
    };

    let pages = handler.get_pages(response.page_metas.iter().map(|m| (**m).clone()).collect());

    let mut out: Vec<Entry> = Vec::new();
    // Without per-page bounds, we return full pages in order for now.
    for page_arc in pages.into_iter() {
        out.extend_from_slice(&page_arc.page.entries);
    }

    out
}

fn range_scan_columns_entries() {
    // honestly, the columns are pretty decoupled already

    // I doubt there is any shared contention or coordination thingy here, its just doing a bunch of independent stuff fast
}