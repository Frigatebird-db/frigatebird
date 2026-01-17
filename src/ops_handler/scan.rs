use crate::entry::Entry;
use crate::metadata_store::{DEFAULT_TABLE, PageDescriptor};
use crate::page_handler::PageHandler;

pub fn range_scan_column_entry(
    handler: &PageHandler,
    col: &str,
    l_row: u64,
    r_row: u64,
    commit_time_upper_bound: u64,
) -> Vec<Entry> {
    range_scan_table_column_entry(
        handler,
        DEFAULT_TABLE,
        col,
        l_row,
        r_row,
        commit_time_upper_bound,
    )
}

pub fn range_scan_table_column_entry(
    handler: &PageHandler,
    table: &str,
    col: &str,
    l_row: u64,
    r_row: u64,
    _commit_time_upper_bound: u64,
) -> Vec<Entry> {
    let slices = handler.list_range_in_table(table, col, l_row, r_row);
    if slices.is_empty() {
        return Vec::new();
    }

    let descriptors: Vec<PageDescriptor> = slices
        .iter()
        .map(|slice| slice.descriptor.clone())
        .collect();
    let pages = handler.get_pages(descriptors);

    let mut out: Vec<Entry> = Vec::new();
    for (slice, page_arc) in slices.into_iter().zip(pages.into_iter()) {
        let start = slice.start_row_offset as usize;
        let end = slice.end_row_offset as usize;
        let total = page_arc.page.len();
        if start >= total {
            continue;
        }
        let end_clamped = end.min(total);
        for idx in start..end_clamped {
            if let Some(entry) = page_arc.page.entry_at(idx) {
                out.push(entry);
            }
        }
    }

    out
}
