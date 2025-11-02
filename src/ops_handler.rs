// these are our external API contracts; callers supply table + column identifiers.

use crate::entry::Entry;
use crate::metadata_store::{
    CatalogError, ColumnDefinition, DEFAULT_TABLE, PageDescriptor, PageDirectory, TableDefinition,
};
use crate::page_handler::PageHandler;
use crate::sql::CreateTablePlan;
use std::cmp::Ordering;

pub fn create_table_from_plan(
    directory: &PageDirectory,
    plan: &CreateTablePlan,
) -> Result<(), CatalogError> {
    let columns: Vec<ColumnDefinition> = plan
        .columns
        .iter()
        .map(|spec| ColumnDefinition::new(spec.name.clone(), spec.data_type.clone()))
        .collect();
    let definition = TableDefinition::new(plan.table_name.clone(), columns, plan.order_by.clone());
    match directory.register_table(definition) {
        Ok(_) => Ok(()),
        Err(CatalogError::TableExists(_)) if plan.if_not_exists => Ok(()),
        Err(err) => Err(err),
    }
}

// TODO: we also have to update the (l,r) ranges whenever we upsert something into it
pub fn upsert_data_into_column(
    handler: &PageHandler,
    col: &str,
    data: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    upsert_data_into_table_column(handler, DEFAULT_TABLE, col, data)
}

pub fn upsert_data_into_table_column(
    handler: &PageHandler,
    table: &str,
    col: &str,
    data: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    if let Some(catalog) = handler.table_catalog(table) {
        let sort_columns = catalog.sort_key();
        if sort_columns.len() == 1 && sort_columns[0].name == col {
            return sorted_insert_single_column(handler, table, col, data);
        }
    }

    let page_meta = handler
        .locate_latest_in_table(table, col)
        .ok_or_else(|| "missing page metadata for column")?;

    let page_arc = handler
        .get_page(page_meta.clone())
        .ok_or_else(|| "unable to load page")?;

    let mut updated = (*page_arc).clone();
    updated.page.add_entry(Entry::new(data));

    handler.write_back_uncompressed(&page_meta.id, updated);

    Ok(true)
}

fn sorted_insert_single_column(
    handler: &PageHandler,
    table: &str,
    col: &str,
    data: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let page_meta = handler
        .locate_latest_in_table(table, col)
        .ok_or_else(|| "missing page metadata for column")?;

    let page_arc = handler
        .get_page(page_meta.clone())
        .ok_or_else(|| "unable to load page")?;

    let mut updated = (*page_arc).clone();
    let insert_idx = binary_search_insert_index(&updated.page.entries, data);
    updated
        .page
        .entries
        .insert(insert_idx, Entry::new(data));
    let new_entry_count = updated.page.entries.len() as u64;

    handler.write_back_uncompressed(&page_meta.id, updated);
    handler
        .update_entry_count_in_table(table, col, new_entry_count)
        .map_err(|err| format!("failed to update metadata entry count: {err}"))?;
    Ok(true)
}

fn binary_search_insert_index(entries: &[Entry], value: &str) -> usize {
    match entries.binary_search_by(|entry| compare_entry_value(entry, value)) {
        Ok(idx) | Err(idx) => idx,
    }
}

fn compare_entry_value(entry: &Entry, value: &str) -> Ordering {
    compare_strs(entry.get_data(), value)
}

fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}

pub fn update_column_entry(
    handler: &PageHandler,
    col: &str,
    data: &str,
    row: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    update_column_entry_in_table(handler, DEFAULT_TABLE, col, data, row)
}

pub fn update_column_entry_in_table(
    handler: &PageHandler,
    table: &str,
    col: &str,
    data: &str,
    row: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    let page_meta = handler
        .locate_latest_in_table(table, col)
        .ok_or_else(|| "missing page metadata for column")?;

    let page_arc = handler
        .get_page(page_meta.clone())
        .ok_or_else(|| "unable to load page")?;

    let mut updated = (*page_arc).clone();
    if (row as usize) >= updated.page.entries.len() {
        return Ok(false);
    }
    updated.page.entries[row as usize] = Entry::new(data);

    handler.write_back_uncompressed(&page_meta.id, updated);

    Ok(true)
}

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
        let entries = &page_arc.page.entries;
        let start = slice.start_row_offset as usize;
        let end = slice.end_row_offset as usize;
        if start >= entries.len() {
            continue;
        }
        let end_clamped = end.min(entries.len());
        out.extend_from_slice(&entries[start..end_clamped]);
    }

    out
}

fn range_scan_columns_entries() {
    // honestly, the columns are pretty decoupled already

    // I doubt there is any shared contention or coordination thingy here, its just doing a bunch of independent stuff fast
}
