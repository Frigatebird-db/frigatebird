use crate::entry::Entry;
use crate::metadata_store::ColumnCatalog;
use crate::page_handler::PageHandler;
use std::cmp::Ordering;
use std::io::{Error as IoError, ErrorKind};

pub fn other_error(msg: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(IoError::other(msg.into()))
}

pub fn find_insert_position(
    handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    sort_ordinals: &[usize],
    new_row: &[String],
    row_count: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    for idx in 0..row_count {
        let cmp = compare_row_against_existing(
            handler,
            table,
            columns,
            sort_ordinals,
            new_row,
            idx as u64,
        )?;
        if cmp == Ordering::Less {
            return Ok(idx);
        }
    }
    Ok(row_count)
}

pub fn compare_row_against_existing(
    handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    sort_ordinals: &[usize],
    new_row: &[String],
    existing_idx: u64,
) -> Result<Ordering, Box<dyn std::error::Error>> {
    for &ordinal in sort_ordinals {
        let column_name = &columns[ordinal].name;
        let existing_entry = handler
            .read_entry_at(table, column_name, existing_idx)
            .ok_or_else(|| {
                other_error(format!(
                    "unable to read row {existing_idx} from {table}.{column_name}"
                ))
            })?;
        let cmp = compare_strs(&new_row[ordinal], existing_entry.get_data());
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

pub fn binary_search_insert_index(entries: &[Entry], value: &str) -> usize {
    match entries.binary_search_by(|entry| compare_entry_value(entry, value)) {
        Ok(idx) | Err(idx) => idx,
    }
}

pub fn compare_entry_value(entry: &Entry, value: &str) -> Ordering {
    compare_strs(entry.get_data(), value)
}

pub fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}
