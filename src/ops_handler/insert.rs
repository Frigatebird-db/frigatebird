use crate::entry::Entry;
use crate::metadata_store::DEFAULT_TABLE;
use crate::page_handler::PageHandler;
use std::collections::HashMap;

use super::helpers::{binary_search_insert_index, find_insert_position, other_error};

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

pub fn sorted_insert_single_column(
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
    updated.page.entries.insert(insert_idx, Entry::new(data));
    let new_entry_count = updated.page.entries.len() as u64;

    handler.write_back_uncompressed(&page_meta.id, updated);
    handler
        .update_entry_count_in_table(table, col, new_entry_count)
        .map_err(|err| format!("failed to update metadata entry count: {err}"))?;
    Ok(true)
}

pub fn insert_sorted_row(
    handler: &PageHandler,
    table: &str,
    row: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    sorted_insert_row(handler, table, row)
}

pub fn sorted_insert_row(
    handler: &PageHandler,
    table: &str,
    row: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = handler
        .table_catalog(table)
        .ok_or_else(|| other_error(format!("unknown table: {table}")))?;

    let columns = catalog.columns();
    if columns.is_empty() {
        return Ok(());
    }

    let sort_ordinals: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
    if sort_ordinals.is_empty() {
        return Err(other_error(format!(
            "table {table} does not define an ORDER BY clause"
        )));
    }

    let mut row_values: Vec<Option<String>> = vec![None; columns.len()];
    let mut provided = HashMap::new();
    for (name, value) in row {
        provided.insert((*name).to_string(), (*value).to_string());
    }

    for column in columns {
        if let Some(value) = provided.get(&column.name) {
            row_values[column.ordinal] = Some(value.clone());
        }
    }

    for &ordinal in &sort_ordinals {
        if row_values[ordinal].is_none() {
            return Err(other_error(format!(
                "missing value for sort column {}",
                columns[ordinal].name
            )));
        }
    }

    let final_row: Vec<String> = row_values
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| "".to_string()))
        .collect();

    let leading_column_name = &columns[sort_ordinals[0]].name;
    let row_count = handler
        .locate_latest_in_table(table, leading_column_name)
        .map(|desc| desc.entry_count as usize)
        .unwrap_or(0);

    let insert_idx = find_insert_position(
        handler,
        table,
        columns,
        &sort_ordinals,
        &final_row,
        row_count,
    )?;

    let new_count = row_count + 1;
    for (ordinal, column) in columns.iter().enumerate() {
        let descriptor = handler
            .locate_latest_in_table(table, &column.name)
            .ok_or_else(|| {
                other_error(format!(
                    "missing page metadata for column {table}.{}",
                    column.name
                ))
            })?;

        let page_arc = handler
            .get_page(descriptor.clone())
            .ok_or_else(|| other_error("unable to load page data"))?;

        let mut updated = (*page_arc).clone();
        let insert_pos = insert_idx.min(updated.page.entries.len());
        updated
            .page
            .entries
            .insert(insert_pos, Entry::new(&final_row[ordinal]));

        handler.write_back_uncompressed(&descriptor.id, updated);
        handler
            .update_entry_count_in_table(table, &column.name, new_count as u64)
            .map_err(|err| other_error(format!("failed to update metadata entry count: {err}")))?;
    }

    Ok(())
}
