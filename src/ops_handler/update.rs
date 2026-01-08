use crate::entry::Entry;
use crate::metadata_store::DEFAULT_TABLE;
use crate::page_handler::PageHandler;

use super::helpers::other_error;

pub fn overwrite_row(
    handler: &PageHandler,
    table: &str,
    row_idx: u64,
    new_values: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = handler
        .table_catalog(table)
        .ok_or_else(|| other_error(format!("unknown table: {table}")))?;

    if new_values.len() != catalog.columns().len() {
        return Err(other_error(format!(
            "expected {} column values, got {}",
            catalog.columns().len(),
            new_values.len()
        )));
    }

    for column in catalog.columns() {
        let location = handler
            .locate_row_in_table(table, &column.name, row_idx)
            .ok_or_else(|| {
                other_error(format!(
                    "unable to locate row {row_idx} for {table}.{}",
                    column.name
                ))
            })?;

        let descriptor = location.descriptor;
        let page_arc = handler
            .get_page(descriptor.clone())
            .ok_or_else(|| other_error("unable to load page"))?;

        let mut updated = (*page_arc).clone();
        let idx = location.page_row_index as usize;
        let mut in_bounds = true;
        updated.mutate_disk_page(
            |disk_page| {
                if idx >= disk_page.entries.len() {
                    in_bounds = false;
                    return;
                }
                disk_page.entries[idx] = Entry::new(&new_values[column.ordinal]);
            },
            column.data_type,
        );
        if !in_bounds {
            return Err(other_error(format!("row {row_idx} out of bounds")));
        }
        handler
            .persist_descriptor_page(&descriptor, &updated)
            .map_err(|err| {
                other_error(format!("failed to persist {table}.{}: {err}", column.name))
            })?;
        handler.write_back_uncompressed(&descriptor.id, updated);
    }

    Ok(())
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
    let data_type = handler
        .table_catalog(table)
        .and_then(|catalog| catalog.column(col).map(|column| column.data_type))
        .unwrap_or(crate::sql::types::DataType::String);
    let page_meta = handler
        .locate_latest_in_table(table, col)
        .ok_or_else(|| "missing page metadata for column")?;

    let page_arc = handler
        .get_page(page_meta.clone())
        .ok_or_else(|| "unable to load page")?;

    let mut updated = (*page_arc).clone();
    let mut in_bounds = true;
    updated.mutate_disk_page(
        |disk_page| {
            let idx = row as usize;
            if idx >= disk_page.entries.len() {
                in_bounds = false;
                return;
            }
            disk_page.entries[idx] = Entry::new(data);
        },
        data_type,
    );
    if !in_bounds {
        return Ok(false);
    }

    handler
        .persist_descriptor_page(&page_meta, &updated)
        .map_err(|err| other_error(format!("failed to persist {table}.{col}: {err}")))?;
    handler.write_back_uncompressed(&page_meta.id, updated);

    Ok(true)
}
