use crate::page_handler::PageHandler;

use super::helpers::other_error;

pub fn delete_row(
    handler: &PageHandler,
    table: &str,
    row_idx: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = handler
        .table_catalog(table)
        .ok_or_else(|| other_error(format!("unknown table: {table}")))?;

    for column in catalog.columns().iter().rev() {
        let descriptor = handler
            .locate_latest_in_table(table, &column.name)
            .ok_or_else(|| {
                other_error(format!("missing page metadata for {table}.{}", column.name))
            })?;

        let page_arc = handler
            .get_page(descriptor.clone())
            .ok_or_else(|| other_error("unable to load page"))?;

        let mut updated = (*page_arc).clone();
        let idx = row_idx as usize;
        if idx >= updated.page.entries.len() {
            return Err(other_error(format!("row {row_idx} out of bounds")));
        }
        updated.page.entries.remove(idx);
        let new_len = updated.page.entries.len() as u64;
        handler.write_back_uncompressed(&descriptor.id, updated);
        handler
            .update_entry_count_in_table(table, &column.name, new_len)
            .map_err(|err| other_error(format!("failed to update metadata entry count: {err}")))?;
    }

    Ok(())
}
