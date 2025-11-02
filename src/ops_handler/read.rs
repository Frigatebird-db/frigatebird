use crate::page_handler::PageHandler;

use super::helpers::other_error;

pub fn read_row(
    handler: &PageHandler,
    table: &str,
    row_idx: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let catalog = handler
        .table_catalog(table)
        .ok_or_else(|| other_error(format!("unknown table: {table}")))?;

    let mut row = Vec::with_capacity(catalog.columns().len());
    for column in catalog.columns() {
        let entry = handler
            .read_entry_at(table, &column.name, row_idx)
            .ok_or_else(|| {
                other_error(format!(
                    "unable to read row {row_idx} for column {table}.{}",
                    column.name
                ))
            })?;
        row.push(entry.get_data().to_string());
    }

    Ok(row)
}
