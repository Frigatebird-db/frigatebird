use crate::metadata_store::ColumnCatalog;
use crate::page_handler::PageHandler;
use crate::sql::runtime::SqlExecutionError;

pub(crate) fn estimate_table_row_count(
    page_handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
) -> Result<u64, SqlExecutionError> {
    for column in columns {
        if let Some(descriptor) = page_handler.locate_latest_in_table(table, &column.name) {
            return Ok(descriptor.entry_count);
        }
    }
    Ok(0)
}
