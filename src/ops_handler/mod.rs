// API contracts; callers supply table + column identifiers.

mod create;
mod delete;
mod helpers;
mod insert;
mod read;
mod scan;
mod update;

// Re-export public functions
pub use create::create_table_from_plan;
pub use delete::delete_row;
pub use insert::{
    insert_sorted_row, sorted_insert_single_column, upsert_data_into_column,
    upsert_data_into_table_column,
};
pub use read::read_row;
pub use scan::{range_scan_column_entry, range_scan_table_column_entry};
pub use update::{overwrite_row, update_column_entry, update_column_entry_in_table};
