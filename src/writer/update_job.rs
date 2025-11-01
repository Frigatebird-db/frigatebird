use crate::entry::Entry;

/// Represents a per-column mutation batch within an update job.
#[derive(Clone)]
pub struct ColumnUpdate {
    pub column: String,
    pub operations: Vec<UpdateOp>,
}

impl ColumnUpdate {
    pub fn new(column: impl Into<String>, operations: Vec<UpdateOp>) -> Self {
        ColumnUpdate {
            column: column.into(),
            operations,
        }
    }
}

/// Individual mutations that can be applied to a page.
#[derive(Clone)]
pub enum UpdateOp {
    /// Overwrite a specific logical row with the provided entry.
    Overwrite { row: u64, entry: Entry },
    /// Append a new entry to the end of the page.
    Append { entry: Entry },
}

/// A unit of work handed to the Writer.
#[derive(Clone)]
pub struct UpdateJob {
    pub table: String,
    pub columns: Vec<ColumnUpdate>,
}

impl UpdateJob {
    pub fn new(table: impl Into<String>, columns: Vec<ColumnUpdate>) -> Self {
        UpdateJob {
            table: table.into(),
            columns,
        }
    }
}
