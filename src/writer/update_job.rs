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
    /// Insert a new entry at the provided logical row, shifting existing entries.
    InsertAt { row: u64, entry: Entry },
    /// Buffer a fully materialized row for later ingestion.
    BufferRow { row: Vec<String> },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_job_new_creates_job() {
        let job = UpdateJob::new("users", vec![]);
        assert_eq!(job.table, "users");
        assert_eq!(job.columns.len(), 0);
    }

    #[test]
    fn update_job_with_columns() {
        let col_update = ColumnUpdate::new("email", vec![]);
        let job = UpdateJob::new("users", vec![col_update]);
        assert_eq!(job.columns.len(), 1);
        assert_eq!(job.columns[0].column, "email");
    }

    #[test]
    fn column_update_new_creates_update() {
        let update = ColumnUpdate::new("name", vec![]);
        assert_eq!(update.column, "name");
        assert_eq!(update.operations.len(), 0);
    }

    #[test]
    fn column_update_with_operations() {
        let ops = vec![
            UpdateOp::Append {
                entry: Entry::new("data1"),
            },
            UpdateOp::Append {
                entry: Entry::new("data2"),
            },
        ];
        let update = ColumnUpdate::new("col", ops);
        assert_eq!(update.operations.len(), 2);
    }

    #[test]
    fn update_op_append_stores_entry() {
        let entry = Entry::new("test_data");
        let op = UpdateOp::Append {
            entry: entry.clone(),
        };

        match op {
            UpdateOp::Append { entry: e } => {
                assert_eq!(e.get_data(), "test_data");
            }
            _ => panic!("Expected Append variant"),
        }
    }

    #[test]
    fn update_op_overwrite_stores_row_and_entry() {
        let entry = Entry::new("updated_data");
        let op = UpdateOp::Overwrite {
            row: 42,
            entry: entry.clone(),
        };

        match op {
            UpdateOp::Overwrite { row, entry: e } => {
                assert_eq!(row, 42);
                assert_eq!(e.get_data(), "updated_data");
            }
            _ => panic!("Expected Overwrite variant"),
        }
    }

    #[test]
    fn update_op_insert_at_stores_row_and_entry() {
        let entry = Entry::new("inserted");
        let op = UpdateOp::InsertAt {
            row: 3,
            entry: entry.clone(),
        };

        match op {
            UpdateOp::InsertAt { row, entry: e } => {
                assert_eq!(row, 3);
                assert_eq!(e.get_data(), "inserted");
            }
            _ => panic!("Expected InsertAt variant"),
        }
    }

    #[test]
    fn update_job_multiple_columns() {
        let job = UpdateJob::new(
            "products",
            vec![
                ColumnUpdate::new(
                    "id",
                    vec![UpdateOp::Append {
                        entry: Entry::new("1"),
                    }],
                ),
                ColumnUpdate::new(
                    "name",
                    vec![UpdateOp::Append {
                        entry: Entry::new("Widget"),
                    }],
                ),
                ColumnUpdate::new(
                    "price",
                    vec![UpdateOp::Append {
                        entry: Entry::new("9.99"),
                    }],
                ),
            ],
        );

        assert_eq!(job.table, "products");
        assert_eq!(job.columns.len(), 3);
        assert_eq!(job.columns[0].column, "id");
        assert_eq!(job.columns[1].column, "name");
        assert_eq!(job.columns[2].column, "price");
    }

    #[test]
    fn update_job_mixed_operations() {
        let ops = vec![
            UpdateOp::Append {
                entry: Entry::new("new"),
            },
            UpdateOp::Overwrite {
                row: 0,
                entry: Entry::new("updated"),
            },
            UpdateOp::Append {
                entry: Entry::new("another"),
            },
        ];

        let job = UpdateJob::new("test", vec![ColumnUpdate::new("data", ops)]);

        assert_eq!(job.columns[0].operations.len(), 3);
    }

    #[test]
    fn column_update_clone_works() {
        let update = ColumnUpdate::new(
            "test",
            vec![UpdateOp::Append {
                entry: Entry::new("data"),
            }],
        );

        let cloned = update.clone();
        assert_eq!(cloned.column, "test");
        assert_eq!(cloned.operations.len(), 1);
    }

    #[test]
    fn update_op_clone_works() {
        let op = UpdateOp::Append {
            entry: Entry::new("test"),
        };

        let cloned = op.clone();
        match cloned {
            UpdateOp::Append { entry } => {
                assert_eq!(entry.get_data(), "test");
            }
            _ => panic!("Expected Append variant"),
        }
    }
}
