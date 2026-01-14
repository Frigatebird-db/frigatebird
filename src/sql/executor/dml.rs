use super::{SqlExecutionError, SqlExecutor};
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::ColumnCatalog;
use crate::page::Page;
use crate::sql::runtime::helpers::{expr_to_string, object_name_to_string};
use sqlparser::ast::{Expr, SetExpr, Statement, Value};
use std::collections::HashMap;

impl SqlExecutor {
    #[allow(clippy::type_complexity)]
    pub(crate) fn parse_insert_values(
        &self,
        statement: Statement,
    ) -> Result<(String, Vec<ColumnCatalog>, Vec<Vec<String>>, Vec<usize>), SqlExecutionError> {
        let Statement::Insert {
            table_name,
            columns: specified_columns,
            source,
            ..
        } = statement
        else {
            unreachable!("matched Insert above");
        };

        let table = object_name_to_string(&table_name);
        let query = source
            .as_ref()
            .ok_or_else(|| SqlExecutionError::Unsupported("INSERT without VALUES".into()))?;

        let SetExpr::Values(values) = query.body.as_ref() else {
            return Err(SqlExecutionError::Unsupported(
                "only INSERT ... VALUES is supported".into(),
            ));
        };

        let catalog = self
            .page_directory()
            .table_catalog(&table)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();

        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        let specified_ordinals: Vec<usize> = if specified_columns.is_empty() {
            (0..columns.len()).collect()
        } else {
            specified_columns
                .iter()
                .map(|ident| {
                    let name = ident.value.clone();
                    column_ordinals.get(&name).copied().ok_or_else(|| {
                        SqlExecutionError::ColumnMismatch {
                            table: table.clone(),
                            column: name,
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "INSERT currently requires ORDER BY tables".into(),
            ));
        }

        let mut rows = Vec::with_capacity(values.rows.len());
        for row in &values.rows {
            if row.len() != specified_ordinals.len() {
                return Err(SqlExecutionError::ValueMismatch(format!(
                    "expected {} values, got {}",
                    specified_ordinals.len(),
                    row.len()
                )));
            }

            let mut row_values: Vec<Option<String>> = vec![None; columns.len()];
            for (expr, &ordinal) in row.iter().zip(&specified_ordinals) {
                let literal = expr_to_string(expr)?;
                row_values[ordinal] = Some(literal);
            }

            rows.push(
                row_values
                    .into_iter()
                    .map(|value| value.unwrap_or_default())
                    .collect(),
            );
        }

        Ok((table, columns, rows, sort_indices))
    }

    pub(crate) fn table_is_empty(
        &self,
        table: &str,
        leading_column: &str,
    ) -> Result<bool, SqlExecutionError> {
        match self
            .page_handler()
            .locate_latest_in_table(table, leading_column)
        {
            Some(descriptor) => Ok(descriptor.entry_count == 0),
            None => Ok(true),
        }
    }

    pub(crate) fn initialise_table_with_row(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        row: &[String],
    ) -> Result<(), SqlExecutionError> {
        for column in columns {
            let descriptor = self
                .page_handler()
                .locate_latest_in_table(table, &column.name)
                .or_else(|| {
                    self.page_directory().register_page_in_table_with_sizes(
                        table,
                        &column.name,
                        format!("mem://{table}_{}_page0", column.name),
                        0,
                        0,
                        0,
                        0,
                    )
                })
                .ok_or_else(|| {
                    SqlExecutionError::OperationFailed(format!(
                        "failed to allocate page for {table}.{}",
                        column.name
                    ))
                })?;

            let mut page = Page::new();
            page.page_metadata = descriptor.id.clone();
            page.entries.push(Entry::new(&row[column.ordinal]));
            self.page_handler().write_back_uncompressed(
                &descriptor.id,
                PageCacheEntryUncompressed::from_disk_page(page, column.data_type),
            );
            self.page_handler()
                .update_entry_count_in_table(table, &column.name, 1)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }
        Ok(())
    }
}
