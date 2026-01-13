use super::{SqlExecutionError, SqlExecutor};
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::ColumnCatalog;
use crate::ops_handler::{delete_row, insert_sorted_row, overwrite_row, read_row};
use crate::page::Page;
use crate::sql::PlannerError;
use crate::sql::executor::helpers::{
    collect_expr_column_ordinals, expr_to_string, object_name_to_string,
    table_with_joins_to_name,
};
use crate::sql::executor::physical_evaluator::filter_supported;
use crate::sql::executor::projection_helpers::materialize_columns;
use crate::sql::executor::scan_stream::merge_stream_to_batch;
use crate::sql::executor::{filter_rows_with_expr, refine_rows_with_vectorized_filter};
use crate::sql::executor::scan_helpers::{collect_sort_key_filters, collect_sort_key_prefixes};
use crate::sql::executor::values::compare_strs;
use crate::sql::planner::ExpressionPlanner;
use crate::sql::types::DataType;
use crate::writer::{ColumnUpdate, UpdateJob, UpdateOp};
use sqlparser::ast::{
    Assignment, Expr, FromTable, ObjectName, OrderByExpr, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

impl SqlExecutor {
    pub(crate) fn execute_insert(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        if self.use_writer_inserts {
            self.execute_insert_writer(statement)
        } else {
            self.execute_insert_legacy(statement)
        }
    }

    fn execute_insert_writer(&self, statement: Statement) -> Result<(), SqlExecutionError> {
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
            .page_directory
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

            for &ordinal in &sort_indices {
                if row_values[ordinal].is_none() {
                    return Err(SqlExecutionError::ValueMismatch(format!(
                        "missing value for sort column {}",
                        columns[ordinal].name
                    )));
                }
            }

            let final_row: Vec<String> = row_values
                .into_iter()
                .map(|value| value.unwrap_or_default())
                .collect();

            let column_update =
                ColumnUpdate::new("*", vec![UpdateOp::BufferRow { row: final_row }]);
            let job = UpdateJob::new(table.clone(), vec![column_update]);
            self.writer.submit(job).map_err(|err| {
                SqlExecutionError::OperationFailed(format!("failed to submit insert job: {err:?}"))
            })?;
        }

        if !values.rows.is_empty() {
            self.writer.flush_table(&table).map_err(|err| {
                SqlExecutionError::OperationFailed(format!("flush failed: {err:?}"))
            })?;
        }

        Ok(())
    }

    fn execute_insert_legacy(&self, statement: Statement) -> Result<(), SqlExecutionError> {
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
            .page_directory
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
        for row in &values.rows {
            if row.len() != specified_ordinals.len() {
                return Err(SqlExecutionError::ValueMismatch(format!(
                    "expected {} values, got {}",
                    specified_ordinals.len(),
                    row.len()
                )));
            }

            let mut values_by_ordinal: Vec<String> = vec![String::new(); columns.len()];
            for (expr, &ordinal) in row.iter().zip(&specified_ordinals) {
                let literal = expr_to_string(expr)?;
                values_by_ordinal[ordinal] = literal;
            }

            let leading_column = &columns[sort_indices[0]].name;
            if self.table_is_empty(&table, leading_column)? {
                self.initialise_table_with_row(&table, &columns, &values_by_ordinal)?;
            } else {
                let mut kv_pairs = Vec::with_capacity(columns.len());
                for column in &columns {
                    kv_pairs.push((
                        column.name.clone(),
                        values_by_ordinal[column.ordinal].clone(),
                    ));
                }
                let tuple: Vec<(&str, &str)> = kv_pairs
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                insert_sorted_row(&self.page_handler, &table, &tuple)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            }
        }

        Ok(())
    }

    pub(crate) fn execute_update(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        from: Option<TableWithJoins>,
    ) -> Result<(), SqlExecutionError> {
        if returning.is_some() || from.is_some() {
            return Err(SqlExecutionError::Unsupported(
                "UPDATE with RETURNING or FROM is not supported".into(),
            ));
        }

        let table_name = table_with_joins_to_name(&table)?;
        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "UPDATE currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_indices
            .iter()
            .map(|&idx| columns[idx].clone())
            .collect();

        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        let mut column_types: HashMap<String, DataType> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
            column_types.insert(column.name.clone(), column.data_type);
        }

        if assignments.is_empty() {
            return Ok(());
        }

        let mut assignments_vec = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            if assignment.id.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "assignment missing column".into(),
                ));
            }
            let column_name = assignment.id.last().unwrap().value.clone();
            let ordinal = column_ordinals.get(&column_name).copied().ok_or_else(|| {
                SqlExecutionError::ColumnMismatch {
                    table: table_name.clone(),
                    column: column_name.clone(),
                }
            })?;
            let value = expr_to_string(&assignment.value)?;
            assignments_vec.push((ordinal, value));
        }

        let selection_expr = selection.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
        let has_selection = !matches!(selection_expr, Expr::Value(Value::Boolean(true)));

        let expr_planner = ExpressionPlanner::new(&catalog);
        let physical_selection_expr = if has_selection {
            match expr_planner.plan_expression(&selection_expr) {
                Ok(expr) => Some(expr),
                Err(PlannerError::Unsupported(_)) => None,
                Err(err) => return Err(SqlExecutionError::Plan(err)),
            }
        } else {
            None
        };

        let can_use_physical_filter = physical_selection_expr
            .as_ref()
            .map_or(false, filter_supported);
        let mut required_ordinals: BTreeSet<usize> = sort_indices.iter().copied().collect();
        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;
        let sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };

        let mut key_values = Vec::with_capacity(sort_columns.len());
        let mut selection_applied_in_scan = false;
        let mut candidate_rows = if let Some(filters) = sort_key_filters {
            for column in &sort_columns {
                let value = filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "UPDATE requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?;
                key_values.push(value);
            }
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?
        } else if let Some(prefixes) = sort_key_prefixes {
            self.locate_rows_by_sort_prefixes(&table_name, &sort_columns, &prefixes)?
        } else {
            let stream = self.build_scan_stream(
                &table_name,
                &columns,
                &required_ordinals,
                if can_use_physical_filter {
                    physical_selection_expr.as_ref()
                } else {
                    None
                },
                &column_ordinals,
                catalog.rows_per_page_group,
                None,
            )?;
            let batch = merge_stream_to_batch(stream)?;
            selection_applied_in_scan = has_selection && can_use_physical_filter;
            batch.row_ids
        };
        if candidate_rows.is_empty() {
            return Ok(());
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        if has_selection && can_use_physical_filter && !selection_applied_in_scan {
            if let Some(expr) = &physical_selection_expr {
                candidate_rows = refine_rows_with_vectorized_filter(
                    &self.page_handler,
                    &table_name,
                    &columns,
                    expr,
                    &column_ordinals,
                    &candidate_rows,
                    catalog.rows_per_page_group,
                )?;
            }
        }
        if has_selection && !can_use_physical_filter {
            let materialized = materialize_columns(
                &self.page_handler,
                &table_name,
                &columns,
                &required_ordinals,
                &candidate_rows,
            )?;
            candidate_rows = filter_rows_with_expr(
                &selection_expr,
                &candidate_rows,
                &materialized,
                &column_ordinals,
                &column_types,
                false,
            )?;
        }
        if candidate_rows.is_empty() {
            return Ok(());
        }

        let mut matching_rows = candidate_rows;
        matching_rows.sort_unstable();

        for &row_idx in matching_rows.iter().rev() {
            let current_row = read_row(&self.page_handler, &table_name, row_idx)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            let mut new_row = current_row.clone();
            for (ordinal, value) in &assignments_vec {
                new_row[*ordinal] = value.clone();
            }

            let sort_changed = sort_indices
                .iter()
                .any(|&idx| compare_strs(&current_row[idx], &new_row[idx]) != Ordering::Equal);

            if sort_changed {
                delete_row(&self.page_handler, &table_name, row_idx)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
                let kv_pairs: Vec<(String, String)> = columns
                    .iter()
                    .map(|col| (col.name.clone(), new_row[col.ordinal].clone()))
                    .collect();
                let tuple: Vec<(&str, &str)> = kv_pairs
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                insert_sorted_row(&self.page_handler, &table_name, &tuple)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            } else {
                overwrite_row(&self.page_handler, &table_name, row_idx, &new_row)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            }
        }

        Ok(())
    }

    pub(crate) fn execute_delete(
        &self,
        tables: Vec<ObjectName>,
        from: FromTable,
        using: Option<Vec<TableWithJoins>>,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        order_by: Vec<OrderByExpr>,
        limit: Option<Expr>,
    ) -> Result<(), SqlExecutionError> {
        if !tables.is_empty()
            || using.is_some()
            || returning.is_some()
            || !order_by.is_empty()
            || limit.is_some()
        {
            return Err(SqlExecutionError::Unsupported(
                "DELETE with advanced clauses is not supported".into(),
            ));
        }

        let table_with_joins = match &from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                if tables.len() != 1 {
                    return Err(SqlExecutionError::Unsupported(
                        "DELETE supports exactly one table".into(),
                    ));
                }
                &tables[0]
            }
        };
        if !table_with_joins.joins.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "DELETE with JOINs is not supported".into(),
            ));
        }
        let table_name = match &table_with_joins.relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "unsupported DELETE target".into(),
                ));
            }
        };

        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        let mut column_types: HashMap<String, DataType> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
            column_types.insert(column.name.clone(), column.data_type);
        }

        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "DELETE currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_indices
            .iter()
            .map(|&idx| columns[idx].clone())
            .collect();

        let selection_expr = selection.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
        let has_selection = !matches!(selection_expr, Expr::Value(Value::Boolean(true)));

        let expr_planner = ExpressionPlanner::new(&catalog);
        let physical_selection_expr = if has_selection {
            match expr_planner.plan_expression(&selection_expr) {
                Ok(expr) => Some(expr),
                Err(PlannerError::Unsupported(_)) => None,
                Err(err) => return Err(SqlExecutionError::Plan(err)),
            }
        } else {
            None
        };

        let can_use_physical_filter = physical_selection_expr
            .as_ref()
            .map_or(false, filter_supported);
        let mut required_ordinals: BTreeSet<usize> = sort_indices.iter().copied().collect();
        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;
        let sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };

        let mut key_values = Vec::with_capacity(sort_columns.len());
        let mut selection_applied_in_scan = false;
        let mut candidate_rows = if let Some(filters) = sort_key_filters {
            for column in &sort_columns {
                let value = filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "DELETE requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?;
                key_values.push(value);
            }
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?
        } else if let Some(prefixes) = sort_key_prefixes {
            self.locate_rows_by_sort_prefixes(&table_name, &sort_columns, &prefixes)?
        } else {
            let stream = self.build_scan_stream(
                &table_name,
                &columns,
                &required_ordinals,
                if can_use_physical_filter {
                    physical_selection_expr.as_ref()
                } else {
                    None
                },
                &column_ordinals,
                catalog.rows_per_page_group,
                None,
            )?;
            let batch = merge_stream_to_batch(stream)?;
            selection_applied_in_scan = has_selection && can_use_physical_filter;
            batch.row_ids
        };

        if candidate_rows.is_empty() {
            return Ok(());
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        if has_selection && can_use_physical_filter && !selection_applied_in_scan {
            if let Some(expr) = &physical_selection_expr {
                candidate_rows = refine_rows_with_vectorized_filter(
                    &self.page_handler,
                    &table_name,
                    &columns,
                    expr,
                    &column_ordinals,
                    &candidate_rows,
                    catalog.rows_per_page_group,
                )?;
            }
        }
        if has_selection && !can_use_physical_filter {
            let materialized = materialize_columns(
                &self.page_handler,
                &table_name,
                &columns,
                &required_ordinals,
                &candidate_rows,
            )?;
            candidate_rows = filter_rows_with_expr(
                &selection_expr,
                &candidate_rows,
                &materialized,
                &column_ordinals,
                &column_types,
                false,
            )?;
        }
        if candidate_rows.is_empty() {
            return Ok(());
        }

        let mut matching_rows = candidate_rows;
        matching_rows.sort_unstable();
        matching_rows.dedup();
        for row_idx in matching_rows.into_iter().rev() {
            delete_row(&self.page_handler, &table_name, row_idx)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }

        Ok(())
    }

    fn table_is_empty(&self, table: &str, leading_column: &str) -> Result<bool, SqlExecutionError> {
        match self
            .page_handler
            .locate_latest_in_table(table, leading_column)
        {
            Some(descriptor) => Ok(descriptor.entry_count == 0),
            None => Ok(true),
        }
    }

    fn initialise_table_with_row(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        row: &[String],
    ) -> Result<(), SqlExecutionError> {
        for column in columns {
            let descriptor = self
                .page_handler
                .locate_latest_in_table(table, &column.name)
                .or_else(|| {
                    self.page_directory.register_page_in_table_with_sizes(
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
            self.page_handler.write_back_uncompressed(
                &descriptor.id,
                PageCacheEntryUncompressed::from_disk_page(page, column.data_type),
            );
            self.page_handler
                .update_entry_count_in_table(table, &column.name, 1)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }
        Ok(())
    }
}
