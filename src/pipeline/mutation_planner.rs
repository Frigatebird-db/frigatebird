use crate::metadata_store::ColumnCatalog;
use crate::ops_handler::{delete_row, insert_sorted_row, overwrite_row, read_row};
use crate::pipeline::planner::plan_row_ids_for_select;
use crate::pipeline::operators::{FilterOperator, PipelineOperator};
use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::helpers::{
    collect_expr_column_ordinals, expr_to_string, object_name_to_string, table_with_joins_to_name,
};
use crate::sql::runtime::physical_evaluator::filter_supported;
use crate::sql::runtime::scan_helpers::build_scan_stream;
use crate::sql::runtime::scan_stream::collect_stream_batches;
use crate::sql::runtime::values::compare_strs;
use crate::sql::runtime::SqlExecutionError;
use crate::sql::planner::ExpressionPlanner;
use crate::sql::types::DataType;
use sqlparser::ast::{
    Assignment, Expr, FromTable, ObjectName, OrderByExpr, SelectItem, TableFactor, TableWithJoins,
    Value,
};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

pub(crate) fn execute_update_plan(
    executor: &SqlExecutor,
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
    let catalog = executor
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

    let matching_rows = collect_matching_row_ids(
        executor,
        &table_name,
        &catalog,
        &columns,
        &sort_columns,
        selection,
        &column_ordinals,
        &column_types,
    )?;
    if matching_rows.is_empty() {
        return Ok(());
    }

    for &row_idx in matching_rows.iter().rev() {
        let current_row = read_row(executor.page_handler().as_ref(), &table_name, row_idx)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        let mut new_row = current_row.clone();
        for (ordinal, value) in &assignments_vec {
            new_row[*ordinal] = value.clone();
        }

        let sort_changed =
            sort_indices.iter().any(|&idx| compare_strs(&current_row[idx], &new_row[idx]) != Ordering::Equal);

        if sort_changed {
            delete_row(executor.page_handler().as_ref(), &table_name, row_idx)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            let tuple: Vec<(&str, &str)> = columns
                .iter()
                .map(|col| (col.name.as_str(), new_row[col.ordinal].as_str()))
                .collect();
            insert_sorted_row(executor.page_handler().as_ref(), &table_name, &tuple)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        } else {
            overwrite_row(executor.page_handler().as_ref(), &table_name, row_idx, &new_row)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }
    }

    Ok(())
}

pub(crate) fn execute_delete_plan(
    executor: &SqlExecutor,
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

    let catalog = executor
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

    let mut matching_rows = collect_matching_row_ids(
        executor,
        &table_name,
        &catalog,
        &columns,
        &sort_columns,
        selection,
        &column_ordinals,
        &column_types,
    )?;
    if matching_rows.is_empty() {
        return Ok(());
    }
    matching_rows.dedup();
    for row_idx in matching_rows.into_iter().rev() {
        delete_row(executor.page_handler().as_ref(), &table_name, row_idx)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
    }

    Ok(())
}

fn collect_matching_row_ids(
    executor: &SqlExecutor,
    table_name: &str,
    catalog: &crate::metadata_store::TableCatalog,
    columns: &[ColumnCatalog],
    sort_columns: &[ColumnCatalog],
    selection: Option<Expr>,
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
) -> Result<Vec<u64>, SqlExecutionError> {
    let selection_expr = selection.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
    let has_selection = !matches!(selection_expr, Expr::Value(Value::Boolean(true)));

    let expr_planner = ExpressionPlanner::new(catalog);
    let physical_selection_expr = if has_selection {
        match expr_planner.plan_expression(&selection_expr) {
            Ok(expr) => Some(expr),
            Err(crate::sql::PlannerError::Unsupported(_)) => None,
            Err(err) => return Err(SqlExecutionError::Plan(err)),
        }
    } else {
        None
    };

    let can_use_physical_filter = physical_selection_expr
        .as_ref()
        .map_or(false, filter_supported);

    let mut required_ordinals: BTreeSet<usize> =
        sort_columns.iter().map(|col| col.ordinal).collect();
    if has_selection {
        let predicate_ordinals =
            collect_expr_column_ordinals(&selection_expr, column_ordinals, table_name)?;
        required_ordinals.extend(predicate_ordinals);
    }

    let row_ids = plan_row_ids_for_select(
        executor,
        table_name,
        sort_columns,
        if has_selection { Some(&selection_expr) } else { None },
        column_types,
    )?;
    let mut row_ids = row_ids;
    if let Some(rows) = row_ids.as_mut() {
        rows.sort_unstable();
        rows.dedup();
        if rows.is_empty() {
            row_ids = None;
        }
    }
    let has_row_ids = row_ids.is_some();
    let vectorized_selection_expr = if can_use_physical_filter {
        physical_selection_expr.as_ref()
    } else {
        None
    };
    let stream = build_scan_stream(
        executor.page_handler(),
        table_name,
        columns,
        &required_ordinals,
        vectorized_selection_expr,
        row_ids,
    )?;
    let selection_applied_in_scan = has_selection && can_use_physical_filter && !has_row_ids;
    let mut matching_rows = Vec::new();
    let batches = collect_stream_batches(stream)?;
    for batch in batches {
        let mut batch = batch;
        if has_selection && !selection_applied_in_scan {
            let mut filter = FilterOperator::new(
                executor.page_handler().as_ref(),
                &selection_expr,
                vectorized_selection_expr,
                catalog,
                table_name,
                columns,
                column_ordinals,
                column_types,
            );
            let mut results = filter.execute(batch)?;
            batch = results
                .pop()
                .unwrap_or_else(crate::sql::runtime::batch::ColumnarBatch::new);
        }
        if batch.num_rows > 0 {
            matching_rows.extend(batch.row_ids);
        }
    }
    matching_rows.sort_unstable();
    Ok(matching_rows)
}
