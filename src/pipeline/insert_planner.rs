use crate::metadata_store::ColumnCatalog;
use crate::ops_handler::insert_sorted_row;
use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::SqlExecutionError;
use crate::writer::{ColumnUpdate, UpdateJob, UpdateOp};
use sqlparser::ast::Statement;

pub(crate) fn execute_insert_plan(
    executor: &SqlExecutor,
    statement: Statement,
) -> Result<(), SqlExecutionError> {
    if executor.use_writer_inserts() {
        execute_insert_writer(executor, statement)
    } else {
        execute_insert_legacy(executor, statement)
    }
}

fn execute_insert_writer(
    executor: &SqlExecutor,
    statement: Statement,
) -> Result<(), SqlExecutionError> {
    let (table, _columns, rows, _sort_indices) = executor.parse_insert_values(statement)?;
    for row in &rows {
        let column_update = ColumnUpdate::new("*", vec![UpdateOp::BufferRow { row: row.clone() }]);
        let job = UpdateJob::new(table.clone(), vec![column_update]);
        executor.writer().submit(job).map_err(|err| {
            SqlExecutionError::OperationFailed(format!("failed to submit insert job: {err:?}"))
        })?;
    }

    if !rows.is_empty() {
        executor
            .writer()
            .flush_table(&table)
            .map_err(|err| SqlExecutionError::OperationFailed(format!("flush failed: {err:?}")))?;
    }

    Ok(())
}

fn execute_insert_legacy(
    executor: &SqlExecutor,
    statement: Statement,
) -> Result<(), SqlExecutionError> {
    let (table, columns, rows, sort_indices) = executor.parse_insert_values(statement)?;
    if rows.is_empty() {
        return Ok(());
    }

    let leading_column = &columns[sort_indices[0]].name;
    for row in rows {
        if executor.table_is_empty(&table, leading_column)? {
            executor.initialise_table_with_row(&table, &columns, &row)?;
        } else {
            let tuple: Vec<(&str, &str)> = build_insert_tuple(&columns, &row);
            insert_sorted_row(executor.page_handler().as_ref(), &table, &tuple)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }
    }

    Ok(())
}

fn build_insert_tuple<'a>(
    columns: &'a [ColumnCatalog],
    row: &'a [String],
) -> Vec<(&'a str, &'a str)> {
    columns
        .iter()
        .map(|col| (col.name.as_str(), row[col.ordinal].as_str()))
        .collect()
}
