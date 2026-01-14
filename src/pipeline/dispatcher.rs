use crate::sql::executor::{SqlExecutionError, SqlExecutor, SelectResult};
use sqlparser::ast::Statement;

pub(crate) fn execute_statement(
    executor: &SqlExecutor,
    statement: Statement,
) -> Result<Option<SelectResult>, SqlExecutionError> {
    match statement {
        Statement::CreateTable { .. } => {
            super::ddl_planner::execute_ddl_plan(executor, statement)?;
            Ok(None)
        }
        Statement::Insert { .. } => {
            super::insert_planner::execute_insert_plan(executor, statement)?;
            Ok(None)
        }
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            from,
            ..
        } => {
            super::mutation_planner::execute_update_plan(
                executor,
                table,
                assignments,
                selection,
                returning,
                from,
            )?;
            Ok(None)
        }
        Statement::Delete {
            tables,
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
            ..
        } => {
            super::mutation_planner::execute_delete_plan(
                executor, tables, from, using, selection, returning, order_by, limit,
            )?;
            Ok(None)
        }
        Statement::Query(query) => {
            let result = executor.execute_select(*query)?;
            Ok(Some(result))
        }
        other => Err(SqlExecutionError::Unsupported(format!(
            "{other:?} is not supported yet"
        ))),
    }
}
