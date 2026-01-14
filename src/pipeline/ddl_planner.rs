use crate::metadata_store::{JournalColumnDef, MetaRecord, ROWS_PER_PAGE_GROUP};
use crate::ops_handler::create_table_from_plan;
use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::SqlExecutionError;
use crate::sql::{CreateTablePlan, plan_create_table_statement};
use sqlparser::ast::Statement;

pub(crate) fn execute_ddl_plan(
    executor: &SqlExecutor,
    statement: Statement,
) -> Result<(), SqlExecutionError> {
    match statement {
        Statement::CreateTable { .. } => execute_create(executor, statement),
        other => Err(SqlExecutionError::Unsupported(format!(
            "{other:?} is not supported yet"
        ))),
    }
}

fn execute_create(executor: &SqlExecutor, statement: Statement) -> Result<(), SqlExecutionError> {
    let plan: CreateTablePlan = plan_create_table_statement(&statement)?;
    create_table_from_plan(executor.page_directory(), &plan)
        .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;

    if let Some(journal) = executor.meta_journal() {
        let columns: Vec<JournalColumnDef> = plan
            .columns
            .iter()
            .map(|spec| JournalColumnDef {
                name: spec.name.clone(),
                data_type: crate::sql::types::DataType::from_sql(&spec.data_type)
                    .unwrap_or(crate::sql::types::DataType::String),
            })
            .collect();
        let record = MetaRecord::CreateTable {
            name: plan.table_name.clone(),
            columns,
            sort_key: plan.order_by.clone(),
            rows_per_page_group: ROWS_PER_PAGE_GROUP,
        };
        journal
            .append_commit(&plan.table_name, &record)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
    }

    Ok(())
}
