use crate::metadata_store::ColumnCatalog;
use crate::sql::runtime::{SelectResult, SqlExecutionError};

use super::core::SqlExecutor;

impl SqlExecutor {
    pub fn flush_table(&self, table: &str) -> Result<(), SqlExecutionError> {
        self.writer.flush_table(table).map_err(|err| {
            SqlExecutionError::OperationFailed(format!("writer flush failed for {table}: {err:?}"))
        })
    }

    pub fn execute(&self, sql: &str) -> Result<(), SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }

        let statement = statements.remove(0);
        crate::pipeline::dispatcher::execute_statement(self, statement)?;
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<SelectResult, SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }

        let statement = statements.remove(0);
        match crate::pipeline::dispatcher::execute_statement(self, statement)? {
            Some(result) => Ok(result),
            None => Err(SqlExecutionError::Unsupported(
                "expected query statement".into(),
            )),
        }
    }

    fn estimate_table_row_count(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
    ) -> Result<u64, SqlExecutionError> {
        crate::sql::runtime::row_counts::estimate_table_row_count(
            &self.page_handler,
            table,
            columns,
        )
    }
}
