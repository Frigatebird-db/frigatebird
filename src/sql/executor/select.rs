use super::{SqlExecutionError, SqlExecutor, SelectResult};
use crate::pipeline::select_planner::execute_select_plan;
use sqlparser::ast::Query;

impl SqlExecutor {
    pub(crate) fn execute_select(&self, mut query: Query) -> Result<SelectResult, SqlExecutionError> {
        execute_select_plan(self, query)
    }
}
