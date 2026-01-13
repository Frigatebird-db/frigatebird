use crate::metadata_store::TableCatalog;
use crate::sql::executor::{OrderClause, SqlExecutionError, SqlExecutor};

use super::{PipelineBatch, PipelineOperator};

pub struct SortOperator<'a> {
    executor: &'a SqlExecutor,
    clauses: &'a [OrderClause],
    catalog: &'a TableCatalog,
}

impl<'a> SortOperator<'a> {
    pub(crate) fn new(
        executor: &'a SqlExecutor,
        clauses: &'a [OrderClause],
        catalog: &'a TableCatalog,
    ) -> Self {
        Self {
            executor,
            clauses,
            catalog,
        }
    }
}

impl<'a> PipelineOperator for SortOperator<'a> {
    fn name(&self) -> &'static str {
        "sort"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        if input.num_rows == 0 {
            return Ok(vec![PipelineBatch::new()]);
        }
        self.executor
            .execute_sort(std::iter::once(input), self.clauses, self.catalog)
    }
}
