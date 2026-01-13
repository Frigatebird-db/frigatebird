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

    pub(crate) fn execute_batches(
        &mut self,
        batches: Vec<PipelineBatch>,
    ) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        self.executor
            .execute_sort(batches.into_iter(), self.clauses, self.catalog)
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
        self.execute_batches(vec![input])
    }
}
