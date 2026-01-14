use crate::metadata_store::TableCatalog;
use crate::sql::runtime::sort_exec::execute_sort;
use crate::sql::runtime::{OrderClause, SqlExecutionError};

use super::{PipelineBatch, PipelineOperator};

pub struct SortOperator<'a> {
    clauses: &'a [OrderClause],
    catalog: &'a TableCatalog,
}

impl<'a> SortOperator<'a> {
    pub(crate) fn new(clauses: &'a [OrderClause], catalog: &'a TableCatalog) -> Self {
        Self { clauses, catalog }
    }

    pub(crate) fn execute_batches(
        &mut self,
        batches: Vec<PipelineBatch>,
    ) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        execute_sort(batches, self.clauses, self.catalog)
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
        // Sort preserves row_ids alignment with rows.
        self.execute_batches(vec![input])
    }
}
