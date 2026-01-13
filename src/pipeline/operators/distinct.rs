use crate::sql::executor::{deduplicate_batches, SqlExecutionError};

use super::{PipelineBatch, PipelineOperator};

pub struct DistinctOperator {
    column_count: usize,
}

impl DistinctOperator {
    pub(crate) fn new(column_count: usize) -> Self {
        Self { column_count }
    }

    pub(crate) fn execute_batches(
        &mut self,
        batches: Vec<PipelineBatch>,
    ) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        Ok(deduplicate_batches(batches, self.column_count))
    }
}

impl PipelineOperator for DistinctOperator {
    fn name(&self) -> &'static str {
        "distinct"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        if input.num_rows == 0 {
            return Ok(vec![PipelineBatch::new()]);
        }
        self.execute_batches(vec![input])
    }
}
