use crate::sql::runtime::SqlExecutionError;
use crate::sql::runtime::limit_exec::apply_limit_offset;

use super::{PipelineBatch, PipelineOperator};

pub struct LimitOperator {
    offset: usize,
    limit: Option<usize>,
}

impl LimitOperator {
    pub(crate) fn new(offset: usize, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }

    pub(crate) fn execute_batches(
        &mut self,
        batches: Vec<PipelineBatch>,
    ) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        apply_limit_offset(batches, self.offset, self.limit)
    }
}

impl PipelineOperator for LimitOperator {
    fn name(&self) -> &'static str {
        "limit"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        if input.num_rows == 0 {
            return Ok(vec![PipelineBatch::new()]);
        }
        // Limit/offset slices batches, preserving row_ids for kept rows.
        self.execute_batches(vec![input])
    }
}
