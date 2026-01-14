use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::SqlExecutionError;

use super::{PipelineBatch, PipelineOperator};

pub struct LimitOperator<'a> {
    executor: &'a SqlExecutor,
    offset: usize,
    limit: Option<usize>,
}

impl<'a> LimitOperator<'a> {
    pub(crate) fn new(executor: &'a SqlExecutor, offset: usize, limit: Option<usize>) -> Self {
        Self {
            executor,
            offset,
            limit,
        }
    }

    pub(crate) fn execute_batches(
        &mut self,
        batches: Vec<PipelineBatch>,
    ) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        self.executor
            .apply_limit_offset(batches.into_iter(), self.offset, self.limit)
    }
}

impl<'a> PipelineOperator for LimitOperator<'a> {
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
