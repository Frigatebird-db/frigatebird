use crate::sql::runtime::SqlExecutionError;
use crate::sql::runtime::limit_exec::apply_limit_offset;

use super::{PipelineBatch, PipelineOperator};

pub struct LimitOperator {
    offset: usize,
    limit: Option<usize>,
    rows_seen: usize,
    rows_emitted: usize,
}

impl LimitOperator {
    pub(crate) fn new(offset: usize, limit: Option<usize>) -> Self {
        Self {
            offset,
            limit,
            rows_seen: 0,
            rows_emitted: 0,
        }
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
        if let Some(limit_value) = self.limit
            && self.rows_emitted >= limit_value
        {
            return Ok(vec![PipelineBatch::new()]);
        }

        let batch_end = self.rows_seen + input.num_rows;
        if batch_end <= self.offset {
            self.rows_seen = batch_end;
            return Ok(Vec::new());
        }

        let mut start_in_batch = 0usize;
        if self.offset > self.rows_seen {
            start_in_batch = self.offset - self.rows_seen;
        }

        let mut end_in_batch = input.num_rows;
        if let Some(limit_value) = self.limit {
            let remaining = limit_value.saturating_sub(self.rows_emitted);
            if remaining == 0 {
                return Ok(vec![PipelineBatch::new()]);
            }
            end_in_batch = (start_in_batch + remaining).min(input.num_rows);
        }

        self.rows_seen = batch_end;
        if start_in_batch >= end_in_batch {
            return Ok(Vec::new());
        }

        let slice = input.slice(start_in_batch, end_in_batch);
        if slice.num_rows > 0 {
            self.rows_emitted += slice.num_rows;
            if let Some(limit_value) = self.limit
                && self.rows_emitted >= limit_value
            {
                return Ok(vec![slice, PipelineBatch::new()]);
            }
            return Ok(vec![slice]);
        }

        Ok(Vec::new())
    }
}
