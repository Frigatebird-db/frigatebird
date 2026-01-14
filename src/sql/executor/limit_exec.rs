use super::batch::ColumnarBatch;
use super::{SqlExecutionError, SqlExecutor};

impl SqlExecutor {
    pub(crate) fn apply_limit_offset<I>(
        &self,
        batches: I,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<ColumnarBatch>, SqlExecutionError>
    where
        I: IntoIterator<Item = ColumnarBatch>,
    {
        let mut rows_seen = 0usize;
        let mut rows_emitted = 0usize;
        let mut limited_batches = Vec::new();

        for batch in batches.into_iter() {
            if batch.num_rows == 0 {
                continue;
            }

            if let Some(limit_value) = limit {
                if rows_emitted >= limit_value {
                    break;
                }
            }

            let batch_end = rows_seen + batch.num_rows;
            if batch_end <= offset {
                rows_seen = batch_end;
                continue;
            }

            let mut start_in_batch = 0usize;
            if offset > rows_seen {
                start_in_batch = offset - rows_seen;
            }

            let mut end_in_batch = batch.num_rows;
            if let Some(limit_value) = limit {
                let remaining = limit_value.saturating_sub(rows_emitted);
                if remaining == 0 {
                    break;
                }
                end_in_batch = (start_in_batch + remaining).min(batch.num_rows);
            }

            if start_in_batch >= end_in_batch {
                rows_seen = batch_end;
                continue;
            }

            let slice = batch.slice(start_in_batch, end_in_batch);
            if slice.num_rows > 0 {
                rows_emitted += slice.num_rows;
                limited_batches.push(slice);
            }
            rows_seen = batch_end;

            if let Some(limit_value) = limit {
                if rows_emitted >= limit_value {
                    break;
                }
            }
        }

        Ok(limited_batches)
    }
}
