use crate::metadata_store::TableCatalog;
use crate::sql::runtime::batch::ColumnarBatch;
use crate::sql::runtime::ordering::{MergeOperator, OrderClause, sort_batch_in_memory};
use crate::sql::runtime::spill::SpillManager;
use super::SqlExecutionError;

const SORT_OUTPUT_BATCH_SIZE: usize = 1_024;

pub(crate) fn execute_sort<I>(
    batches: I,
    clauses: &[OrderClause],
    catalog: &TableCatalog,
) -> Result<Vec<ColumnarBatch>, SqlExecutionError>
where
    I: IntoIterator<Item = ColumnarBatch>,
{
    if clauses.is_empty() {
        return Ok(batches
            .into_iter()
            .filter(|batch| batch.num_rows > 0)
            .collect());
    }

    let mut spill_manager = SpillManager::new()
        .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;

    for batch in batches.into_iter() {
        if batch.num_rows == 0 {
            continue;
        }
        let sorted = sort_batch_in_memory(&batch, clauses, catalog)?;
        spill_manager
            .spill_batch(sorted)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
    }

    let runs = spill_manager
        .finish()
        .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
    if runs.len() <= 1 {
        return Ok(runs);
    }

    let mut merge_operator =
        MergeOperator::new(runs, clauses, catalog, SORT_OUTPUT_BATCH_SIZE)?;
    let mut merged_batches = Vec::new();
    while let Some(batch) = merge_operator.next_batch()? {
        if batch.num_rows > 0 {
            merged_batches.push(batch);
        }
    }
    Ok(merged_batches)
}
