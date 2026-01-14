use super::batch::ColumnarBatch;
use super::executor_types::ProjectionPlan;
use super::expressions::evaluate_expression_on_batch;
use super::{ProjectionItem, SqlExecutionError, SqlExecutor};
use crate::metadata_store::TableCatalog;

impl SqlExecutor {
    pub(crate) fn build_projection_batch(
        &self,
        batch: &ColumnarBatch,
        projection_plan: &ProjectionPlan,
        catalog: &TableCatalog,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        let mut final_batch = ColumnarBatch::with_capacity(projection_plan.items.len());
        final_batch.num_rows = batch.num_rows;
        final_batch.row_ids = batch.row_ids.clone();
        for (idx, item) in projection_plan.items.iter().enumerate() {
            let column_page = match item {
                ProjectionItem::Direct { ordinal } => {
                    batch.columns.get(ordinal).cloned().ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing column ordinal {ordinal} in vectorized batch"
                        ))
                    })?
                }
                ProjectionItem::Computed { expr } => {
                    evaluate_expression_on_batch(expr, batch, catalog)?
                }
            };
            final_batch.columns.insert(idx, column_page);
        }
        Ok(final_batch)
    }
}
