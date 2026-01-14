use crate::metadata_store::TableCatalog;
use crate::pipeline::window_helpers::{WindowFunctionPlan, WindowOperator as ExecWindowOperator};
use crate::sql::runtime::executor_utils::{chunk_batch, merge_batches};
use crate::sql::runtime::ordering::{NullsPlacement, OrderClause, sort_batch_in_memory};
use crate::sql::runtime::{SqlExecutionError, WINDOW_BATCH_CHUNK_SIZE};
use sqlparser::ast::Expr;

use super::{PipelineBatch, PipelineOperator};

pub struct WindowOperator<'a> {
    catalog: &'a TableCatalog,
    window_plans: Vec<WindowFunctionPlan>,
    partition_exprs: Vec<Expr>,
}

impl<'a> WindowOperator<'a> {
    pub(crate) fn new(
        window_plans: Vec<WindowFunctionPlan>,
        partition_exprs: Vec<Expr>,
        catalog: &'a TableCatalog,
    ) -> Self {
        Self {
            catalog,
            window_plans,
            partition_exprs,
        }
    }
}

impl<'a> PipelineOperator for WindowOperator<'a> {
    fn name(&self) -> &'static str {
        "window"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        if input.num_rows == 0 {
            return Ok(vec![PipelineBatch::new()]);
        }

        let mut batch = input;
        // Window computation preserves row_ids alignment; partition sorting reorders rows.
        if !self.partition_exprs.is_empty() {
            let partition_clauses: Vec<OrderClause> = self
                .partition_exprs
                .iter()
                .map(|expr| OrderClause {
                    expr: expr.clone(),
                    descending: false,
                    nulls: NullsPlacement::Default,
                })
                .collect();
            batch = sort_batch_in_memory(&batch, &partition_clauses, self.catalog)?;
        }

        let window_plans = std::mem::take(&mut self.window_plans);
        let partition_exprs = std::mem::take(&mut self.partition_exprs);
        let chunks = chunk_batch(&batch, WINDOW_BATCH_CHUNK_SIZE);
        let mut operator = ExecWindowOperator::new(
            chunks.into_iter(),
            window_plans,
            partition_exprs,
            self.catalog,
        );
        let mut processed_batches = Vec::new();
        while let Some(processed) = operator.next_batch()? {
            processed_batches.push(processed);
        }
        Ok(vec![merge_batches(processed_batches)])
    }
}
