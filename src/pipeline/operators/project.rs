use crate::metadata_store::TableCatalog;
use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::{ProjectionPlan, SqlExecutionError};

use super::{PipelineBatch, PipelineOperator};

pub struct ProjectOperator<'a> {
    executor: &'a SqlExecutor,
    projection_plan: &'a ProjectionPlan,
    catalog: &'a TableCatalog,
}

impl<'a> ProjectOperator<'a> {
    pub(crate) fn new(
        executor: &'a SqlExecutor,
        projection_plan: &'a ProjectionPlan,
        catalog: &'a TableCatalog,
    ) -> Self {
        Self {
            executor,
            projection_plan,
            catalog,
        }
    }
}

impl<'a> PipelineOperator for ProjectOperator<'a> {
    fn name(&self) -> &'static str {
        "project"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        if input.num_rows == 0 {
            return Ok(vec![PipelineBatch::new()]);
        }
        let projected = self
            .executor
            .build_projection_batch(&input, self.projection_plan, self.catalog)?;
        Ok(vec![projected])
    }
}
