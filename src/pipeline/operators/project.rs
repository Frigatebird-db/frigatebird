use crate::metadata_store::TableCatalog;
use crate::sql::runtime::projection_exec::build_projection_batch;
use crate::sql::runtime::{ProjectionPlan, SqlExecutionError};

use super::{PipelineBatch, PipelineOperator};

pub struct ProjectOperator<'a> {
    projection_plan: &'a ProjectionPlan,
    catalog: &'a TableCatalog,
}

impl<'a> ProjectOperator<'a> {
    pub(crate) fn new(
        projection_plan: &'a ProjectionPlan,
        catalog: &'a TableCatalog,
    ) -> Self {
        Self {
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
        let projected = build_projection_batch(&input, self.projection_plan, self.catalog)?;
        Ok(vec![projected])
    }
}
