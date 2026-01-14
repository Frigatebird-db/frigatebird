use crate::sql::runtime::SqlExecutionError;
use crate::sql::runtime::batch::ColumnarBatch;

pub mod aggregate;
pub mod distinct;
pub mod filter;
pub mod limit;
pub mod project;
pub mod sort;
pub mod window;

pub type PipelineBatch = ColumnarBatch;

pub trait PipelineOperator {
    fn name(&self) -> &'static str;
    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError>;
}

pub use aggregate::AggregateOperator;
pub use distinct::DistinctOperator;
pub use filter::FilterOperator;
pub use limit::LimitOperator;
pub use project::ProjectOperator;
pub use sort::SortOperator;
pub use window::WindowOperator;
