pub mod aggregates;
pub mod aggregation_exec;
pub mod aggregation_helpers;
pub mod batch;
pub mod executor_types;
pub mod executor_utils;
pub mod expressions;
pub mod grouping_helpers;
pub mod helpers;
pub mod limit_exec;
pub mod ordering;
pub mod page_pruning;
pub mod physical_evaluator;
pub mod physical_ordinals;
pub mod projection_exec;
pub mod projection_helpers;
pub mod row_functions;
pub mod scan_helpers;
pub mod scalar_functions;
pub mod scan_stream;
pub mod sort_exec;
pub mod select_helpers;
pub mod values;
pub mod spill;

mod error;
mod result;

pub use error::SqlExecutionError;
pub use result::{RowIter, SelectResult};
pub(crate) use aggregates::AggregateProjectionPlan;
pub(crate) use executor_types::{AggregatedRow, GroupKey, ProjectionItem, ProjectionPlan};
pub(crate) use ordering::{NullsPlacement, OrderClause, OrderKey};

pub(crate) const WINDOW_BATCH_CHUNK_SIZE: usize = 1_024;
