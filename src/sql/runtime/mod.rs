pub mod aggregates;
pub mod aggregation_helpers;
pub mod batch;
pub mod executor_types;
pub mod executor_utils;
pub mod expressions;
pub mod grouping_helpers;
pub mod helpers;
pub mod ordering;
pub mod physical_evaluator;
pub mod projection_helpers;
pub mod row_functions;
pub mod scalar_functions;
pub mod scan_stream;
pub mod select_helpers;
pub mod values;

mod error;
mod result;

pub use error::SqlExecutionError;
pub use result::{RowIter, SelectResult};
pub(crate) use aggregates::AggregateProjectionPlan;
pub(crate) use executor_types::{AggregatedRow, GroupKey, ProjectionItem, ProjectionPlan};
pub(crate) use ordering::{NullsPlacement, OrderClause, OrderKey};

pub(crate) const WINDOW_BATCH_CHUNK_SIZE: usize = 1_024;
