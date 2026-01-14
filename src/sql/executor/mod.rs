mod dml;
mod select;

pub use crate::sql::runtime::{RowIter, SelectResult, SqlExecutionError};

use crate::sql::runtime::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use crate::sql::runtime::executor_types::{GroupByInfo, GroupingSetPlan, VectorAggregationOutput};
use crate::sql::runtime::executor_utils::rows_to_batch;
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{
    ColumnCatalog, JournalColumnDef, MetaJournal, MetaRecord, PageDirectory, ROWS_PER_PAGE_GROUP,
    TableCatalog,
};
use crate::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crate::wal::{FsyncSchedule, ReadConsistency, Walrus};
use crate::writer::{
    ColumnUpdate, DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator,
    UpdateJob, UpdateOp, Writer,
};
use sqlparser::ast::{
    Assignment, Expr, FromTable, ObjectName, Offset, OrderByExpr, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::sql::types::DataType;
use crate::sql::runtime::aggregates::{
    AggregateDataset, AggregateFunctionKind, AggregateFunctionPlan, AggregateProjection,
    AggregationHashTable, MaterializedColumns,
    ensure_state_vec, evaluate_aggregate_outputs, plan_aggregate_projection,
    select_item_contains_aggregate, vectorized_average_update,
    vectorized_count_distinct_update, vectorized_count_star_update,
    vectorized_count_value_update, vectorized_max_update, vectorized_min_update,
    vectorized_sum_update, vectorized_variance_update,
};
use crate::sql::runtime::expressions::{evaluate_row_expr, evaluate_scalar_expression};
use crate::sql::runtime::grouping_helpers::{evaluate_group_key, evaluate_having, validate_group_by};
use crate::sql::runtime::helpers::{
    collect_expr_column_ordinals, expr_to_string, object_name_to_string, table_with_joins_to_name,
};

pub(crate) use crate::sql::runtime::aggregates::AggregateProjectionPlan;
pub(crate) use crate::sql::runtime::executor_types::{
    AggregatedRow, GroupKey, ProjectionItem, ProjectionPlan,
};
pub(crate) use crate::sql::runtime::executor_utils::{chunk_batch, deduplicate_batches, merge_batches};
pub(crate) use crate::sql::runtime::helpers::{parse_limit, parse_offset};
pub(crate) use crate::sql::runtime::ordering::{
    NullsPlacement, OrderClause, OrderKey, build_order_keys_on_batch, compare_order_keys,
    sort_batch_in_memory,
};
pub(crate) use crate::sql::runtime::expressions::evaluate_expression_on_batch;
pub(crate) use crate::sql::runtime::grouping_helpers::evaluate_group_keys_on_batch;
use crate::sql::runtime::projection_helpers::build_projection;
use crate::sql::runtime::scan_stream::{
    BatchStream, PipelineBatchStream, PipelineScanBuilder, SingleBatchStream,
};


const FULL_SCAN_BATCH_SIZE: u64 = 4_096;
const SORT_OUTPUT_BATCH_SIZE: usize = 1_024;

static SQL_EXECUTOR_WAL_COUNTER: AtomicUsize = AtomicUsize::new(0);
const SQL_EXECUTOR_WAL_PREFIX: &str = "sql-executor-";

pub struct SqlExecutor {
    page_handler: Arc<PageHandler>,
    page_directory: Arc<PageDirectory>,
    writer: Arc<Writer>,
    meta_journal: Option<Arc<MetaJournal>>,
    use_writer_inserts: bool,
    wal_namespace: String,
    meta_namespace: String,
    cleanup_wal_on_drop: bool,
}

impl SqlExecutor {
    pub(crate) fn table_catalog(&self, table: &str) -> Option<TableCatalog> {
        self.page_directory.table_catalog(table)
    }

    pub(crate) fn page_handler(&self) -> &Arc<PageHandler> {
        &self.page_handler
    }

    pub(crate) fn writer(&self) -> &Writer {
        &self.writer
    }

    pub(crate) fn use_writer_inserts(&self) -> bool {
        self.use_writer_inserts
    }

    pub(crate) fn page_directory(&self) -> &Arc<PageDirectory> {
        &self.page_directory
    }

    pub(crate) fn meta_journal(&self) -> Option<&MetaJournal> {
        self.meta_journal.as_ref().map(|journal| journal.as_ref())
    }
    pub fn new(page_handler: Arc<PageHandler>, page_directory: Arc<PageDirectory>) -> Self {
        Self::new_with_writer_mode(page_handler, page_directory, true)
    }

    pub fn new_with_writer_mode(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
    ) -> Self {
        let wal_id = SQL_EXECUTOR_WAL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let wal_namespace = format!("{SQL_EXECUTOR_WAL_PREFIX}{wal_id}");
        let options = SqlExecutorWalOptions::new(wal_namespace);
        SqlExecutor::with_wal_options(page_handler, page_directory, use_writer_inserts, options)
    }

    pub fn with_wal_options(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
        options: SqlExecutorWalOptions,
    ) -> Self {
        let SqlExecutorWalOptions {
            namespace,
            cleanup_on_drop,
            reset_namespace,
            storage_dir,
            fsync_schedule,
            wal_enabled,
        } = options;
        let allocator: Arc<dyn PageAllocator> = if let Some(dir) = storage_dir {
            Arc::new(DirectBlockAllocator::with_data_dir(dir).expect("allocator init failed"))
        } else {
            Arc::new(DirectBlockAllocator::new().expect("allocator init failed"))
        };
        ensure_sql_executor_wal_root();
        let meta_namespace = format!("{namespace}-meta");
        if reset_namespace {
            remove_sql_executor_wal_dir(&namespace);
            remove_sql_executor_wal_dir(&meta_namespace);
        }
        let wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &namespace,
                ReadConsistency::StrictlyAtOnce,
                fsync_schedule,
            )
            .expect("wal init failed"),
        );
        let meta_wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &meta_namespace,
                ReadConsistency::AtLeastOnce {
                    persist_every: u32::MAX,
                },
                FsyncSchedule::SyncEach,
            )
            .expect("metadata wal init failed"),
        );
        let meta_journal = Arc::new(MetaJournal::new(Arc::clone(&meta_wal), 16));
        meta_journal
            .replay_into(&page_directory)
            .expect("metadata journal replay failed");
        let metadata_client: Arc<dyn MetadataClient> = Arc::new(DirectoryMetadataClient::new(
            Arc::clone(&page_directory),
            Arc::clone(&meta_journal),
        ));

        let shard_count = crate::writer::GLOBAL_WRITER_SHARD_COUNT
            .load(AtomicOrdering::Acquire)
            .max(1);

        let writer = Arc::new(Writer::with_shard_count(
            Arc::clone(&page_handler),
            allocator,
            metadata_client,
            wal,
            shard_count,
            wal_enabled,
        ));

        SqlExecutor {
            page_handler,
            page_directory,
            writer,
            meta_journal: Some(meta_journal),
            use_writer_inserts,
            wal_namespace: namespace,
            meta_namespace,
            cleanup_wal_on_drop: cleanup_on_drop,
        }
    }

    pub fn flush_table(&self, table: &str) -> Result<(), SqlExecutionError> {
        self.writer.flush_table(table).map_err(|err| {
            SqlExecutionError::OperationFailed(format!("writer flush failed for {table}: {err:?}"))
        })
    }

    pub fn execute(&self, sql: &str) -> Result<(), SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }
        let statement = statements.remove(0);
        crate::pipeline::dispatcher::execute_statement(self, statement)?;
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<SelectResult, SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }

        let statement = statements.remove(0);
        match crate::pipeline::dispatcher::execute_statement(self, statement)? {
            Some(result) => Ok(result),
            None => Err(SqlExecutionError::Unsupported(
                "expected query statement".into(),
            )),
        }
    }

    // select.rs and select_exec.rs provide the SELECT flow.

    // dml.rs provides impl SqlExecutor for DML flow.
    // scan_helpers_exec.rs provides impl SqlExecutor for scan/index helpers.

    fn estimate_table_row_count(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
    ) -> Result<u64, SqlExecutionError> {
        for column in columns {
            if let Some(descriptor) = self
                .page_handler
                .locate_latest_in_table(table, &column.name)
            {
                return Ok(descriptor.entry_count);
            }
        }
        Ok(0)
    }
}

// Legacy row-id refinement path removed after pipeline refactor.

impl Drop for SqlExecutor {
    fn drop(&mut self) {
        if self.cleanup_wal_on_drop {
            remove_sql_executor_wal_dir(&self.wal_namespace);
            remove_sql_executor_wal_dir(&self.meta_namespace);
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlExecutorWalOptions {
    namespace: String,
    cleanup_on_drop: bool,
    reset_namespace: bool,
    storage_dir: Option<String>,
    fsync_schedule: FsyncSchedule,
    wal_enabled: bool,
}

impl SqlExecutorWalOptions {
    pub fn new(namespace: impl Into<String>) -> Self {
        SqlExecutorWalOptions {
            namespace: namespace.into(),
            cleanup_on_drop: true,
            reset_namespace: true,
            storage_dir: None,
            fsync_schedule: FsyncSchedule::SyncEach,
            wal_enabled: true,
        }
    }

    pub fn cleanup_on_drop(mut self, value: bool) -> Self {
        self.cleanup_on_drop = value;
        self
    }

    pub fn reset_namespace(mut self, value: bool) -> Self {
        self.reset_namespace = value;
        self
    }

    pub fn storage_dir(mut self, dir: impl Into<String>) -> Self {
        self.storage_dir = Some(dir.into());
        self
    }

    pub fn fsync_schedule(mut self, schedule: FsyncSchedule) -> Self {
        self.fsync_schedule = schedule;
        self
    }

    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

fn sql_executor_wal_base_dir() -> PathBuf {
    std::env::var("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("wal_files"))
}

fn ensure_sql_executor_wal_root() {
    let _ = fs::create_dir_all(sql_executor_wal_base_dir());
}

fn remove_sql_executor_wal_dir(namespace: &str) {
    let dir = sql_executor_wal_base_dir().join(namespace);
    if dir.exists() {
        let _ = fs::remove_dir_all(dir);
    }
}

// Legacy batch-to-row helpers removed after pipeline refactor.
