mod aggregates;
mod aggregation_helpers;
mod aggregation_exec;
pub mod batch;
mod executor_types;
mod executor_utils;
mod expressions;
mod grouping_helpers;
pub(crate) mod helpers;
mod ordering;
pub(crate) mod physical_evaluator;
mod projection_helpers;
mod scan_stream;
mod dml;
mod select;
mod select_exec;
mod select_helpers;
mod scan_helpers_exec;
mod row_functions;
mod scalar_functions;
mod scan_helpers;
mod spill;
pub mod values;
mod window_helpers;

use self::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use executor_types::{
    GroupByInfo, GroupKey, GroupingSetPlan, ProjectionItem, VectorAggregationOutput,
};
use executor_utils::{rows_to_batch};
use self::spill::SpillManager;
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{
    ColumnCatalog, ColumnStats, ColumnStatsKind, JournalColumnDef, MetaJournal, MetaRecord,
    PageDescriptor, PageDirectory, ROWS_PER_PAGE_GROUP, TableCatalog,
};
use crate::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::pipeline::{Job, PipelineBatch, PipelineStep};
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::planner::ExpressionPlanner;
use crate::sql::{CreateTablePlan, PlannerError, plan_create_table_statement};
use crate::wal::{FsyncSchedule, ReadConsistency, Walrus};
use crate::writer::{
    ColumnUpdate, DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator,
    UpdateJob, UpdateOp, Writer,
};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, FromTable, Ident, ObjectName, Offset, OrderByExpr, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator, Value,
};
use sqlparser::parser::ParserError;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::sql::types::DataType;
use aggregates::{
    AggregateDataset, AggregateFunctionKind, AggregateFunctionPlan, AggregateProjection,
    AggregationHashTable, MaterializedColumns,
    ensure_state_vec, evaluate_aggregate_outputs, plan_aggregate_projection,
    select_item_contains_aggregate, vectorized_average_update,
    vectorized_count_distinct_update, vectorized_count_star_update,
    vectorized_count_value_update, vectorized_max_update, vectorized_min_update,
    vectorized_sum_update, vectorized_variance_update,
};
use expressions::{evaluate_expression_on_batch, evaluate_row_expr, evaluate_scalar_expression};
use grouping_helpers::{
    evaluate_group_key, evaluate_group_keys_on_batch, evaluate_having, validate_group_by,
};
use helpers::{
    collect_expr_column_ordinals, column_name_from_expr, expr_to_string, object_name_to_string,
    parse_limit, parse_offset, table_with_joins_to_name,
};
use ordering::{
    MergeOperator, build_group_order_key, compare_order_keys,
};

pub(crate) use aggregates::AggregateProjectionPlan;
pub(crate) use executor_types::{AggregatedRow, ProjectionPlan};
pub(crate) use executor_utils::{chunk_batch, deduplicate_batches, merge_batches};
pub(crate) use ordering::{NullsPlacement, OrderClause, sort_batch_in_memory};
pub(crate) use window_helpers::{WindowFunctionPlan, WindowOperator};
use physical_evaluator::PhysicalEvaluator;
use projection_helpers::{build_projection, materialize_columns};
use scan_stream::{
    BatchStream, PipelineBatchStream, PipelineScanBuilder, SingleBatchStream,
};
use scan_helpers::{SortKeyPrefix, collect_sort_key_filters, collect_sort_key_prefixes};
use values::{
    CachedValue, ScalarValue, cached_to_scalar_with_type, compare_strs,
};


#[derive(Debug)]
pub enum SqlExecutionError {
    Parse(ParserError),
    Plan(crate::sql::models::PlannerError),
    Unsupported(String),
    TableNotFound(String),
    ColumnMismatch { table: String, column: String },
    ValueMismatch(String),
    OperationFailed(String),
}

impl std::fmt::Display for SqlExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlExecutionError::Parse(err) => write!(f, "failed to parse SQL: {err}"),
            SqlExecutionError::Plan(err) => write!(f, "{err}"),
            SqlExecutionError::Unsupported(msg) => write!(f, "unsupported SQL: {msg}"),
            SqlExecutionError::TableNotFound(table) => write!(f, "unknown table: {table}"),
            SqlExecutionError::ColumnMismatch { table, column } => {
                write!(f, "column {column} is not defined on table {table}")
            }
            SqlExecutionError::ValueMismatch(msg) => write!(f, "{msg}"),
            SqlExecutionError::OperationFailed(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SqlExecutionError {}

impl From<ParserError> for SqlExecutionError {
    fn from(value: ParserError) -> Self {
        SqlExecutionError::Parse(value)
    }
}

impl From<crate::sql::models::PlannerError> for SqlExecutionError {
    fn from(value: crate::sql::models::PlannerError) -> Self {
        SqlExecutionError::Plan(value)
    }
}

use std::fmt;

#[derive(Clone)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub batches: Vec<ColumnarBatch>,
}

impl SelectResult {
    pub fn row_iter(&self) -> RowIter<'_> {
        RowIter {
            result: self,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|batch| batch.num_rows == 0)
    }
}

impl fmt::Debug for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectResult")
            .field("columns", &self.columns)
            .field("row_count", &self.row_count())
            .finish()
    }
}

impl fmt::Display for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            writeln!(f, "(0 columns)")?;
        } else {
            for (idx, column) in self.columns.iter().enumerate() {
                if idx > 0 {
                    write!(f, " | ")?;
                }
                write!(f, "{column}")?;
            }
            writeln!(f)?;
        }

        let mut row_count = 0usize;
        for batch in &self.batches {
            for row_idx in 0..batch.num_rows {
                row_count += 1;
                for col_idx in 0..self.columns.len() {
                    if col_idx > 0 {
                        write!(f, " | ")?;
                    }
                    let value = batch
                        .columns
                        .get(&col_idx)
                        .ok_or(fmt::Error)?
                        .value_as_string(row_idx);
                    match value {
                        Some(text) => write!(f, "{text}")?,
                        None => write!(f, "NULL")?,
                    }
                }
                writeln!(f)?;
            }
        }

        writeln!(f, "({row_count} rows)")
    }
}


const FULL_SCAN_BATCH_SIZE: u64 = 4_096;
pub(crate) const WINDOW_BATCH_CHUNK_SIZE: usize = 1_024;
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
        match statement {
            Statement::CreateTable { .. } => self.execute_create(statement),
            Statement::Insert { .. } => self.execute_insert(statement),
            Statement::Update {
                table,
                assignments,
                selection,
                returning,
                from,
                ..
            } => self.execute_update(table, assignments, selection, returning, from),
            Statement::Delete {
                tables,
                from,
                using,
                selection,
                returning,
                order_by,
                limit,
                ..
            } => self.execute_delete(tables, from, using, selection, returning, order_by, limit),
            other => Err(SqlExecutionError::Unsupported(format!(
                "{other:?} is not supported yet"
            ))),
        }
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

        match statements.remove(0) {
            Statement::Query(query) => self.execute_select(*query),
            other => Err(SqlExecutionError::Unsupported(format!(
                "{other:?} is not supported yet"
            ))),
        }
    }

    // select.rs and select_exec.rs provide the SELECT flow.

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

    fn apply_qualify_filter(
        &self,
        batch: ColumnarBatch,
        expr: &Expr,
        catalog: &TableCatalog,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        let filter_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
        let bitmap = boolean_bitmap_from_page(&filter_page)?;
        if bitmap.count_ones() == batch.num_rows {
            return Ok(batch);
        }
        if bitmap.count_ones() == 0 {
            return Ok(ColumnarBatch::new());
        }
        Ok(batch.filter_by_bitmap(&bitmap))
    }

    pub(crate) fn apply_filter_expr(
        &self,
        batch: ColumnarBatch,
        expr: &Expr,
        physical_expr: Option<&PhysicalExpr>,
        catalog: &TableCatalog,
        table: &str,
        columns: &[ColumnCatalog],
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        if batch.num_rows == 0 {
            return Ok(batch);
        }

        if let Some(physical_expr) = physical_expr {
            let bitmap = PhysicalEvaluator::evaluate_filter(physical_expr, &batch);
            return Ok(batch.filter_by_bitmap(&bitmap));
        }

        match evaluate_expression_on_batch(expr, &batch, catalog) {
            Ok(filter_page) => {
                let bitmap = boolean_bitmap_from_page(&filter_page)?;
                Ok(batch.filter_by_bitmap(&bitmap))
            }
            Err(SqlExecutionError::Unsupported(_)) => {
                let ordinals = collect_expr_column_ordinals(expr, column_ordinals, table)?;
                let materialized = materialize_columns(
                    &self.page_handler,
                    table,
                    columns,
                    &ordinals,
                    &batch.row_ids,
                )?;
                let matching_rows = filter_rows_with_expr(
                    expr,
                    &batch.row_ids,
                    &materialized,
                    column_ordinals,
                    column_types,
                    false,
                )?;
                if matching_rows.is_empty() {
                    return Ok(ColumnarBatch::new());
                }
                let matching: HashSet<u64> = matching_rows.into_iter().collect();
                let mut bitmap = Bitmap::new(batch.num_rows);
                for (idx, row_id) in batch.row_ids.iter().enumerate() {
                    if matching.contains(row_id) {
                        bitmap.set(idx);
                    }
                }
                Ok(batch.filter_by_bitmap(&bitmap))
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn execute_sort<I>(
        &self,
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

    fn execute_create(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        let plan: CreateTablePlan = plan_create_table_statement(&statement)?;
        create_table_from_plan(&self.page_directory, &plan)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;

        // Write table schema to metadata journal for crash recovery
        if let Some(ref journal) = self.meta_journal {
            let columns: Vec<JournalColumnDef> = plan
                .columns
                .iter()
                .map(|spec| JournalColumnDef {
                    name: spec.name.clone(),
                    data_type: crate::sql::types::DataType::from_sql(&spec.data_type)
                        .unwrap_or(crate::sql::types::DataType::String),
                })
                .collect();
            let record = MetaRecord::CreateTable {
                name: plan.table_name.clone(),
                columns,
                sort_key: plan.order_by.clone(),
                rows_per_page_group: ROWS_PER_PAGE_GROUP,
            };
            journal
                .append_commit(&plan.table_name, &record)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }

        Ok(())
    }

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

fn collect_physical_ordinals(expr: &PhysicalExpr, ordinals: &mut BTreeSet<usize>) {
    match expr {
        PhysicalExpr::Column { index, .. } => {
            ordinals.insert(*index);
        }
        PhysicalExpr::BinaryOp { left, right, .. } => {
            collect_physical_ordinals(left, ordinals);
            collect_physical_ordinals(right, ordinals);
        }
        PhysicalExpr::UnaryOp { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Like {
            expr,
            pattern,
            case_insensitive: _,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::RLike {
            expr,
            pattern,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::InList { expr, list, .. } => {
            collect_physical_ordinals(expr, ordinals);
            for item in list {
                collect_physical_ordinals(item, ordinals);
            }
        }
        PhysicalExpr::Cast { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::IsNull(expr) | PhysicalExpr::IsNotNull(expr) => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Literal(_) => {}
    }
}

fn filter_rows_with_expr(
    expr: &Expr,
    rows: &[u64],
    materialized: &MaterializedColumns,
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
    prefer_exact_numeric: bool,
) -> Result<Vec<u64>, SqlExecutionError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let dataset = AggregateDataset {
        rows,
        materialized,
        column_ordinals,
        column_types,
        masked_exprs: None,
        prefer_exact_numeric,
    };

    let mut filtered = Vec::with_capacity(rows.len());
    for &row_idx in rows {
        let value = evaluate_row_expr(expr, row_idx, &dataset)?;
        if value.as_bool().unwrap_or(false) {
            filtered.push(row_idx);
        }
    }

    Ok(filtered)
}

// Legacy row-id refinement path; unused after pipeline refactor.
/*
fn refine_rows_with_vectorized_filter(
    page_handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    expr: &PhysicalExpr,
    column_ordinals: &HashMap<String, usize>,
    candidate_rows: &[u64],
    rows_per_page_group: u64,
) -> Result<Vec<u64>, SqlExecutionError> {
    if candidate_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ordinals = BTreeSet::new();
    collect_physical_ordinals(expr, &mut ordinals);
    if ordinals.is_empty() {
        return Ok(candidate_rows.to_vec());
    }

    let mut rows = Vec::new();
    let mut idx = 0;
    while idx < candidate_rows.len() {
        let row = candidate_rows[idx];
        let page_idx = (row / rows_per_page_group) as usize;
        let page_base = (page_idx as u64) * rows_per_page_group;

        let mut page_rows = Vec::new();
        while idx < candidate_rows.len()
            && (candidate_rows[idx] / rows_per_page_group) as usize == page_idx
        {
            page_rows.push((candidate_rows[idx] - page_base) as usize);
            idx += 1;
        }

        let mut batch_slice = ColumnarBatch::with_capacity(ordinals.len());
        let mut pages_keepalive = Vec::with_capacity(ordinals.len());
        let mut num_rows = 0;
        let mut segment_valid = true;

        for &ordinal in &ordinals {
            let column = &columns[ordinal];
            let col_descriptors = page_handler.list_pages_in_table(table, &column.name);
            if let Some(desc) = col_descriptors.get(page_idx) {
                let page_arc = page_handler
                    .get_page(desc.clone())
                    .ok_or_else(|| SqlExecutionError::OperationFailed("page load failed".into()))?;
                num_rows = page_arc.page.num_rows;
                pages_keepalive.push((ordinal, page_arc));
            } else {
                segment_valid = false;
                break;
            }
        }

        if !segment_valid || num_rows == 0 {
            continue;
        }

        for (ordinal, page_arc) in &pages_keepalive {
            batch_slice.columns.insert(*ordinal, page_arc.page.clone());
        }
        batch_slice.num_rows = num_rows;
        batch_slice.aliases = column_ordinals.clone();

        let mut bitmap = PhysicalEvaluator::evaluate_filter(expr, &batch_slice);
        let mut candidate_bitmap = Bitmap::new(num_rows);
        for offset in page_rows {
            candidate_bitmap.set(offset);
        }
        bitmap.and(&candidate_bitmap);

        for offset in bitmap.iter_ones() {
            rows.push(page_base + offset as u64);
        }
    }

    Ok(rows)
}
*/

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

fn boolean_bitmap_from_page(page: &ColumnarPage) -> Result<Bitmap, SqlExecutionError> {
    match &page.data {
        ColumnData::Text(col) => {
            let mut bitmap = Bitmap::new(page.len());
            for idx in 0..col.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let value = col.get_bytes(idx);
                if value.eq_ignore_ascii_case(b"true") {
                    bitmap.set(idx);
                }
            }
            Ok(bitmap)
        }
        _ => Err(SqlExecutionError::Unsupported(
            "QUALIFY expressions must produce boolean results".into(),
        )),
    }
}

pub struct RowIter<'a> {
    result: &'a SelectResult,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = RowView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.batch_idx < self.result.batches.len() {
            let batch = &self.result.batches[self.batch_idx];
            if self.row_idx >= batch.num_rows {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }
            let view = RowView {
                batch,
                row_idx: self.row_idx,
                column_count: self.result.columns.len(),
            };
            self.row_idx += 1;
            return Some(view);
        }
        None
    }
}

pub struct RowView<'a> {
    batch: &'a ColumnarBatch,
    row_idx: usize,
    column_count: usize,
}

impl<'a> RowView<'a> {
    pub fn value_as_string(&self, column_idx: usize) -> Option<String> {
        self.batch
            .columns
            .get(&column_idx)
            .and_then(|page| page.value_as_string(self.row_idx))
    }

    pub fn to_vec(&self) -> Vec<Option<String>> {
        let mut row = Vec::with_capacity(self.column_count);
        for column_idx in 0..self.column_count {
            row.push(
                self.batch
                    .columns
                    .get(&column_idx)
                    .and_then(|page| page.value_as_string(self.row_idx)),
            );
        }
        row
    }
}

// Legacy batch-to-row helpers; unused after pipeline refactor.
/*
fn batch_to_rows(batch: &ColumnarBatch) -> Vec<Vec<Option<String>>> {
    if batch.num_rows == 0 {
        return Vec::new();
    }
    let column_count = batch.columns.len();
    let mut rows = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut row = Vec::with_capacity(column_count);
        for column_idx in 0..column_count {
            let column = batch
                .columns
                .get(&column_idx)
                .expect("missing projection column in batch");
            row.push(column.value_as_string(row_idx));
        }
        rows.push(row);
    }
    rows
}

fn batches_to_rows(batches: &[ColumnarBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch_to_rows(batch));
    }
    rows
}
*/

#[derive(Debug, Clone)]
struct PagePrunePredicate {
    column: String,
    comparison: PagePruneComparison,
    value: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
enum PagePruneComparison {
    GreaterThan { inclusive: bool },
    LessThan { inclusive: bool },
    Equal,
    IsNull,
    IsNotNull,
}

fn extract_page_prunable_predicates(expr: &Expr) -> Option<Vec<PagePrunePredicate>> {
    let mut predicates = Vec::new();
    if gather_numeric_prunable_predicates(expr, &mut predicates) && !predicates.is_empty() {
        Some(predicates)
    } else {
        None
    }
}

fn gather_numeric_prunable_predicates(expr: &Expr, acc: &mut Vec<PagePrunePredicate>) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                gather_numeric_prunable_predicates(left, acc)
                    && gather_numeric_prunable_predicates(right, acc)
            }
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Eq => build_prunable_comparison(left, op, right, acc),
            _ => false,
        },
        Expr::IsNull(inner) => build_null_prunable_predicate(inner, true, acc),
        Expr::IsNotNull(inner) => build_null_prunable_predicate(inner, false, acc),
        Expr::Nested(inner) => gather_numeric_prunable_predicates(inner, acc),
        _ => false,
    }
}

fn build_prunable_comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(left) {
        if let Some(value) = parse_numeric_literal(right) {
            if let Some(comparison) = comparison_for_operator(op, true) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    if let Some(column) = column_name_from_expr(right) {
        if let Some(value) = parse_numeric_literal(left) {
            if let Some(comparison) = comparison_for_operator(op, false) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    false
}

fn build_null_prunable_predicate(
    expr: &Expr,
    expect_null: bool,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(expr) {
        acc.push(PagePrunePredicate {
            column,
            comparison: if expect_null {
                PagePruneComparison::IsNull
            } else {
                PagePruneComparison::IsNotNull
            },
            value: None,
        });
        return true;
    }
    false
}

fn comparison_for_operator(
    op: &BinaryOperator,
    column_on_left: bool,
) -> Option<PagePruneComparison> {
    match op {
        BinaryOperator::Gt => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: false }
        } else {
            PagePruneComparison::LessThan { inclusive: false }
        }),
        BinaryOperator::GtEq => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: true }
        } else {
            PagePruneComparison::LessThan { inclusive: true }
        }),
        BinaryOperator::Lt => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: false }
        } else {
            PagePruneComparison::GreaterThan { inclusive: false }
        }),
        BinaryOperator::LtEq => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: true }
        } else {
            PagePruneComparison::GreaterThan { inclusive: true }
        }),
        BinaryOperator::Eq => Some(PagePruneComparison::Equal),
        _ => None,
    }
}

fn parse_numeric_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<f64>().ok(),
        Expr::Value(Value::SingleQuotedString(text)) => text.parse::<f64>().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => parse_numeric_literal(expr).map(|value| -value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => parse_numeric_literal(expr),
        Expr::Nested(inner) => parse_numeric_literal(inner),
        _ => None,
    }
}

fn should_prune_page(
    page_idx: usize,
    predicates: &[PagePrunePredicate],
    descriptor_map: &HashMap<usize, Vec<PageDescriptor>>,
    column_ordinals: &HashMap<String, usize>,
) -> bool {
    for predicate in predicates {
        let ordinal = match column_ordinals.get(&predicate.column) {
            Some(ord) => *ord,
            None => continue,
        };
        let descriptors = match descriptor_map.get(&ordinal) {
            Some(list) => list,
            None => continue,
        };
        let descriptor = match descriptors.get(page_idx) {
            Some(desc) => desc,
            None => continue,
        };
        if let Some(stats) = &descriptor.stats {
            match predicate.comparison {
                PagePruneComparison::IsNull => {
                    if stats.null_count == 0 {
                        return true;
                    }
                }
                PagePruneComparison::IsNotNull => {
                    if stats.null_count == descriptor.entry_count {
                        return true;
                    }
                }
                _ => {
                    if matches!(
                        stats.kind,
                        ColumnStatsKind::Int64 | ColumnStatsKind::Float64
                    ) && predicate_disqualifies(stats, predicate)
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn predicate_disqualifies(stats: &ColumnStats, predicate: &PagePrunePredicate) -> bool {
    let value = match predicate.value {
        Some(v) => v,
        None => return false,
    };
    let min = stats
        .min_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());
    let max = stats
        .max_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());

    match predicate.comparison {
        PagePruneComparison::GreaterThan { inclusive } => match max {
            Some(max_val) => {
                if inclusive {
                    max_val < value
                } else {
                    max_val <= value
                }
            }
            None => false,
        },
        PagePruneComparison::LessThan { inclusive } => match min {
            Some(min_val) => {
                if inclusive {
                    min_val > value
                } else {
                    min_val >= value
                }
            }
            None => false,
        },
        PagePruneComparison::Equal => match (min, max) {
            (Some(min_val), Some(max_val)) => value < min_val || value > max_val,
            _ => false,
        },
        PagePruneComparison::IsNull | PagePruneComparison::IsNotNull => false,
    }
}

#[cfg(test)]
mod pruning_tests {
    use super::*;
    use std::collections::HashMap;

    fn column_expr(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn number_expr(value: &str) -> Expr {
        Expr::Value(Value::Number(value.into(), false))
    }

    #[test]
    fn extract_prunable_predicates_from_conjunction() {
        let greater_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Gt,
            right: Box::new(number_expr("100")),
        };
        let less_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::LtEq,
            right: Box::new(number_expr("500")),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(greater_expr),
            op: BinaryOperator::And,
            right: Box::new(less_expr),
        };

        let predicates = extract_page_prunable_predicates(&expr).expect("predicates");
        assert_eq!(predicates.len(), 2);
        assert!(matches!(
            predicates[0].comparison,
            PagePruneComparison::GreaterThan { inclusive: false }
        ));
        assert!(matches!(
            predicates[1].comparison,
            PagePruneComparison::LessThan { inclusive: true }
        ));
    }

    #[test]
    fn unsupported_expression_returns_none() {
        let expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Or,
            right: Box::new(number_expr("10")),
        };
        assert!(extract_page_prunable_predicates(&expr).is_none());
    }

    fn descriptor_with_stats(stats: ColumnStats, entry_count: u64) -> PageDescriptor {
        PageDescriptor {
            id: "test".into(),
            disk_path: "/tmp/pg".into(),
            offset: 0,
            alloc_len: 0,
            actual_len: 0,
            entry_count,
            data_type: crate::sql::types::DataType::Int64,
            stats: Some(stats),
        }
    }

    #[test]
    fn should_prune_when_range_disjoint() {
        let stats = ColumnStats {
            min_value: Some("0".into()),
            max_value: Some("10".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::GreaterThan { inclusive: false },
            value: Some(25.0),
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn does_not_prune_when_overlap_exists() {
        let stats = ColumnStats {
            min_value: Some("5".into()),
            max_value: Some("50".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::LessThan { inclusive: true },
            value: Some(30.0),
        };
        assert!(!should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn prunes_is_null_when_page_has_no_nulls() {
        let stats = ColumnStats {
            min_value: Some("1".into()),
            max_value: Some("2".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 8);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNull,
            value: None,
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn prunes_is_not_null_when_page_all_nulls() {
        let stats = ColumnStats {
            min_value: None,
            max_value: None,
            null_count: 4,
            kind: ColumnStatsKind::Text,
        };
        let descriptor = descriptor_with_stats(stats, 4);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNotNull,
            value: None,
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }
}
