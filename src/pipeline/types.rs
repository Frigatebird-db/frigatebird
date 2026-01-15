use crate::metadata_store::{ColumnStats, ColumnStatsKind, PageDescriptor, ROWS_PER_PAGE_GROUP};
use crate::page_handler::PageHandler;
use crate::sql::models::FilterExpr;
use crate::sql::runtime::batch::{Bitmap, ColumnarBatch, ColumnarPage};
use crate::sql::runtime::physical_evaluator::PhysicalEvaluator;
use crate::sql::runtime::values::ScalarValue;
use crossbeam::channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

pub trait PipelineStepInterface: Send + Sync {
    fn execute(&self);
    fn debug_label(&self) -> &str;
}

fn evaluate_filters(filters: &[FilterExpr], batch: &ColumnarBatch) -> Bitmap {
    if filters.is_empty() {
        let mut bitmap = Bitmap::new(batch.num_rows);
        bitmap.fill(true);
        return bitmap;
    }
    let mut bitmap = evaluate_filter_expr_with_batch(&filters[0], batch);
    for filter in filters.iter().skip(1) {
        let rhs = evaluate_filter_expr_with_batch(filter, batch);
        bitmap.and(&rhs);
    }
    bitmap
}

fn evaluate_filter_expr_with_batch(filter: &FilterExpr, batch: &ColumnarBatch) -> Bitmap {
    match filter {
        FilterExpr::Leaf(expr) => PhysicalEvaluator::evaluate_filter(expr, batch),
        FilterExpr::And(filters) => {
            if filters.is_empty() {
                let mut bitmap = Bitmap::new(batch.num_rows);
                bitmap.fill(true);
                return bitmap;
            }
            let mut bitmap = evaluate_filter_expr_with_batch(&filters[0], batch);
            for expr in filters.iter().skip(1) {
                let rhs = evaluate_filter_expr_with_batch(expr, batch);
                bitmap.and(&rhs);
            }
            bitmap
        }
        FilterExpr::Or(filters) => {
            let mut bitmap = Bitmap::new(batch.num_rows);
            for expr in filters {
                let rhs = evaluate_filter_expr_with_batch(expr, batch);
                bitmap.or(&rhs);
            }
            bitmap
        }
    }
}

pub type PipelineBatch = ColumnarBatch;

#[derive(Clone)]
pub struct PipelineStep {
    pub current_producer: Sender<PipelineBatch>,
    pub previous_receiver: Receiver<PipelineBatch>,
    pub column: String,
    pub column_ordinal: usize,
    pub filters: Vec<FilterExpr>,
    pub prune_predicates: Vec<PagePrunePredicate>,
    pub is_root: bool,
    pub table: String,
    pub page_handler: Arc<PageHandler>,
    pub row_ids: Option<Arc<Vec<u64>>>,
}

impl std::fmt::Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("column", &self.column)
            .field("column_ordinal", &self.column_ordinal)
            .field("filters", &self.filters)
            .field("prune_predicates", &self.prune_predicates)
            .field("is_root", &self.is_root)
            .field("table", &self.table)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum PagePruneOp {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug)]
pub struct PagePrunePredicate {
    pub op: PagePruneOp,
    pub value: Option<ScalarValue>,
}

impl PipelineStep {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: String,
        column: String,
        column_ordinal: usize,
        filters: Vec<FilterExpr>,
        prune_predicates: Vec<PagePrunePredicate>,
        is_root: bool,
        page_handler: Arc<PageHandler>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
        row_ids: Option<Arc<Vec<u64>>>,
    ) -> Self {
        Self {
            current_producer,
            previous_receiver,
            column,
            column_ordinal,
            filters,
            prune_predicates,
            is_root,
            table,
            page_handler,
            row_ids,
        }
    }

    pub fn execute(&self) {
        if self.is_root {
            self.execute_root();
        } else {
            self.execute_non_root();
        }
    }

    fn execute_root(&self) {
        if let Some(row_ids) = self.row_ids.as_ref() {
            self.execute_root_for_row_ids(row_ids);
            return;
        }
        let descriptors = self
            .page_handler
            .list_pages_in_table(&self.table, &self.column);
        let descriptors = if self.prune_predicates.is_empty() {
            descriptors
        } else {
            descriptors
                .into_iter()
                .filter(|descriptor| {
                    !should_prune_descriptor(descriptor, &self.prune_predicates)
                })
                .collect()
        };
        let page_ids: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
        self.page_handler.ensure_pages_cached(&page_ids);
        let pages = self.page_handler.get_pages(descriptors);

        let mut base_row = 0usize;
        for page in pages {
            let page_len = page.page.num_rows;
            if page_len == 0 {
                continue;
            }

            let mut batch = ColumnarBatch::new();
            batch.num_rows = page_len;
            batch.columns.insert(self.column_ordinal, page.page.clone());
            batch.row_ids = (base_row as u64..(base_row + page_len) as u64).collect();

            let bitmap = evaluate_filters(&self.filters, &batch);
            let filtered_batch = batch.filter_by_bitmap(&bitmap);

            if filtered_batch.num_rows > 0 && self.current_producer.send(filtered_batch).is_err() {
                return;
            }

            base_row += page_len;
        }
        let _ = self.current_producer.send(ColumnarBatch::new());
    }

    fn execute_root_for_row_ids(&self, row_ids: &[u64]) {
        if row_ids.is_empty() {
            let _ = self.current_producer.send(ColumnarBatch::new());
            return;
        }

        let rows_per_page_group = self
            .page_handler
            .table_catalog(&self.table)
            .map(|catalog| catalog.rows_per_page_group)
            .unwrap_or(ROWS_PER_PAGE_GROUP);

        let mut idx = 0;
        while idx < row_ids.len() {
            let start = idx;
            let base_page = row_ids[start] / rows_per_page_group;
            let mut end = start + 1;
            while end < row_ids.len() && row_ids[end] / rows_per_page_group == base_page {
                end += 1;
            }

            let slice = &row_ids[start..end];
            let mut batch = ColumnarBatch::new();
            batch.num_rows = slice.len();
            batch.row_ids = slice.to_vec();
            batch.columns.insert(
                self.column_ordinal,
                materialize_column_in_batch(&self.page_handler, &self.table, &self.column, slice),
            );

            let bitmap = evaluate_filters(&self.filters, &batch);
            let filtered_batch = batch.filter_by_bitmap(&bitmap);

            if filtered_batch.num_rows > 0 && self.current_producer.send(filtered_batch).is_err() {
                return;
            }

            idx = end;
        }

        let _ = self.current_producer.send(ColumnarBatch::new());
    }

    fn execute_non_root(&self) {
        let mut sent_termination = false;
        while let Ok(mut batch) = self.previous_receiver.recv() {
            if batch.num_rows == 0 {
                if self.current_producer.send(ColumnarBatch::new()).is_err() {
                    return;
                }
                sent_termination = true;
                break;
            }

            // Ensure column is loaded
            if !batch.columns.contains_key(&self.column_ordinal) {
                let column_data = materialize_column_in_batch(
                    &self.page_handler,
                    &self.table,
                    &self.column,
                    &batch.row_ids,
                );
                batch.columns.insert(self.column_ordinal, column_data);
            }

            let page = batch
                .columns
                .get(&self.column_ordinal)
                .expect("column just materialized");
            let bitmap = evaluate_filters(&self.filters, &batch);
            let filtered_batch = batch.filter_by_bitmap(&bitmap);

            if filtered_batch.num_rows > 0 && self.current_producer.send(filtered_batch).is_err() {
                sent_termination = true;
                break;
            }
        }
        if !sent_termination {
            let _ = self.current_producer.send(ColumnarBatch::new());
        }
    }
}

fn should_prune_descriptor(
    descriptor: &PageDescriptor,
    predicates: &[PagePrunePredicate],
) -> bool {
    let Some(stats) = &descriptor.stats else {
        return false;
    };
    for predicate in predicates {
        if predicate_prunes(stats, descriptor.entry_count, predicate) {
            return true;
        }
    }
    false
}

fn predicate_prunes(
    stats: &ColumnStats,
    entry_count: u64,
    predicate: &PagePrunePredicate,
) -> bool {
    match predicate.op {
        PagePruneOp::IsNull => {
            if stats.null_count == 0 {
                return true;
            }
        }
        PagePruneOp::IsNotNull => {
            if stats.null_count == entry_count {
                return true;
            }
        }
        PagePruneOp::Eq
        | PagePruneOp::Gt
        | PagePruneOp::GtEq
        | PagePruneOp::Lt
        | PagePruneOp::LtEq => match stats.kind {
            ColumnStatsKind::Int64 | ColumnStatsKind::Float64 => {
                let value = predicate.value.as_ref().and_then(scalar_to_f64);
                let Some(value) = value else {
                    return false;
                };
                let min = stats
                    .min_value
                    .as_ref()
                    .and_then(|value| value.parse::<f64>().ok());
                let max = stats
                    .max_value
                    .as_ref()
                    .and_then(|value| value.parse::<f64>().ok());
                return range_prunes_numeric(min, max, value, &predicate.op);
            }
            ColumnStatsKind::Text => {
                let Some(value) = predicate.value.as_ref().and_then(scalar_to_text) else {
                    return false;
                };
                let min = stats.min_value.as_deref();
                let max = stats.max_value.as_deref();
                return range_prunes_text(min, max, value, &predicate.op);
            }
        },
    }
    false
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int64(v) => Some(*v as f64),
        ScalarValue::Float64(v) => Some(*v),
        ScalarValue::Timestamp(v) => Some(*v as f64),
        _ => None,
    }
}

fn scalar_to_text(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::String(text) => Some(text.as_str()),
        ScalarValue::Boolean(value) => Some(if *value { "true" } else { "false" }),
        _ => None,
    }
}

fn range_prunes_numeric(
    min: Option<f64>,
    max: Option<f64>,
    value: f64,
    op: &PagePruneOp,
) -> bool {
    match op {
        PagePruneOp::Eq => match (min, max) {
            (Some(min_val), Some(max_val)) => value < min_val || value > max_val,
            _ => false,
        },
        PagePruneOp::Gt => max.map_or(false, |max_val| max_val <= value),
        PagePruneOp::GtEq => max.map_or(false, |max_val| max_val < value),
        PagePruneOp::Lt => min.map_or(false, |min_val| min_val >= value),
        PagePruneOp::LtEq => min.map_or(false, |min_val| min_val > value),
        _ => false,
    }
}

fn range_prunes_text(
    min: Option<&str>,
    max: Option<&str>,
    value: &str,
    op: &PagePruneOp,
) -> bool {
    let cmp_min = min.map(|min_val| value.as_bytes().cmp(min_val.as_bytes()));
    let cmp_max = max.map(|max_val| value.as_bytes().cmp(max_val.as_bytes()));
    match op {
        PagePruneOp::Eq => match (cmp_min, cmp_max) {
            (Some(min_cmp), Some(max_cmp)) => {
                min_cmp == std::cmp::Ordering::Less || max_cmp == std::cmp::Ordering::Greater
            }
            _ => false,
        },
        PagePruneOp::Gt => cmp_max.map_or(false, |cmp| cmp != std::cmp::Ordering::Less),
        PagePruneOp::GtEq => cmp_max.map_or(false, |cmp| cmp == std::cmp::Ordering::Greater),
        PagePruneOp::Lt => cmp_min.map_or(false, |cmp| cmp != std::cmp::Ordering::Greater),
        PagePruneOp::LtEq => cmp_min.map_or(false, |cmp| cmp == std::cmp::Ordering::Less),
        _ => false,
    }
}

impl PipelineStepInterface for PipelineStep {
    fn execute(&self) {
        PipelineStep::execute(self);
    }

    fn debug_label(&self) -> &str {
        &self.column
    }
}

fn materialize_column_in_batch(
    page_handler: &PageHandler,
    table: &str,
    column: &str,
    row_ids: &[u64],
) -> ColumnarPage {
    page_handler.gather_column_for_rows(table, column, row_ids)
}

pub struct Job {
    pub table_name: String,
    pub steps: Vec<PipelineStep>,
    pub cost: usize,
    pub next_free_slot: AtomicUsize,
    pub id: String,
    pub entry_producer: Sender<PipelineBatch>,
    pub output_receiver: Receiver<PipelineBatch>,
}

impl Job {
    pub fn new(
        table_name: String,
        steps: Vec<PipelineStep>,
        entry_producer: Sender<PipelineBatch>,
        output_receiver: Receiver<PipelineBatch>,
    ) -> Self {
        let cost = steps.len();
        Job {
            table_name,
            steps,
            cost,
            next_free_slot: AtomicUsize::new(0),
            id: super::builder::generate_pipeline_id(),
            entry_producer,
            output_receiver,
        }
    }

    pub fn get_next(&self) {
        let total = self.steps.len();
        if total == 0 {
            return;
        }

        let mut slot = self.next_free_slot.load(AtomicOrdering::Relaxed);
        loop {
            if slot >= total {
                return;
            }

            match self.next_free_slot.compare_exchange_weak(
                slot,
                slot + 1,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    self.steps[slot].execute();
                    return;
                }
                Err(current) => {
                    slot = current;
                }
            }
        }
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by cost first (fewer steps = higher priority)
        // Then by table name for deterministic ordering
        self.cost
            .cmp(&other.cost)
            .then_with(|| self.table_name.cmp(&other.table_name))
    }
}
