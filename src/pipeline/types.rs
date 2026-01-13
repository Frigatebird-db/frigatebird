use crate::page_handler::PageHandler;
use crate::sql::executor::batch::{Bitmap, ColumnarBatch, ColumnarPage};
use crate::sql::executor::physical_evaluator::PhysicalEvaluator;
use crate::sql::models::FilterExpr;
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
    pub is_root: bool,
    pub table: String,
    pub page_handler: Arc<PageHandler>,
}

impl std::fmt::Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("column", &self.column)
            .field("column_ordinal", &self.column_ordinal)
            .field("filters", &self.filters)
            .field("is_root", &self.is_root)
            .field("table", &self.table)
            .finish()
    }
}

impl PipelineStep {
    pub fn new(
        table: String,
        column: String,
        column_ordinal: usize,
        filters: Vec<FilterExpr>,
        is_root: bool,
        page_handler: Arc<PageHandler>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
    ) -> Self {
        Self {
            current_producer,
            previous_receiver,
            column,
            column_ordinal,
            filters,
            is_root,
            table,
            page_handler,
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
        let descriptors = self
            .page_handler
            .list_pages_in_table(&self.table, &self.column);
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
            batch
                .columns
                .insert(self.column_ordinal, page.page.clone());
            batch.row_ids = (base_row as u64..(base_row + page_len) as u64).collect();

            let bitmap = evaluate_filters(&self.filters, &batch);
            let filtered_batch = batch.filter_by_bitmap(&bitmap);

            if filtered_batch.num_rows > 0 {
                if self.current_producer.send(filtered_batch).is_err() {
                    return;
                }
            }

            base_row += page_len;
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

            if filtered_batch.num_rows > 0 {
                if self.current_producer.send(filtered_batch).is_err() {
                    sent_termination = true;
                    break;
                }
            }
        }
        if !sent_termination {
            let _ = self.current_producer.send(ColumnarBatch::new());
        }
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
    if row_ids.is_empty() {
        return ColumnarPage::empty();
    }
    
    // In a real vectorized system, we would batch this fetch more efficiently.
    // For now, we reuse the existing page scatter-gather logic but build a ColumnarPage.
    
    // We can reuse the page_handler logic to fetch ranges of rows
    // Since rows are likely somewhat sequential from the scan, we can optimize.
    
    let min_row = *row_ids.iter().min().unwrap();
    let max_row = *row_ids.iter().max().unwrap();
    
    let slices = page_handler.list_range_in_table(table, column, min_row, max_row);
    let descriptors: Vec<_> = slices.iter().map(|s| s.descriptor.clone()).collect();
    let pages = page_handler.get_pages(descriptors);
    
    let mut page_map = HashMap::with_capacity(pages.len());
    for p in pages {
        page_map.insert(p.page.page_metadata.clone(), p.page.clone());
    }
    
    let catalog = page_handler.table_catalog(table).expect("missing catalog");
    let col_cat = catalog.column(column).expect("missing column");
    
    // Reconstruct a single columnar page from the pieces
    // This is essentially "gather"
    
    // Note: We need to build a single ColumnarPage that matches `row_ids` order.
    // Ideally ColumnarBatch supports multiple pages per column (ChunkedArray),
    // but our current definition is one Page per Column per Batch.
    // So we must concat/gather.

    // 1. Collect all values
    let mut entries = Vec::with_capacity(row_ids.len());
    
    for &row_id in row_ids {
        // Find which page contains this row_id
        // This is slow O(N*M) but correct for now. Optimization: lock-step iterator.
        // Given row_ids are sorted in the pipeline usually, we can optimize.
        
        let mut found = false;
        for slice in &slices {
             if row_id >= slice.start_row_offset && row_id < slice.end_row_offset {
                 if let Some(page) = page_map.get(&slice.descriptor.id) {
                     let idx = (row_id - slice.start_row_offset) as usize;
                     if let Some(entry) = page.entry_at(idx) {
                         entries.push(entry);
                         found = true;
                     }
                 }
                 break;
             }
        }
        if !found {
            // Should not happen in valid pipeline
            entries.push(crate::entry::Entry::new("")); // Null/Empty fallback
        }
    }
    
    // 2. Convert entries back to ColumnarPage
    // This double conversion (Page -> Entry -> Page) is inefficient but safe for this refactor step.
    // A better way is ColumnarPage::gather but we need cross-page gather.
    
    let disk_page = crate::page::Page {
        page_metadata: String::new(),
        entries,
    };
    
    ColumnarPage::load(disk_page, col_cat.data_type)
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
