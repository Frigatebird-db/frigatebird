use crate::page_handler::PageHandler;
use crate::sql::models::FilterExpr;
use crossbeam::channel::{Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

pub type PipelineBatch = Vec<usize>;

#[derive(Clone)]
pub struct PipelineStep {
    pub current_producer: Sender<PipelineBatch>,
    pub previous_receiver: Receiver<PipelineBatch>,
    pub column: String,
    pub filters: Vec<FilterExpr>,
    pub is_root: bool,
    pub table: String,
    pub page_handler: Arc<PageHandler>,
}

impl std::fmt::Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("column", &self.column)
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
            let entries = &page.page.entries;
            let mut passing_rows = Vec::new();

            for (i, entry) in entries.iter().enumerate() {
                let row_id = base_row + i;
                let value = entry.get_data();
                if self
                    .filters
                    .iter()
                    .all(|f| super::filters::eval_filter(f, value))
                {
                    passing_rows.push(row_id);
                }
            }

            if !passing_rows.is_empty() {
                if self.current_producer.send(passing_rows).is_err() {
                    return;
                }
            }

            base_row += entries.len();
        }
        let _ = self.current_producer.send(Vec::new());
    }

    fn execute_non_root(&self) {
        while let Ok(mut batch) = self.previous_receiver.recv() {
            if batch.is_empty() {
                let _ = self.current_producer.send(Vec::new());
                break;
            }

            batch.retain(|&row_id| {
                let row_id_u64 = row_id as u64;
                if let Some(entry) =
                    self.page_handler
                        .read_entry_at(&self.table, &self.column, row_id_u64)
                {
                    let value = entry.get_data();
                    self.filters
                        .iter()
                        .all(|f| super::filters::eval_filter(f, value))
                } else {
                    false
                }
            });

            if batch.is_empty() {
                continue;
            }

            if self.current_producer.send(batch).is_err() {
                break;
            }
        }
    }
}

pub struct Job {
    pub table_name: String,
    pub steps: Vec<PipelineStep>,
    pub cost: usize,
    pub next_free_slot: AtomicUsize,
    pub id: String,
    pub entry_producer: Sender<PipelineBatch>,
}

impl Job {
    pub fn new(
        table_name: String,
        steps: Vec<PipelineStep>,
        entry_producer: Sender<PipelineBatch>,
    ) -> Self {
        let cost = steps.len();
        Job {
            table_name,
            steps,
            cost,
            next_free_slot: AtomicUsize::new(0),
            id: super::builder::generate_pipeline_id(),
            entry_producer,
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
