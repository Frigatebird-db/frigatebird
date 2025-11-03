use crate::page_handler::PageHandler;
use crate::sql::models::FilterExpr;
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;

pub struct PipelineBatch {
    pub rows: Vec<u64>,
}

pub struct PipelineStep {
    /// Produces batches for the downstream step.
    pub current_producer: Sender<PipelineBatch>,
    /// Receives batches produced by the previous step.
    pub previous_receiver: Receiver<PipelineBatch>,
    /// The column this step filters on
    pub column: String,
    /// The filter expressions to apply to this column
    pub filters: Vec<FilterExpr>,
    /// Indicates whether this is the first step in the job chain
    pub is_root: bool,
    /// The table this step operates on
    pub table: String,
    /// Page handler for accessing column data
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

        let mut base_row = 0u64;
        for page in pages {
            let entries = &page.page.entries;
            let mut passing_rows = Vec::new();

            for (i, entry) in entries.iter().enumerate() {
                let row_id = base_row + i as u64;
                let value = entry.get_data();
                if self.filters.iter().all(|f| super::filters::eval_filter(f, value)) {
                    passing_rows.push(row_id);
                }
            }

            if !passing_rows.is_empty() {
                let batch = PipelineBatch { rows: passing_rows };
                if self.current_producer.send(batch).is_err() {
                    return;
                }
            }

            base_row += entries.len() as u64;
        }
        drop(self.current_producer.clone());
    }

    fn execute_non_root(&self) {
        while let Ok(mut batch) = self.previous_receiver.recv() {
            batch.rows.retain(|&row_id| {
                if let Some(entry) = self
                    .page_handler
                    .read_entry_at(&self.table, &self.column, row_id)
                {
                    let value = entry.get_data();
                    self.filters.iter().all(|f| super::filters::eval_filter(f, value))
                } else {
                    false
                }
            });

            if !batch.rows.is_empty() && self.current_producer.send(batch).is_err() {
                return;
            }
        }
    }
}

pub struct Job {
    pub pipeline_id: String,
    pub steps: Vec<PipelineStep>,
    pub final_receiver: Option<Receiver<PipelineBatch>>,
}

impl Job {
    pub fn new(pipeline_id: String) -> Self {
        Self {
            pipeline_id,
            steps: Vec::new(),
            final_receiver: None,
        }
    }

    pub fn add_step(&mut self, step: PipelineStep) {
        self.steps.push(step);
    }

    pub fn set_final_receiver(&mut self, receiver: Receiver<PipelineBatch>) {
        self.final_receiver = Some(receiver);
    }

    pub fn execute(self) -> Vec<u64> {
        let handles: Vec<_> = self
            .steps
            .into_iter()
            .map(|step| {
                std::thread::spawn(move || {
                    step.execute();
                })
            })
            .collect();

        let mut results = Vec::new();
        if let Some(receiver) = self.final_receiver {
            while let Ok(batch) = receiver.recv() {
                results.extend(batch.rows);
            }
        }

        for handle in handles {
            let _ = handle.join();
        }

        results
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.pipeline_id == other.pipeline_id
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
