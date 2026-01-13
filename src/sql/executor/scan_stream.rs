use super::SqlExecutionError;
use super::batch::{ColumnarBatch, ColumnarPage};
use crate::entry::Entry;
use crate::metadata_store::ColumnCatalog;
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::pipeline::{Job, PipelineBatch};
use crossbeam::channel::Receiver;
use std::collections::HashMap;
use std::sync::Arc;

pub trait BatchStream {
    fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError>;
}

pub struct SingleBatchStream {
    batch: Option<ColumnarBatch>,
}

impl SingleBatchStream {
    pub fn new(batch: ColumnarBatch) -> Self {
        Self { batch: Some(batch) }
    }
}

impl BatchStream for SingleBatchStream {
    fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        Ok(self.batch.take())
    }
}

pub fn merge_stream_to_batch(
    mut stream: Box<dyn BatchStream>,
) -> Result<ColumnarBatch, SqlExecutionError> {
    let mut merged = ColumnarBatch::new();
    while let Some(batch) = stream.next_batch()? {
        if merged.num_rows == 0 && merged.columns.is_empty() {
            merged = batch;
        } else {
            merged.append(&batch);
        }
    }
    Ok(merged)
}

pub struct RowIdBatchStream {
    page_handler: Arc<PageHandler>,
    table: String,
    columns: Vec<ColumnCatalog>,
    ordinals: Vec<usize>,
    row_ids: Vec<u64>,
    rows_per_page_group: u64,
    aliases: HashMap<String, usize>,
    position: usize,
}

impl RowIdBatchStream {
    pub fn new(
        page_handler: Arc<PageHandler>,
        table: String,
        columns: Vec<ColumnCatalog>,
        ordinals: Vec<usize>,
        row_ids: Vec<u64>,
        rows_per_page_group: u64,
        aliases: HashMap<String, usize>,
    ) -> Self {
        Self {
            page_handler,
            table,
            columns,
            ordinals,
            row_ids,
            rows_per_page_group,
            aliases,
            position: 0,
        }
    }
}

impl BatchStream for RowIdBatchStream {
    fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        if self.position >= self.row_ids.len() {
            return Ok(None);
        }

        let start = self.position;
        let base_page = self.row_ids[start] / self.rows_per_page_group;
        let mut end = start + 1;
        while end < self.row_ids.len()
            && self.row_ids[end] / self.rows_per_page_group == base_page
        {
            end += 1;
        }

        let slice = &self.row_ids[start..end];
        let mut batch = ColumnarBatch::with_capacity(self.ordinals.len());
        batch.num_rows = slice.len();
        batch.row_ids = slice.to_vec();
        batch.aliases = self.aliases.clone();

        for &ordinal in &self.ordinals {
            let column = self
                .columns
                .get(ordinal)
                .ok_or_else(|| SqlExecutionError::OperationFailed("missing column ordinal".into()))?;
            let page = gather_column_for_rows(
                &self.page_handler,
                &self.table,
                column,
                slice,
            );
            batch.columns.insert(ordinal, page);
        }

        self.position = end;
        Ok(Some(batch))
    }
}

pub struct PipelineBatchStream {
    job: Option<Job>,
    receiver: Receiver<PipelineBatch>,
    executed: bool,
}

impl PipelineBatchStream {
    pub fn new(job: Job, receiver: Receiver<PipelineBatch>) -> Self {
        Self {
            job: Some(job),
            receiver,
            executed: false,
        }
    }

    fn ensure_executed(&mut self) {
        if self.executed {
            return;
        }
        if let Some(job) = self.job.take() {
            let step_count = job.steps.len();
            for _ in 0..step_count {
                job.get_next();
            }
        }
        self.executed = true;
    }
}

impl BatchStream for PipelineBatchStream {
    fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        self.ensure_executed();
        match self.receiver.recv() {
            Ok(batch) if batch.num_rows == 0 => Ok(None),
            Ok(batch) => Ok(Some(batch)),
            Err(_) => Ok(None),
        }
    }
}

fn gather_column_for_rows(
    page_handler: &PageHandler,
    table: &str,
    column: &ColumnCatalog,
    row_ids: &[u64],
) -> ColumnarPage {
    if row_ids.is_empty() {
        return ColumnarPage::empty();
    }

    let min_row = *row_ids.first().expect("row_ids not empty");
    let max_row = *row_ids.last().expect("row_ids not empty");

    let slices = page_handler.list_range_in_table(table, &column.name, min_row, max_row);
    if slices.is_empty() {
        return ColumnarPage::empty();
    }

    let descriptors: Vec<_> = slices.iter().map(|s| s.descriptor.clone()).collect();
    let pages = page_handler.get_pages(descriptors);
    let mut page_map = HashMap::with_capacity(pages.len());
    for page in pages {
        page_map.insert(page.page.page_metadata.clone(), page.page.clone());
    }

    let mut entries = Vec::with_capacity(row_ids.len());
    for &row_id in row_ids {
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
            entries.push(Entry::new(""));
        }
    }

    let disk_page = Page {
        page_metadata: String::new(),
        entries,
    };

    ColumnarPage::load(disk_page, column.data_type)
}
