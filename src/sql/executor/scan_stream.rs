use super::SqlExecutionError;
use super::batch::{ColumnarBatch, ColumnarPage};
use crate::entry::Entry;
use crate::metadata_store::ColumnCatalog;
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::pipeline::{Job, PipelineBatch, PipelineStep};
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use std::collections::BTreeSet;
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

pub struct PipelineScanBuilder<'a> {
    page_handler: Arc<PageHandler>,
    table: &'a str,
    columns: &'a [ColumnCatalog],
    scan_ordinals: &'a BTreeSet<usize>,
    selection_expr: Option<&'a PhysicalExpr>,
}

impl<'a> PipelineScanBuilder<'a> {
    pub fn new(
        page_handler: Arc<PageHandler>,
        table: &'a str,
        columns: &'a [ColumnCatalog],
        scan_ordinals: &'a BTreeSet<usize>,
        selection_expr: Option<&'a PhysicalExpr>,
    ) -> Self {
        Self {
            page_handler,
            table,
            columns,
            scan_ordinals,
            selection_expr,
        }
    }

    pub fn build(self) -> Result<Option<PipelineBatchStream>, SqlExecutionError> {
        let mut ordinals: Vec<usize> = self.scan_ordinals.iter().copied().collect();
        if ordinals.is_empty() {
            return Ok(None);
        }
        ordinals.sort_unstable();

        let mut steps = Vec::new();
        let (entry_producer, mut previous_receiver) =
            crossbeam::channel::unbounded::<PipelineBatch>();
        let mut is_root = true;

        for &ordinal in &ordinals {
            let column = self.columns.get(ordinal).ok_or_else(|| {
                SqlExecutionError::OperationFailed("invalid scan ordinal".into())
            })?;
            let (current_producer, next_receiver) =
                crossbeam::channel::unbounded::<PipelineBatch>();
            steps.push(PipelineStep::new(
                self.table.to_string(),
                column.name.clone(),
                ordinal,
                Vec::new(),
                is_root,
                Arc::clone(&self.page_handler),
                current_producer,
                previous_receiver,
            ));
            previous_receiver = next_receiver;
            is_root = false;
        }

        if let Some(expr) = self.selection_expr {
            let (current_producer, next_receiver) =
                crossbeam::channel::unbounded::<PipelineBatch>();
            let filter_step = PipelineStep::new(
                self.table.to_string(),
                self.columns[ordinals[0]].name.clone(),
                ordinals[0],
                vec![FilterExpr::leaf(expr.clone())],
                false,
                Arc::clone(&self.page_handler),
                current_producer,
                previous_receiver,
            );
            steps.push(filter_step);
            previous_receiver = next_receiver;
        }

        let output_receiver = previous_receiver;
        let job = Job::new(
            self.table.to_string(),
            steps,
            entry_producer,
            output_receiver.clone(),
        );
        Ok(Some(PipelineBatchStream::new(job, output_receiver)))
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
    let mut entries = Vec::with_capacity(row_ids.len());
    for &row_id in row_ids {
        if let Some(entry) = page_handler.read_entry_at(table, &column.name, row_id) {
            entries.push(entry);
        } else {
            entries.push(Entry::new(""));
        }
    }

    let disk_page = Page {
        page_metadata: String::new(),
        entries,
    };

    ColumnarPage::load(disk_page, column.data_type)
}
