use super::SqlExecutionError;
use super::batch::ColumnarBatch;
use crate::metadata_store::ColumnCatalog;
use crate::page_handler::PageHandler;
use crate::pipeline::{Job, PipelineBatch, PipelineStep};
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crossbeam::channel::Receiver;
use std::collections::BTreeSet;
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

pub fn collect_stream_batches(
    mut stream: Box<dyn BatchStream>,
) -> Result<Vec<ColumnarBatch>, SqlExecutionError> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch()? {
        if batch.num_rows > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

// Legacy row-id stream removed after pipeline row-id scans.

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
    row_ids: Option<Arc<Vec<u64>>>,
}

impl<'a> PipelineScanBuilder<'a> {
    pub fn new(
        page_handler: Arc<PageHandler>,
        table: &'a str,
        columns: &'a [ColumnCatalog],
        scan_ordinals: &'a BTreeSet<usize>,
        selection_expr: Option<&'a PhysicalExpr>,
        row_ids: Option<Arc<Vec<u64>>>,
    ) -> Self {
        Self {
            page_handler,
            table,
            columns,
            scan_ordinals,
            selection_expr,
            row_ids,
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
            let column = self
                .columns
                .get(ordinal)
                .ok_or_else(|| SqlExecutionError::OperationFailed("invalid scan ordinal".into()))?;
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
                if is_root { self.row_ids.clone() } else { None },
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
                None,
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

// Legacy gather_column_for_rows helper removed after pipeline row-id scans.
