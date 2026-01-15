use super::SqlExecutionError;
use super::batch::ColumnarBatch;
use crate::metadata_store::ColumnCatalog;
use crate::page_handler::PageHandler;
use crate::executor::PipelineExecutor;
use crate::pipeline::{Job, PagePruneOp, PagePrunePredicate, PipelineBatch, PipelineStep};
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::runtime::physical_evaluator::reverse_operator;
use crossbeam::channel::Receiver;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
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

static SELECT_SCAN_ACTIVE: AtomicBool = AtomicBool::new(false);

pub fn is_select_scan_in_progress() -> bool {
    SELECT_SCAN_ACTIVE.load(Ordering::Relaxed)
}

// Legacy row-id stream removed after pipeline row-id scans.

pub struct PipelineBatchStream {
    job: Option<Job>,
    receiver: Receiver<PipelineBatch>,
    executed: bool,
    executor: Option<Arc<PipelineExecutor>>,
}

impl PipelineBatchStream {
    pub fn new(
        job: Job,
        receiver: Receiver<PipelineBatch>,
        executor: Option<Arc<PipelineExecutor>>,
    ) -> Self {
        Self {
            job: Some(job),
            receiver,
            executed: false,
            executor,
        }
    }

    fn ensure_executed(&mut self) {
        if self.executed {
            return;
        }
        SELECT_SCAN_ACTIVE.store(true, Ordering::Relaxed);
        if let Some(job) = self.job.take() {
            if let Some(executor) = self.executor.as_ref() {
                executor.submit(job);
            } else {
                let step_count = job.steps.len();
                for _ in 0..step_count {
                    job.get_next();
                }
            }
        }
        self.executed = true;
    }
}

impl BatchStream for PipelineBatchStream {
    fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        self.ensure_executed();
        match self.receiver.recv() {
            Ok(batch) if batch.num_rows == 0 => {
                SELECT_SCAN_ACTIVE.store(false, Ordering::Relaxed);
                Ok(None)
            }
            Ok(batch) => Ok(Some(batch)),
            Err(_) => {
                SELECT_SCAN_ACTIVE.store(false, Ordering::Relaxed);
                Ok(None)
            }
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
    executor: Option<Arc<PipelineExecutor>>,
}

impl<'a> PipelineScanBuilder<'a> {
    pub fn new(
        page_handler: Arc<PageHandler>,
        table: &'a str,
        columns: &'a [ColumnCatalog],
        scan_ordinals: &'a BTreeSet<usize>,
        selection_expr: Option<&'a PhysicalExpr>,
        row_ids: Option<Arc<Vec<u64>>>,
        executor: Option<Arc<PipelineExecutor>>,
    ) -> Self {
        Self {
            page_handler,
            table,
            columns,
            scan_ordinals,
            selection_expr,
            row_ids,
            executor,
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
            let mut prune_predicates = Vec::new();
            if is_root {
                if let Some(expr) = self.selection_expr {
                    collect_prunable_predicates(expr, ordinal, &mut prune_predicates);
                }
            }
            steps.push(PipelineStep::new(
                self.table.to_string(),
                column.name.clone(),
                ordinal,
                Vec::new(),
                prune_predicates,
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
                Vec::new(),
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
        Ok(Some(PipelineBatchStream::new(
            job,
            output_receiver,
            self.executor,
        )))
    }
}

// Legacy gather_column_for_rows helper removed after pipeline row-id scans.

fn collect_prunable_predicates(
    expr: &PhysicalExpr,
    root_ordinal: usize,
    out: &mut Vec<PagePrunePredicate>,
) {
    match expr {
        PhysicalExpr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                collect_prunable_predicates(left, root_ordinal, out);
                collect_prunable_predicates(right, root_ordinal, out);
            }
            sqlparser::ast::BinaryOperator::Or => {}
            sqlparser::ast::BinaryOperator::Eq
            | sqlparser::ast::BinaryOperator::Gt
            | sqlparser::ast::BinaryOperator::GtEq
            | sqlparser::ast::BinaryOperator::Lt
            | sqlparser::ast::BinaryOperator::LtEq => {
                if let Some(predicate) =
                    prune_from_binary(left, op, right, root_ordinal)
                {
                    out.push(predicate);
                }
            }
            _ => {}
        },
        PhysicalExpr::IsNull(inner) => {
            if matches!(
                inner.as_ref(),
                PhysicalExpr::Column { index, .. } if *index == root_ordinal
            ) {
                out.push(PagePrunePredicate {
                    op: PagePruneOp::IsNull,
                    value: None,
                });
            }
        }
        PhysicalExpr::IsNotNull(inner) => {
            if matches!(
                inner.as_ref(),
                PhysicalExpr::Column { index, .. } if *index == root_ordinal
            ) {
                out.push(PagePrunePredicate {
                    op: PagePruneOp::IsNotNull,
                    value: None,
                });
            }
        }
        PhysicalExpr::Cast { expr, .. } => collect_prunable_predicates(expr, root_ordinal, out),
        _ => {}
    }
}

fn prune_from_binary(
    left: &PhysicalExpr,
    op: &sqlparser::ast::BinaryOperator,
    right: &PhysicalExpr,
    root_ordinal: usize,
) -> Option<PagePrunePredicate> {
    if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(value)) = (left, right)
        && *index == root_ordinal
    {
        return prune_predicate_for_op(op, value.clone());
    }

    if let (PhysicalExpr::Literal(value), PhysicalExpr::Column { index, .. }) = (left, right)
        && *index == root_ordinal
        && let Some(reversed) = reverse_operator(op)
    {
        return prune_predicate_for_op(&reversed, value.clone());
    }

    None
}

fn prune_predicate_for_op(
    op: &sqlparser::ast::BinaryOperator,
    value: crate::sql::runtime::values::ScalarValue,
) -> Option<PagePrunePredicate> {
    let prune_op = match op {
        sqlparser::ast::BinaryOperator::Eq => PagePruneOp::Eq,
        sqlparser::ast::BinaryOperator::Gt => PagePruneOp::Gt,
        sqlparser::ast::BinaryOperator::GtEq => PagePruneOp::GtEq,
        sqlparser::ast::BinaryOperator::Lt => PagePruneOp::Lt,
        sqlparser::ast::BinaryOperator::LtEq => PagePruneOp::LtEq,
        _ => return None,
    };
    Some(PagePrunePredicate {
        op: prune_op,
        value: Some(value),
    })
}
