use crate::page_handler::PageHandler;
use crate::sql::models::{FilterExpr, QueryPlan};
use crossbeam::channel::{self, Receiver, Sender};
use rand::{Rng, distributions::Alphanumeric};
use sqlparser::ast::Expr;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

/// Lightweight batch placeholder flowing between pipeline stages.
pub type PipelineBatch = Vec<usize>;

/// Represents a single step in the execution pipeline that filters on a specific column.
#[derive(Clone)]
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
        column: String,
        filters: Vec<FilterExpr>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
        is_root: bool,
        table: String,
        page_handler: Arc<PageHandler>,
    ) -> Self {
        PipelineStep {
            current_producer,
            previous_receiver,
            column,
            filters,
            is_root,
            table,
            page_handler,
        }
    }

    /// Placeholder execution loop for step-level runtime.
    pub fn execute(&self) {
        let batch = if self.is_root {
            Some(Vec::new())
        } else {
            self.previous_receiver.recv().ok()
        };

        if let Some(mut rows) = batch {
            self.apply_filters(&mut rows);
            let _ = self.current_producer.send(rows);
        }
    }

    fn apply_filters(&self, rows: &mut PipelineBatch) {
        if self.filters.is_empty() || rows.is_empty() {
            return;
        }

        // FAST PATH: Batch fetch all needed pages once
        let first_row = *rows.first().unwrap() as u64;
        let last_row = *rows.last().unwrap() as u64;

        let slices = self.page_handler.list_range_in_table(
            &self.table,
            &self.column,
            first_row,
            last_row,
        );

        if slices.is_empty() {
            rows.clear();
            return;
        }

        let pages = self.page_handler.get_pages(
            slices.iter().map(|s| s.descriptor.clone()).collect()
        );

        // Build row->value lookup (amortized O(1) per row)
        let mut row_values: HashMap<usize, &str> = HashMap::with_capacity(rows.len());
        let mut cumulative_offset = 0u64;

        for page in pages.iter() {
            for (idx, entry) in page.page.entries.iter().enumerate() {
                let global_row = cumulative_offset + idx as u64;
                if let Ok(row_id) = usize::try_from(global_row) {
                    row_values.insert(row_id, entry.get_data());
                }
            }
            cumulative_offset += page.page.entries.len() as u64;
        }

        // CRITICAL: In-place filtering to avoid allocations
        rows.retain(|&row_id| {
            if let Some(&value) = row_values.get(&row_id) {
                // Short-circuit AND evaluation - fail fast
                self.filters.iter().all(|filter| eval_filter(filter, value))
            } else {
                false
            }
        });
    }
}

/// Represents the execution pipeline for a query.
#[derive(Debug)]
pub struct Job {
    /// The table name this pipeline operates on
    pub table_name: String,
    /// Sequential steps to execute filters, each on a specific column
    pub steps: Vec<PipelineStep>,
    /// Total number of steps, used for quick scheduling heuristics
    pub cost: usize,
    /// Tracks the next slot available for downstream execution bookkeeping
    pub next_free_slot: AtomicUsize,
    /// Identifier for tracing/logging purposes
    pub id: String,
    /// Entry point producer feeding the first pipeline step
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
            id: generate_pipeline_id(),
            entry_producer,
        }
    }

    /// Attempts to run the next pipeline step via CAS on `next_free_slot`.
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
        self.cost == other.cost
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cost.cmp(&other.cost))
    }
}

/// Minimal expression evaluator - inline for performance
#[inline]
fn eval_filter(filter: &FilterExpr, value: &str) -> bool {
    match filter {
        FilterExpr::Leaf(expr) => eval_expr(expr, value),
        FilterExpr::And(filters) => filters.iter().all(|f| eval_filter(f, value)),
        FilterExpr::Or(filters) => filters.iter().any(|f| eval_filter(f, value)),
    }
}

#[inline]
fn eval_expr(expr: &Expr, value: &str) -> bool {
    use sqlparser::ast::{BinaryOperator, Value};

    match expr {
        Expr::BinaryOp { left: _, op, right } => {
            // Extract literal from right side
            let literal = match right.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
                Expr::Value(Value::Number(n, _)) => n.as_str(),
                _ => return true, // Skip unsupported comparisons
            };

            match op {
                BinaryOperator::Eq => value == literal,
                BinaryOperator::NotEq => value != literal,
                BinaryOperator::Gt => value > literal, // Lexicographic for now
                BinaryOperator::GtEq => value >= literal,
                BinaryOperator::Lt => value < literal,
                BinaryOperator::LtEq => value <= literal,
                _ => true, // Conservative: include row if unsure
            }
        }
        _ => true, // Include row for unsupported expressions
    }
}

/// Build a pipeline from a query plan.
/// For now, this randomly orders the filter steps with no optimization.
pub fn build_pipeline(plan: &QueryPlan, page_handler: Arc<PageHandler>) -> Vec<Job> {
    let mut jobs = Vec::with_capacity(plan.tables.len());

    for table in &plan.tables {
        if let Some(filter) = &table.filters {
            // Extract all leaf filters from the filter tree
            let leaf_filters = extract_leaf_filters(filter);

            // Group filters by the column they operate on
            let grouped = group_filters_by_column(leaf_filters);

            // Convert groups into pipeline steps (random order for now)
            let grouped_steps: Vec<(String, Vec<FilterExpr>)> = grouped.into_iter().collect();
            let (entry_producer, steps) = attach_channels(
                grouped_steps,
                table.table_name.clone(),
                Arc::clone(&page_handler),
            );

            jobs.push(Job::new(table.table_name.clone(), steps, entry_producer));
        } else {
            // No filters, empty pipeline
            let (entry_producer, _) = channel::unbounded::<PipelineBatch>();
            jobs.push(Job::new(
                table.table_name.clone(),
                Vec::new(),
                entry_producer,
            ));
        }
    }

    jobs
}

/// Extracts all leaf filter expressions from the filter tree.
fn extract_leaf_filters(filter: &FilterExpr) -> Vec<FilterExpr> {
    let mut leaves = Vec::new();
    collect_leaves(filter, &mut leaves);
    leaves
}

fn collect_leaves(filter: &FilterExpr, leaves: &mut Vec<FilterExpr>) {
    match filter {
        FilterExpr::Leaf(expr) => leaves.push(FilterExpr::Leaf(expr.clone())),
        FilterExpr::And(filters) | FilterExpr::Or(filters) => {
            for f in filters {
                collect_leaves(f, leaves);
            }
        }
    }
}

/// Groups filters by the primary column they operate on.
fn group_filters_by_column(filters: Vec<FilterExpr>) -> HashMap<String, Vec<FilterExpr>> {
    let mut groups: HashMap<String, Vec<FilterExpr>> = HashMap::new();

    for filter in filters {
        if let FilterExpr::Leaf(expr) = &filter {
            let column = extract_primary_column(expr).unwrap_or_else(|| "*".to_string());
            groups.entry(column).or_default().push(filter);
        }
    }

    groups
}

/// Extracts the primary column name from an expression.
/// Returns None if no clear column is found.
fn extract_primary_column(expr: &Expr) -> Option<String> {
    use Expr::*;

    match expr {
        Identifier(ident) => Some(ident.value.clone()),
        CompoundIdentifier(idents) => idents.last().map(|id| id.value.clone()),
        BinaryOp { left, .. } => extract_primary_column(left),
        UnaryOp { expr, .. } | Nested(expr) | Cast { expr, .. } | TryCast { expr, .. } => {
            extract_primary_column(expr)
        }
        IsFalse(expr) | IsNotFalse(expr) | IsTrue(expr) | IsNotTrue(expr) | IsNull(expr)
        | IsNotNull(expr) | IsUnknown(expr) | IsNotUnknown(expr) => extract_primary_column(expr),
        InList { expr, .. } | InSubquery { expr, .. } | InUnnest { expr, .. } => {
            extract_primary_column(expr)
        }
        Between { expr, .. } => extract_primary_column(expr),
        Like { expr, .. } | ILike { expr, .. } | SimilarTo { expr, .. } | RLike { expr, .. } => {
            extract_primary_column(expr)
        }
        _ => None,
    }
}

fn attach_channels(
    grouped_steps: Vec<(String, Vec<FilterExpr>)>,
    table: String,
    page_handler: Arc<PageHandler>,
) -> (Sender<PipelineBatch>, Vec<PipelineStep>) {
    let (entry_producer, mut previous_receiver) = channel::unbounded::<PipelineBatch>();
    let mut steps = Vec::with_capacity(grouped_steps.len());
    let mut is_root = true;

    for (column, filters) in grouped_steps {
        let (current_producer, next_receiver) = channel::unbounded::<PipelineBatch>();
        steps.push(PipelineStep::new(
            column,
            filters,
            current_producer,
            previous_receiver,
            is_root,
            table.clone(),
            Arc::clone(&page_handler),
        ));
        previous_receiver = next_receiver;
        is_root = false;
    }

    (entry_producer, steps)
}

fn generate_pipeline_id() -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();
    format!("pipe-{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
    use crate::helpers::compressor::Compressor;
    use crate::metadata_store::PageDirectory;
    use crate::page_handler::{PageFetcher, PageLocator, PageMaterializer};
    use crate::page_handler::page_io::PageIO;
    use crate::sql::models::TableAccess;
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};
    use std::sync::{Arc, RwLock};

    fn make_ident_expr(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn make_binary_op(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn create_mock_page_handler() -> Arc<PageHandler> {
        let meta_store = Arc::new(RwLock::new(crate::metadata_store::TableMetaStore::new()));
        let directory = Arc::new(PageDirectory::new(meta_store));
        let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));

        let compressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
        let page_io = Arc::new(PageIO {});
        let fetcher = Arc::new(PageFetcher::new(compressed_cache, page_io));

        let uncompressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
        let compressor = Arc::new(Compressor::new());
        let materializer = Arc::new(PageMaterializer::new(uncompressed_cache, compressor));

        Arc::new(PageHandler::new(locator, fetcher, materializer))
    }

    #[test]
    fn test_extract_primary_column_simple() {
        let expr = make_ident_expr("id");
        assert_eq!(extract_primary_column(&expr), Some("id".to_string()));
    }

    #[test]
    fn test_extract_primary_column_binary_op() {
        let expr = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        assert_eq!(extract_primary_column(&expr), Some("age".to_string()));
    }

    #[test]
    fn test_extract_leaf_filters() {
        let filter1 = FilterExpr::Leaf(make_ident_expr("x"));
        let filter2 = FilterExpr::Leaf(make_ident_expr("y"));
        let combined = FilterExpr::and(filter1, filter2);

        let leaves = extract_leaf_filters(&combined);
        assert_eq!(leaves.len(), 2);
    }

    #[test]
    fn test_build_pipeline_no_filters() {
        let table = TableAccess::new("users");
        let plan = QueryPlan::new(vec![table]);
        let page_handler = create_mock_page_handler();

        let pipelines = build_pipeline(&plan, page_handler);
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].steps.len(), 0);
    }

    #[test]
    fn test_build_pipeline_with_filters() {
        let mut table = TableAccess::new("users");
        let filter1 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));
        let filter2 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("John".to_string())),
        ));
        table.add_filter(FilterExpr::and(filter1, filter2));

        let plan = QueryPlan::new(vec![table]);
        let page_handler = create_mock_page_handler();
        let pipelines = build_pipeline(&plan, page_handler);

        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].steps.len(), 2); // Two columns: age and name
    }

    // Tests for eval_expr
    #[test]
    fn test_eval_expr_eq_matches() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_eval_expr_not_eq() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::NotEq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(!eval_expr(&expr, "Alice"));
        assert!(eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_eval_expr_gt_lexicographic() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(eval_expr(&expr, "Bob"));
        assert!(eval_expr(&expr, "Charlie"));
        assert!(!eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "Aaron"));
    }

    #[test]
    fn test_eval_expr_gte() {
        // Note: Uses lexicographic comparison, so "20" > "10" works
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::GtEq,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "z"));
        assert!(eval_expr(&expr, "m"));
        assert!(!eval_expr(&expr, "a"));
    }

    #[test]
    fn test_eval_expr_lt() {
        // Note: Uses lexicographic comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "a"));
        assert!(eval_expr(&expr, "f"));
        assert!(!eval_expr(&expr, "m"));
        assert!(!eval_expr(&expr, "z"));
    }

    #[test]
    fn test_eval_expr_lte() {
        // Note: Uses lexicographic comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::LtEq,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "m"));
        assert!(eval_expr(&expr, "a"));
        assert!(!eval_expr(&expr, "z"));
    }

    #[test]
    fn test_eval_expr_unsupported_returns_true() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Plus,
            Expr::Value(Value::Number("5".to_string(), false)),
        );
        // Conservative: include row if unsure
        assert!(eval_expr(&expr, "anything"));
    }

    #[test]
    fn test_eval_expr_unsupported_right_side() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Eq,
            Expr::Identifier(Ident::new("other_column")),
        );
        // Can't compare to another column yet, conservatively include
        assert!(eval_expr(&expr, "anything"));
    }

    // Tests for eval_filter
    #[test]
    fn test_eval_filter_leaf() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        let filter = FilterExpr::Leaf(expr);
        assert!(eval_filter(&filter, "Alice"));
        assert!(!eval_filter(&filter, "Bob"));
    }

    #[test]
    fn test_eval_filter_and_both_true() {
        let expr1 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        let expr2 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("65".to_string(), false)),
        );
        let filter = FilterExpr::And(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        assert!(eval_filter(&filter, "25"));
        assert!(eval_filter(&filter, "50"));
        assert!(!eval_filter(&filter, "10")); // Too young
        assert!(!eval_filter(&filter, "70")); // Too old
    }

    #[test]
    fn test_eval_filter_and_short_circuit() {
        let expr1 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("100".to_string(), false)),
        );
        let expr2 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("200".to_string(), false)),
        );
        let filter = FilterExpr::And(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        // First condition false, should short-circuit
        assert!(!eval_filter(&filter, "50"));
    }

    #[test]
    fn test_eval_filter_or_any_true() {
        let expr1 = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        );
        let expr2 = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("pending".to_string())),
        );
        let filter = FilterExpr::Or(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        assert!(eval_filter(&filter, "active"));
        assert!(eval_filter(&filter, "pending"));
        assert!(!eval_filter(&filter, "inactive"));
    }

    #[test]
    fn test_eval_filter_nested_and_or() {
        // (age > 18 OR age < 5) AND status = "valid"
        let age_gt = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        let age_lt = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("5".to_string(), false)),
        );
        let status_eq = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("valid".to_string())),
        );

        let age_filter = FilterExpr::Or(vec![
            FilterExpr::Leaf(age_gt.clone()),
            FilterExpr::Leaf(age_lt),
        ]);
        let combined = FilterExpr::And(vec![
            age_filter,
            FilterExpr::Leaf(status_eq.clone()),
        ]);

        // This would require actually checking both columns, which our current
        // implementation doesn't support (it only checks one column at a time)
        // So we test just the filter evaluation logic separately
        assert!(eval_filter(&FilterExpr::Leaf(age_gt), "25"));
        assert!(eval_filter(&FilterExpr::Leaf(status_eq), "valid"));
    }

    // Tests for apply_filters
    use crate::entry::Entry;
    use crate::page::Page;
    use crate::metadata_store::{TableDefinition, ColumnDefinition};

    #[test]
    fn test_apply_filters_empty_batch() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = Vec::new();
        step.apply_filters(&mut batch);
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_apply_filters_no_filters() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let step = PipelineStep::new(
            "age".to_string(),
            vec![], // No filters
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2, 3];
        step.apply_filters(&mut batch);
        // Should remain unchanged when no filters
        assert_eq!(batch.len(), 4);
        assert_eq!(batch, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_apply_filters_no_pages_clears_batch() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "nonexistent_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2, 3];
        step.apply_filters(&mut batch);
        // No pages found, batch should be cleared
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_apply_filters_filters_rows() {
        // This test demonstrates the filtering logic even though we don't have
        // actual page data set up. In practice, this would need actual pages
        // with entries registered in the metadata store.
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2];
        step.apply_filters(&mut batch);

        // Without actual data, all rows will be filtered out
        // (This is expected behavior - rows not found are removed)
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_pipeline_step_execute_with_filters() {
        let page_handler = create_mock_page_handler();
        let (prev_tx, prev_rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        ));

        let step = PipelineStep::new(
            "status".to_string(),
            vec![filter],
            out_tx,
            prev_rx,
            false, // Not root
            "test_table".to_string(),
            page_handler,
        );

        // Send a batch through the pipeline
        prev_tx.send(vec![0, 1, 2, 3]).unwrap();
        drop(prev_tx);

        // Execute the step
        step.execute();

        // Should receive filtered results
        let result = out_rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok());
        let batch = result.unwrap();
        // Without real data, all rows filtered out
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_pipeline_step_execute_root() {
        let page_handler = create_mock_page_handler();
        let (_prev_tx, prev_rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, out_rx) = channel::unbounded::<PipelineBatch>();

        let step = PipelineStep::new(
            "col".to_string(),
            vec![],
            out_tx,
            prev_rx,
            true, // Is root
            "test_table".to_string(),
            page_handler,
        );

        // Execute the root step
        step.execute();

        // Root should send an empty batch
        let result = out_rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_multiple_filters_and_logic() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        // Multiple filters on the same column (AND logic)
        let filter1 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));
        let filter2 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("65".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter1, filter2], // Both must be true
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2];
        step.apply_filters(&mut batch);

        // Without actual data, all filtered out
        assert_eq!(batch.len(), 0);
    }
}
