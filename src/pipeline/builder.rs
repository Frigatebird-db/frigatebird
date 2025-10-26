use crate::sql::models::{FilterExpr, QueryPlan};
use crossbeam::channel::{self, Receiver, Sender};
use rand::{Rng, distributions::Alphanumeric};
use sqlparser::ast::Expr;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

/// Lightweight batch placeholder flowing between pipeline stages.
pub type PipelineBatch = Vec<usize>;

/// Represents a single step in the execution pipeline that filters on a specific column.
#[derive(Debug, Clone)]
pub struct PipelineStep {
    /// Produces batches for the downstream step.
    pub current_producer: Sender<PipelineBatch>,
    /// Receives batches produced by the previous step.
    pub previous_receiver: Receiver<PipelineBatch>,
    /// The column this step filters on
    pub column: String,
    /// The filter expressions to apply to this column
    pub filters: Vec<FilterExpr>,
}

impl PipelineStep {
    pub fn new(
        column: String,
        filters: Vec<FilterExpr>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
    ) -> Self {
        PipelineStep {
            current_producer,
            previous_receiver,
            column,
            filters,
        }
    }

    /// Placeholder execution loop for step-level runtime.
    pub fn execute(&self) -> ! {
        loop {}
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

    /// Placeholder iterator for future pipeline runtime.
    pub fn get_next(&self) -> ! {
        loop {}
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

/// Build a pipeline from a query plan.
/// For now, this randomly orders the filter steps with no optimization.
pub fn build_pipeline(plan: &QueryPlan) -> Vec<Job> {
    let mut jobs = Vec::with_capacity(plan.tables.len());

    for table in &plan.tables {
        if let Some(filter) = &table.filters {
            // Extract all leaf filters from the filter tree
            let leaf_filters = extract_leaf_filters(filter);

            // Group filters by the column they operate on
            let grouped = group_filters_by_column(leaf_filters);

            // Convert groups into pipeline steps (random order for now)
            let grouped_steps: Vec<(String, Vec<FilterExpr>)> = grouped.into_iter().collect();
            let (entry_producer, steps) = attach_channels(grouped_steps);

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
) -> (Sender<PipelineBatch>, Vec<PipelineStep>) {
    let (entry_producer, mut previous_receiver) = channel::unbounded::<PipelineBatch>();
    let mut steps = Vec::with_capacity(grouped_steps.len());

    for (column, filters) in grouped_steps {
        let (current_producer, next_receiver) = channel::unbounded::<PipelineBatch>();
        steps.push(PipelineStep::new(
            column,
            filters,
            current_producer,
            previous_receiver,
        ));
        previous_receiver = next_receiver;
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
    use crate::sql::models::TableAccess;
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

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

        let pipelines = build_pipeline(&plan);
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
        let pipelines = build_pipeline(&plan);

        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].steps.len(), 2); // Two columns: age and name
    }
}
