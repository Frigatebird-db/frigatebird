use crate::sql::models::{FilterExpr, QueryPlan};
use sqlparser::ast::Expr;
use std::collections::HashMap;

/// Represents a single step in the execution pipeline that filters on a specific column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineStep {
    /// The column this step filters on
    pub column: String,
    /// The filter expressions to apply to this column
    pub filters: Vec<FilterExpr>,
}

impl PipelineStep {
    pub fn new(column: String, filters: Vec<FilterExpr>) -> Self {
        PipelineStep { column, filters }
    }
}

/// Represents the execution pipeline for a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    /// The table name this pipeline operates on
    pub table_name: String,
    /// Sequential steps to execute filters, each on a specific column
    pub steps: Vec<PipelineStep>,
}

impl Pipeline {
    pub fn new(table_name: String, steps: Vec<PipelineStep>) -> Self {
        Pipeline { table_name, steps }
    }
}

/// Build a pipeline from a query plan.
/// For now, this randomly orders the filter steps with no optimization.
pub fn build_pipeline(plan: &QueryPlan) -> Vec<Pipeline> {
    let mut pipelines = Vec::with_capacity(plan.tables.len());

    for table in &plan.tables {
        if let Some(filter) = &table.filters {
            // Extract all leaf filters from the filter tree
            let leaf_filters = extract_leaf_filters(filter);

            // Group filters by the column they operate on
            let grouped = group_filters_by_column(leaf_filters);

            // Convert groups into pipeline steps (random order for now)
            let steps: Vec<PipelineStep> = grouped
                .into_iter()
                .map(|(column, filters)| PipelineStep::new(column, filters))
                .collect();

            pipelines.push(Pipeline::new(table.table_name.clone(), steps));
        } else {
            // No filters, empty pipeline
            pipelines.push(Pipeline::new(table.table_name.clone(), Vec::new()));
        }
    }

    pipelines
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
        IsFalse(expr)
        | IsNotFalse(expr)
        | IsTrue(expr)
        | IsNotTrue(expr)
        | IsNull(expr)
        | IsNotNull(expr)
        | IsUnknown(expr)
        | IsNotUnknown(expr) => extract_primary_column(expr),
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
