use super::{filters, parsers, pattern_matching, types};
use types::{Job, PipelineBatch, PipelineStep};

use crate::page_handler::PageHandler;
use crate::sql::models::{FilterExpr, QueryPlan};
use crossbeam::channel::{unbounded, Sender};
use rand::{Rng, distributions::Alphanumeric};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

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

pub(super) fn generate_pipeline_id() -> String {
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
    use crate::cache::page_cache::{
        PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
    };
    use crate::helpers::compressor::Compressor;
    use crate::metadata_store::PageDirectory;
    use crate::page_handler::page_io::PageIO;
    use crate::page_handler::{PageFetcher, PageLocator, PageMaterializer};
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

        let uncompressed_cache =
            Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
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
        let filter = FilterExpr::And(vec![FilterExpr::Leaf(expr1), FilterExpr::Leaf(expr2)]);
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
        let filter = FilterExpr::And(vec![FilterExpr::Leaf(expr1), FilterExpr::Leaf(expr2)]);
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
        let filter = FilterExpr::Or(vec![FilterExpr::Leaf(expr1), FilterExpr::Leaf(expr2)]);
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
        let combined = FilterExpr::And(vec![age_filter, FilterExpr::Leaf(status_eq.clone())]);

        // This would require actually checking both columns, which our current
        // implementation doesn't support (it only checks one column at a time)
        // So we test just the filter evaluation logic separately
        assert!(eval_filter(&FilterExpr::Leaf(age_gt), "25"));
        assert!(eval_filter(&FilterExpr::Leaf(status_eq), "valid"));
    }

    // Tests for apply_filters
    use crate::entry::Entry;
    use crate::metadata_store::{ColumnDefinition, TableDefinition};
    use crate::page::Page;

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

    // Tests for type-aware comparison (compare_values)

    #[test]
    fn test_compare_values_integer_gt() {
        // This would FAIL with lexicographic comparison: "10" < "2"
        // But succeeds with numeric comparison: 10 > 2
        let expr = make_binary_op(
            make_ident_expr("count"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("2".to_string(), false)),
        );
        assert!(eval_expr(&expr, "10"));
        assert!(eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "2"));
        assert!(!eval_expr(&expr, "1"));
    }

    #[test]
    fn test_compare_values_integer_lt() {
        let expr = make_binary_op(
            make_ident_expr("count"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("100".to_string(), false)),
        );
        assert!(eval_expr(&expr, "10"));
        assert!(eval_expr(&expr, "99"));
        assert!(!eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "200"));
    }

    #[test]
    fn test_compare_values_integer_eq() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::Number("42".to_string(), false)),
        );
        assert!(eval_expr(&expr, "42"));
        assert!(!eval_expr(&expr, "43"));
        assert!(!eval_expr(&expr, "41"));
    }

    #[test]
    fn test_compare_values_integer_negative() {
        let expr = make_binary_op(
            make_ident_expr("temperature"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("-10".to_string(), false)),
        );
        assert!(eval_expr(&expr, "0"));
        assert!(eval_expr(&expr, "-5"));
        assert!(!eval_expr(&expr, "-10"));
        assert!(!eval_expr(&expr, "-20"));
    }

    #[test]
    fn test_compare_values_float_gt() {
        let expr = make_binary_op(
            make_ident_expr("price"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("19.99".to_string(), false)),
        );
        assert!(eval_expr(&expr, "20.0"));
        assert!(eval_expr(&expr, "100.5"));
        assert!(!eval_expr(&expr, "19.99"));
        assert!(!eval_expr(&expr, "10.5"));
    }

    #[test]
    fn test_compare_values_float_lte() {
        let expr = make_binary_op(
            make_ident_expr("score"),
            BinaryOperator::LtEq,
            Expr::Value(Value::Number("3.14159".to_string(), false)),
        );
        assert!(eval_expr(&expr, "3.14159"));
        assert!(eval_expr(&expr, "3.0"));
        assert!(eval_expr(&expr, "0.5"));
        assert!(!eval_expr(&expr, "3.2"));
        assert!(!eval_expr(&expr, "10.0"));
    }

    #[test]
    fn test_compare_values_float_scientific() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1e3".to_string(), false)),
        );
        assert!(eval_expr(&expr, "2000"));
        assert!(eval_expr(&expr, "1001"));
        assert!(!eval_expr(&expr, "1000"));
        assert!(!eval_expr(&expr, "999"));
    }

    #[test]
    fn test_compare_values_boolean_true() {
        let expr = make_binary_op(
            make_ident_expr("active"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("true".to_string())),
        );
        assert!(eval_expr(&expr, "true"));
        assert!(eval_expr(&expr, "True"));
        assert!(eval_expr(&expr, "TRUE"));
        assert!(eval_expr(&expr, "t"));
        assert!(eval_expr(&expr, "1"));
        assert!(eval_expr(&expr, "yes"));
        assert!(!eval_expr(&expr, "false"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_compare_values_boolean_false() {
        let expr = make_binary_op(
            make_ident_expr("disabled"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("false".to_string())),
        );
        assert!(eval_expr(&expr, "false"));
        assert!(eval_expr(&expr, "False"));
        assert!(eval_expr(&expr, "FALSE"));
        assert!(eval_expr(&expr, "f"));
        assert!(eval_expr(&expr, "0"));
        assert!(eval_expr(&expr, "no"));
        assert!(!eval_expr(&expr, "true"));
        assert!(!eval_expr(&expr, "1"));
    }

    #[test]
    fn test_compare_values_boolean_gt() {
        // true > false
        let expr = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("false".to_string())),
        );
        assert!(eval_expr(&expr, "true"));
        assert!(eval_expr(&expr, "1"));
        assert!(!eval_expr(&expr, "false"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_compare_values_string_fallback() {
        // When not numbers or bools, falls back to lexicographic
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
    fn test_compare_values_string_eq() {
        let expr = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        );
        assert!(eval_expr(&expr, "active"));
        assert!(!eval_expr(&expr, "inactive"));
        assert!(!eval_expr(&expr, "pending"));
    }

    #[test]
    fn test_compare_values_mixed_numeric_string() {
        // If both can't be parsed as same type, falls back to string comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("abc".to_string())),
        );
        // Comparing number to non-number falls back to string
        assert!(eval_expr(&expr, "def"));
        assert!(!eval_expr(&expr, "123")); // "123" < "abc" lexicographically
    }

    #[test]
    fn test_compare_values_integer_vs_float() {
        // Integer parsing takes precedence, but if one is float, falls back to float comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("10.5".to_string(), false)),
        );
        // "11" vs "10.5" - 11 can be parsed as i64, but 10.5 can't, so uses f64
        assert!(eval_expr(&expr, "11"));
        assert!(eval_expr(&expr, "10.6"));
        assert!(!eval_expr(&expr, "10"));
        assert!(!eval_expr(&expr, "10.5"));
    }

    #[test]
    fn test_compare_values_not_eq_integer() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::NotEq,
            Expr::Value(Value::Number("42".to_string(), false)),
        );
        assert!(eval_expr(&expr, "43"));
        assert!(eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, "42"));
    }

    #[test]
    fn test_compare_values_gte_float() {
        let expr = make_binary_op(
            make_ident_expr("score"),
            BinaryOperator::GtEq,
            Expr::Value(Value::Number("3.14".to_string(), false)),
        );
        assert!(eval_expr(&expr, "3.14"));
        assert!(eval_expr(&expr, "3.15"));
        assert!(eval_expr(&expr, "10.0"));
        assert!(!eval_expr(&expr, "3.13"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_parse_bool_variations() {
        // Test the parse_bool helper directly through eval_expr
        let expr_true = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("yes".to_string())),
        );
        assert!(eval_expr(&expr_true, "y"));
        assert!(eval_expr(&expr_true, "Y"));
        assert!(eval_expr(&expr_true, "YES"));

        let expr_false = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("no".to_string())),
        );
        assert!(eval_expr(&expr_false, "n"));
        assert!(eval_expr(&expr_false, "N"));
        assert!(eval_expr(&expr_false, "NO"));
    }

    #[test]
    fn test_compare_values_zero_comparisons() {
        let expr = make_binary_op(
            make_ident_expr("balance"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("0".to_string(), false)),
        );
        assert!(eval_expr(&expr, "1"));
        assert!(eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, "-1"));
    }

    #[test]
    fn test_compare_values_large_numbers() {
        let expr = make_binary_op(
            make_ident_expr("population"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("1000000".to_string(), false)),
        );
        assert!(eval_expr(&expr, "999999"));
        assert!(eval_expr(&expr, "1"));
        assert!(!eval_expr(&expr, "1000000"));
        assert!(!eval_expr(&expr, "1000001"));
    }

    // ========== Date/Time Comparison Tests ==========

    #[test]
    fn test_datetime_iso_date_comparison() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("2024-01-01".to_string())),
        );
        assert!(eval_expr(&expr, "2024-12-31"));
        assert!(eval_expr(&expr, "2025-01-01"));
        assert!(!eval_expr(&expr, "2024-01-01"));
        assert!(!eval_expr(&expr, "2023-12-31"));
    }

    #[test]
    fn test_datetime_iso_timestamp_comparison() {
        let expr = make_binary_op(
            make_ident_expr("timestamp"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2024-06-15 12:00:00".to_string())),
        );
        assert!(eval_expr(&expr, "2024-06-15 11:59:59"));
        assert!(eval_expr(&expr, "2024-01-01 00:00:00"));
        assert!(!eval_expr(&expr, "2024-06-15 12:00:00"));
        assert!(!eval_expr(&expr, "2024-06-15 12:00:01"));
    }

    #[test]
    fn test_datetime_unix_timestamp() {
        let expr = make_binary_op(
            make_ident_expr("ts"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1704067200".to_string(), false)), // 2024-01-01 00:00:00 UTC
        );
        assert!(eval_expr(&expr, "1735689600")); // 2025-01-01
        assert!(!eval_expr(&expr, "1672531200")); // 2023-01-01
    }

    #[test]
    fn test_datetime_common_format_mdy() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("01/15/2024".to_string())),
        );
        assert!(eval_expr(&expr, "2024-01-15")); // Same date, different format
    }

    #[test]
    fn test_datetime_leap_year() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2024-03-01".to_string())),
        );
        assert!(eval_expr(&expr, "2024-02-29")); // Leap year
        assert!(!eval_expr(&expr, "2024-03-01"));
    }

    // ========== NULL Handling Tests ==========

    #[test]
    fn test_is_null_empty_string() {
        let expr = Expr::IsNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, ""));
        assert!(!eval_expr(&expr, "something"));
    }

    #[test]
    fn test_is_null_null_string() {
        let expr = Expr::IsNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, "null"));
        assert!(eval_expr(&expr, "NULL"));
        assert!(eval_expr(&expr, "Null"));
        assert!(eval_expr(&expr, "nil"));
        assert!(!eval_expr(&expr, "not null"));
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, "something"));
        assert!(eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, ""));
        assert!(!eval_expr(&expr, "null"));
    }

    // ========== LIKE Pattern Matching Tests ==========

    #[test]
    fn test_like_exact_match() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "alice")); // Case sensitive
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_like_starts_with() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("John%".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "John"));
        assert!(eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "Johnson"));
        assert!(!eval_expr(&expr, "Bob"));
        assert!(!eval_expr(&expr, "john")); // Case sensitive
    }

    #[test]
    fn test_like_ends_with() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("email")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString(
                "%@gmail.com".to_string(),
            ))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "user@gmail.com"));
        assert!(eval_expr(&expr, "test.user@gmail.com"));
        assert!(!eval_expr(&expr, "user@yahoo.com"));
    }

    #[test]
    fn test_like_contains() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString(
                "%world%".to_string(),
            ))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "world"));
        assert!(eval_expr(&expr, "the world is big"));
        assert!(!eval_expr(&expr, "hello earth"));
    }

    #[test]
    fn test_like_single_char_wildcard() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("code")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("AB_123".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "ABC123"));
        assert!(eval_expr(&expr, "ABX123"));
        assert!(!eval_expr(&expr, "AB123")); // Missing one char
        assert!(!eval_expr(&expr, "ABCD123")); // Too many chars
    }

    #[test]
    fn test_like_negated() {
        let expr = Expr::Like {
            negated: true,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("John%".to_string()))),
            escape_char: None,
        };
        assert!(!eval_expr(&expr, "John"));
        assert!(!eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_ilike_case_insensitive() {
        let expr = Expr::ILike {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("ALICE".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "alice"));
        assert!(eval_expr(&expr, "Alice"));
        assert!(eval_expr(&expr, "ALICE"));
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_ilike_starts_with_case_insensitive() {
        let expr = Expr::ILike {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("john%".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "JOHN"));
        assert!(eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "JOHNSON"));
    }

    // ========== IP Address Comparison Tests ==========

    #[test]
    fn test_ip_address_comparison_gt() {
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("192.168.1.1".to_string())),
        );
        assert!(eval_expr(&expr, "192.168.1.2"));
        assert!(eval_expr(&expr, "192.168.1.100"));
        assert!(eval_expr(&expr, "192.168.2.1"));
        assert!(!eval_expr(&expr, "192.168.1.1"));
        assert!(!eval_expr(&expr, "192.168.0.255"));
    }

    #[test]
    fn test_ip_address_comparison_lexicographic_wrong() {
        // This demonstrates why we need special IP parsing
        // Lexicographically: "192.168.1.100" < "192.168.1.2" (wrong!)
        // Numerically: 192.168.1.100 > 192.168.1.2 (correct!)
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("192.168.1.2".to_string())),
        );
        assert!(eval_expr(&expr, "192.168.1.100")); // Correct with IP parsing
    }

    #[test]
    fn test_ip_address_different_octets() {
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("10.0.0.1".to_string())),
        );
        assert!(eval_expr(&expr, "9.255.255.255"));
        assert!(!eval_expr(&expr, "10.0.0.1"));
        assert!(!eval_expr(&expr, "10.0.0.2"));
    }

    // ========== UUID Comparison Tests ==========

    #[test]
    fn test_uuid_comparison() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString(
                "550e8400-e29b-41d4-a716-446655440001".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
        assert!(!eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440001"));
        assert!(!eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440002"));
    }

    #[test]
    fn test_uuid_case_insensitive() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString(
                "550E8400-E29B-41D4-A716-446655440000".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
        assert!(eval_expr(&expr, "550E8400-E29B-41D4-A716-446655440000"));
    }

    #[test]
    fn test_uuid_no_hyphens() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString(
                "550e8400e29b41d4a716446655440000".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
    }

    // ========== Version String Comparison Tests ==========

    #[test]
    fn test_version_major_comparison() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.0.0".to_string())),
        );
        assert!(eval_expr(&expr, "2.0.0"));
        assert!(eval_expr(&expr, "10.0.0"));
        assert!(!eval_expr(&expr, "1.0.0"));
        assert!(!eval_expr(&expr, "0.9.9"));
    }

    #[test]
    fn test_version_minor_comparison() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("1.5.0".to_string())),
        );
        assert!(eval_expr(&expr, "1.4.0"));
        assert!(eval_expr(&expr, "1.0.0"));
        assert!(!eval_expr(&expr, "1.5.0"));
        assert!(!eval_expr(&expr, "1.6.0"));
    }

    #[test]
    fn test_version_patch_comparison() {
        // This would FAIL with lexicographic: "1.2.9" > "1.2.10"
        // But works with version parsing: 1.2.10 > 1.2.9
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.2.9".to_string())),
        );
        assert!(eval_expr(&expr, "1.2.10"));
        assert!(eval_expr(&expr, "1.2.100"));
        assert!(!eval_expr(&expr, "1.2.9"));
        assert!(!eval_expr(&expr, "1.2.8"));
    }

    #[test]
    fn test_version_short_format() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("2.0".to_string())),
        );
        assert!(eval_expr(&expr, "2.0.0"));
        assert!(eval_expr(&expr, "2.0"));
    }

    #[test]
    fn test_version_with_prerelease() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.0.0-alpha".to_string())),
        );
        assert!(eval_expr(&expr, "1.0.0-beta")); // beta > alpha lexicographically
        assert!(eval_expr(&expr, "1.0.0")); // Release > prerelease
    }

    // ========== Duration Comparison Tests ==========

    #[test]
    fn test_duration_simple_format() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1 hour".to_string())),
        );
        assert!(eval_expr(&expr, "2 hours"));
        assert!(eval_expr(&expr, "90 minutes")); // 90 min > 60 min
        assert!(!eval_expr(&expr, "30 minutes"));
    }

    #[test]
    fn test_duration_days_vs_hours() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2 days".to_string())),
        );
        assert!(eval_expr(&expr, "1 day"));
        assert!(eval_expr(&expr, "24 hours"));
        assert!(eval_expr(&expr, "47 hours")); // 47h < 48h (2 days)
        assert!(!eval_expr(&expr, "48 hours")); // Equal
        assert!(!eval_expr(&expr, "3 days"));
    }

    #[test]
    fn test_duration_compact_format() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("90 minutes".to_string())),
        );
        assert!(eval_expr(&expr, "1h30m")); // Same duration
    }

    #[test]
    fn test_duration_complex_compact() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1h".to_string())),
        );
        assert!(eval_expr(&expr, "1h1s"));
        assert!(eval_expr(&expr, "2h30m15s"));
        assert!(!eval_expr(&expr, "59m"));
    }

    #[test]
    fn test_duration_weeks() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("1 week".to_string())),
        );
        assert!(eval_expr(&expr, "7 days"));
        assert!(eval_expr(&expr, "168 hours"));
    }

    // ========== Regex Pattern Matching Tests ==========

    #[test]
    fn test_regex_simple_contains() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "this is a test"));
        assert!(eval_expr(&expr, "test"));
        assert!(!eval_expr(&expr, "no match"));
    }

    #[test]
    fn test_regex_starts_with_anchor() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("^hello".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "hello"));
        assert!(!eval_expr(&expr, "say hello"));
    }

    #[test]
    fn test_regex_ends_with_anchor() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("world$".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "world"));
        assert!(!eval_expr(&expr, "world peace"));
    }

    #[test]
    fn test_regex_both_anchors() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString(
                "^exact$".to_string(),
            ))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "exact"));
        assert!(!eval_expr(&expr, "exact match"));
        assert!(!eval_expr(&expr, "not exact"));
    }

    #[test]
    fn test_regex_negated() {
        let expr = Expr::RLike {
            negated: true,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("error".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "success"));
        assert!(!eval_expr(&expr, "error occurred"));
    }

    // ========== Integration Tests (Multiple Type Comparisons) ==========

    #[test]
    fn test_mixed_types_fallback_correctly() {
        // When comparing different types, should fall back gracefully
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("2024-01-01".to_string())),
        );
        // Both parsed as dates
        assert!(eval_expr(&expr, "2024-12-31"));

        // If one isn't a date, falls back to string comparison
        let expr2 = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("not-a-date".to_string())),
        );
        assert!(eval_expr(&expr2, "zebra")); // String comparison
    }

    #[test]
    fn test_number_vs_date_priority() {
        // Numbers are tried before dates, so Unix timestamps work
        let expr = make_binary_op(
            make_ident_expr("timestamp"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1704067200".to_string(), false)),
        );
        assert!(eval_expr(&expr, "1735689600"));
    }
}
