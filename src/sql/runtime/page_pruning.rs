use crate::metadata_store::{ColumnStats, ColumnStatsKind, PageDescriptor};
use crate::sql::runtime::helpers::column_name_from_expr;
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct PagePrunePredicate {
    column: String,
    comparison: PagePruneComparison,
    value: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PagePruneComparison {
    GreaterThan { inclusive: bool },
    LessThan { inclusive: bool },
    Equal,
    IsNull,
    IsNotNull,
}

pub(crate) fn extract_page_prunable_predicates(expr: &Expr) -> Option<Vec<PagePrunePredicate>> {
    let mut predicates = Vec::new();
    if gather_numeric_prunable_predicates(expr, &mut predicates) && !predicates.is_empty() {
        Some(predicates)
    } else {
        None
    }
}

fn gather_numeric_prunable_predicates(expr: &Expr, acc: &mut Vec<PagePrunePredicate>) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                gather_numeric_prunable_predicates(left, acc)
                    && gather_numeric_prunable_predicates(right, acc)
            }
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Eq => build_prunable_comparison(left, op, right, acc),
            _ => false,
        },
        Expr::IsNull(inner) => build_null_prunable_predicate(inner, true, acc),
        Expr::IsNotNull(inner) => build_null_prunable_predicate(inner, false, acc),
        Expr::Nested(inner) => gather_numeric_prunable_predicates(inner, acc),
        _ => false,
    }
}

fn build_prunable_comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(left) {
        if let Some(value) = parse_numeric_literal(right) {
            if let Some(comparison) = comparison_for_operator(op, true) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    if let Some(column) = column_name_from_expr(right) {
        if let Some(value) = parse_numeric_literal(left) {
            if let Some(comparison) = comparison_for_operator(op, false) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    false
}

fn build_null_prunable_predicate(
    expr: &Expr,
    expect_null: bool,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(expr) {
        acc.push(PagePrunePredicate {
            column,
            comparison: if expect_null {
                PagePruneComparison::IsNull
            } else {
                PagePruneComparison::IsNotNull
            },
            value: None,
        });
        return true;
    }
    false
}

fn comparison_for_operator(op: &BinaryOperator, column_on_left: bool) -> Option<PagePruneComparison> {
    match op {
        BinaryOperator::Gt => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: false }
        } else {
            PagePruneComparison::LessThan { inclusive: false }
        }),
        BinaryOperator::GtEq => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: true }
        } else {
            PagePruneComparison::LessThan { inclusive: true }
        }),
        BinaryOperator::Lt => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: false }
        } else {
            PagePruneComparison::GreaterThan { inclusive: false }
        }),
        BinaryOperator::LtEq => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: true }
        } else {
            PagePruneComparison::GreaterThan { inclusive: true }
        }),
        BinaryOperator::Eq => Some(PagePruneComparison::Equal),
        _ => None,
    }
}

fn parse_numeric_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<f64>().ok(),
        Expr::Value(Value::SingleQuotedString(text)) => text.parse::<f64>().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => parse_numeric_literal(expr).map(|value| -value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => parse_numeric_literal(expr),
        Expr::Nested(inner) => parse_numeric_literal(inner),
        _ => None,
    }
}

pub(crate) fn should_prune_page(
    page_idx: usize,
    predicates: &[PagePrunePredicate],
    descriptor_map: &HashMap<usize, Vec<PageDescriptor>>,
    column_ordinals: &HashMap<String, usize>,
) -> bool {
    for predicate in predicates {
        let ordinal = match column_ordinals.get(&predicate.column) {
            Some(ord) => *ord,
            None => continue,
        };
        let descriptors = match descriptor_map.get(&ordinal) {
            Some(list) => list,
            None => continue,
        };
        let descriptor = match descriptors.get(page_idx) {
            Some(desc) => desc,
            None => continue,
        };
        if let Some(stats) = &descriptor.stats {
            match predicate.comparison {
                PagePruneComparison::IsNull => {
                    if stats.null_count == 0 {
                        return true;
                    }
                }
                PagePruneComparison::IsNotNull => {
                    if stats.null_count == descriptor.entry_count {
                        return true;
                    }
                }
                _ => {
                    if matches!(stats.kind, ColumnStatsKind::Int64 | ColumnStatsKind::Float64)
                        && predicate_disqualifies(stats, predicate)
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn predicate_disqualifies(stats: &ColumnStats, predicate: &PagePrunePredicate) -> bool {
    let value = match predicate.value {
        Some(v) => v,
        None => return false,
    };
    let min = stats
        .min_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());
    let max = stats
        .max_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());

    match predicate.comparison {
        PagePruneComparison::GreaterThan { inclusive } => match max {
            Some(max_val) => {
                if inclusive {
                    max_val < value
                } else {
                    max_val <= value
                }
            }
            None => false,
        },
        PagePruneComparison::LessThan { inclusive } => match min {
            Some(min_val) => {
                if inclusive {
                    min_val > value
                } else {
                    min_val >= value
                }
            }
            None => false,
        },
        PagePruneComparison::Equal => match (min, max) {
            (Some(min_val), Some(max_val)) => value < min_val || value > max_val,
            _ => false,
        },
        PagePruneComparison::IsNull | PagePruneComparison::IsNotNull => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::DataType;

    fn column_expr(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn number_expr(value: &str) -> Expr {
        Expr::Value(Value::Number(value.into(), false))
    }

    #[test]
    fn extract_prunable_predicates_from_conjunction() {
        let greater_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Gt,
            right: Box::new(number_expr("100")),
        };
        let less_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::LtEq,
            right: Box::new(number_expr("500")),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(greater_expr),
            op: BinaryOperator::And,
            right: Box::new(less_expr),
        };

        let predicates = extract_page_prunable_predicates(&expr).expect("predicates");
        assert_eq!(predicates.len(), 2);
        assert!(matches!(
            predicates[0].comparison,
            PagePruneComparison::GreaterThan { inclusive: false }
        ));
        assert!(matches!(
            predicates[1].comparison,
            PagePruneComparison::LessThan { inclusive: true }
        ));
    }

    #[test]
    fn unsupported_expression_returns_none() {
        let expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Or,
            right: Box::new(number_expr("10")),
        };
        assert!(extract_page_prunable_predicates(&expr).is_none());
    }

    fn descriptor_with_stats(stats: ColumnStats, entry_count: u64) -> PageDescriptor {
        PageDescriptor {
            id: "test".into(),
            disk_path: "/tmp/pg".into(),
            offset: 0,
            alloc_len: 0,
            actual_len: 0,
            entry_count,
            data_type: DataType::Int64,
            stats: Some(stats),
        }
    }

    #[test]
    fn should_prune_when_range_disjoint() {
        let stats = ColumnStats {
            min_value: Some("0".into()),
            max_value: Some("10".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::GreaterThan { inclusive: false },
            value: Some(25.0),
        };
        assert!(should_prune_page(0, &[predicate], &descriptor_map, &pred_map));
    }

    #[test]
    fn does_not_prune_when_overlap_exists() {
        let stats = ColumnStats {
            min_value: Some("5".into()),
            max_value: Some("50".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::LessThan { inclusive: true },
            value: Some(30.0),
        };
        assert!(!should_prune_page(0, &[predicate], &descriptor_map, &pred_map));
    }

    #[test]
    fn prunes_is_null_when_page_has_no_nulls() {
        let stats = ColumnStats {
            min_value: Some("1".into()),
            max_value: Some("2".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 8);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNull,
            value: None,
        };
        assert!(should_prune_page(0, &[predicate], &descriptor_map, &pred_map));
    }

    #[test]
    fn prunes_is_not_null_when_page_all_nulls() {
        let stats = ColumnStats {
            min_value: None,
            max_value: None,
            null_count: 4,
            kind: ColumnStatsKind::Text,
        };
        let descriptor = descriptor_with_stats(stats, 4);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNotNull,
            value: None,
        };
        assert!(should_prune_page(0, &[predicate], &descriptor_map, &pred_map));
    }
}
