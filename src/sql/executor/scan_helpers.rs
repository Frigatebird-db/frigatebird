use super::SqlExecutionError;
use super::helpers::{collect_expr_column_names, column_name_from_expr, expr_to_string};
use super::values::compare_strs;
use crate::metadata_store::ColumnCatalog;
use sqlparser::ast::{BinaryOperator, Expr, Value};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) fn collect_sort_key_filters(
    expr: Option<&Expr>,
    sort_columns: &[ColumnCatalog],
) -> Result<Option<HashMap<String, String>>, SqlExecutionError> {
    if sort_columns.is_empty() {
        return Ok(Some(HashMap::new()));
    }

    let expr = match expr {
        Some(expr) => expr,
        None => return Ok(None),
    };

    let sort_names: HashSet<&str> = sort_columns.iter().map(|col| col.name.as_str()).collect();
    let mut filters = HashMap::with_capacity(sort_columns.len());
    let compatible = extract_sort_key_filters(expr, &sort_names, &mut filters)?;

    if !compatible {
        return Ok(None);
    }

    for column in sort_columns {
        if !filters.contains_key(&column.name) {
            return Ok(None);
        }
    }

    Ok(Some(filters))
}

fn extract_sort_key_filters(
    expr: &Expr,
    sort_names: &HashSet<&str>,
    filters: &mut HashMap<String, String>,
) -> Result<bool, SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left_ok = extract_sort_key_filters(left, sort_names, filters)?;
                let right_ok = extract_sort_key_filters(right, sort_names, filters)?;
                Ok(left_ok && right_ok)
            }
            BinaryOperator::Eq => {
                if let Some(column_name) = column_name_from_expr(left) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = match expr_to_string(right) {
                            Ok(value) => value,
                            Err(_) => return Ok(false),
                        };
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Ok(false);
                            }
                        } else {
                            filters.insert(column_name.clone(), value);
                        }
                        return Ok(true);
                    }
                }
                if let Some(column_name) = column_name_from_expr(right) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = match expr_to_string(left) {
                            Ok(value) => value,
                            Err(_) => return Ok(false),
                        };
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Ok(false);
                            }
                        } else {
                            filters.insert(column_name.clone(), value);
                        }
                        return Ok(true);
                    }
                }
                Ok(true)
            }
            BinaryOperator::Or | BinaryOperator::Xor => Ok(false),
            _ => {
                if expression_touches_sort(expr, sort_names) {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        },
        Expr::Nested(inner) => extract_sort_key_filters(inner, sort_names, filters),
        _ => {
            if expression_touches_sort(expr, sort_names) {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }
}

fn expression_touches_sort(expr: &Expr, sort_names: &HashSet<&str>) -> bool {
    let mut columns = BTreeSet::new();
    collect_expr_column_names(expr, &mut columns);
    columns.iter().any(|col| sort_names.contains(col.as_str()))
}

#[derive(Debug, Clone)]
pub(crate) struct SortKeyPrefix {
    pub values: Vec<String>,
}

impl SortKeyPrefix {
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

pub(super) fn collect_sort_key_prefixes(
    expr: Option<&Expr>,
    sort_columns: &[ColumnCatalog],
) -> Result<Option<Vec<SortKeyPrefix>>, SqlExecutionError> {
    let expr = match expr {
        Some(expr) => expr,
        None => return Ok(None),
    };
    if sort_columns.is_empty() {
        return Ok(None);
    }

    let dnf = match expr_to_dnf(expr) {
        Some(clauses) => clauses,
        None => return Ok(None),
    };
    if dnf.is_empty() {
        return Ok(None);
    }

    let sort_names: HashSet<&str> = sort_columns.iter().map(|col| col.name.as_str()).collect();
    let mut prefixes = Vec::with_capacity(dnf.len());

    for clause in dnf {
        let prefix = match build_prefix_from_clause(&clause, sort_columns, &sort_names) {
            Some(prefix) => prefix,
            None => return Ok(None),
        };
        if prefix.values.is_empty() {
            return Ok(None);
        }
        prefixes.push(prefix);
    }

    if prefixes.is_empty() {
        return Ok(None);
    }

    Ok(Some(prefixes))
}

fn build_prefix_from_clause(
    clause: &[Expr],
    sort_columns: &[ColumnCatalog],
    sort_names: &HashSet<&str>,
) -> Option<SortKeyPrefix> {
    let mut eq_map: HashMap<String, String> = HashMap::new();

    for expr in clause {
        if let Some((column, value)) = extract_sort_equality(expr, sort_names) {
            match eq_map.get(&column) {
                Some(existing) => {
                    if compare_strs(existing, &value) != Ordering::Equal {
                        return None;
                    }
                }
                None => {
                    eq_map.insert(column, value);
                }
            }
        }
    }

    if eq_map.is_empty() {
        return None;
    }

    let mut prefix_values = Vec::new();
    for column in sort_columns {
        if let Some(value) = eq_map.remove(&column.name) {
            prefix_values.push(value);
        } else {
            break;
        }
    }

    if prefix_values.is_empty() || !eq_map.is_empty() {
        return None;
    }

    Some(SortKeyPrefix {
        values: prefix_values,
    })
}

fn extract_sort_equality(expr: &Expr, sort_names: &HashSet<&str>) -> Option<(String, String)> {
    match expr {
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Eq) => {
            if let Some(column) = column_name_from_expr(left) {
                if sort_names.contains(column.as_str()) {
                    let value = expr_to_string(right).ok()?;
                    return Some((column, value));
                }
            }
            if let Some(column) = column_name_from_expr(right) {
                if sort_names.contains(column.as_str()) {
                    let value = expr_to_string(left).ok()?;
                    return Some((column, value));
                }
            }
            None
        }
        _ => None,
    }
}

fn expr_to_dnf(expr: &Expr) -> Option<Vec<Vec<Expr>>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left_clauses = expr_to_dnf(left)?;
                let right_clauses = expr_to_dnf(right)?;
                Some(combine_and(&left_clauses, &right_clauses))
            }
            BinaryOperator::Or => {
                let mut clauses = expr_to_dnf(left)?;
                clauses.extend(expr_to_dnf(right)?);
                Some(clauses)
            }
            _ => Some(vec![vec![expr.clone()]]),
        },
        Expr::Nested(inner) => expr_to_dnf(inner),
        Expr::InList {
            expr: inner,
            list,
            negated,
            ..
        } => {
            if *negated || list.is_empty() {
                return None;
            }
            let mut clauses = Vec::with_capacity(list.len());
            for item in list {
                let eq_expr = Expr::BinaryOp {
                    left: inner.clone(),
                    op: BinaryOperator::Eq,
                    right: Box::new(item.clone()),
                };
                clauses.push(vec![eq_expr]);
            }
            Some(clauses)
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
            ..
        } => {
            if *negated {
                return None;
            }
            let ge_expr = Expr::BinaryOp {
                left: inner.clone(),
                op: BinaryOperator::GtEq,
                right: low.clone(),
            };
            let le_expr = Expr::BinaryOp {
                left: inner.clone(),
                op: BinaryOperator::LtEq,
                right: high.clone(),
            };
            Some(vec![vec![ge_expr, le_expr]])
        }
        Expr::Value(Value::Boolean(true)) => Some(vec![Vec::new()]),
        Expr::Value(Value::Boolean(false)) => Some(Vec::new()),
        _ => Some(vec![vec![expr.clone()]]),
    }
}

fn combine_and(left: &[Vec<Expr>], right: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    if left.is_empty() || right.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(left.len() * right.len());
    for l in left {
        for r in right {
            let mut clause = Vec::with_capacity(l.len() + r.len());
            clause.extend(l.clone());
            clause.extend(r.clone());
            result.push(clause);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Expr, Value};

    fn col(name: &str, ordinal: usize) -> ColumnCatalog {
        ColumnCatalog {
            name: name.to_string(),
            data_type: crate::sql::types::DataType::String,
            ordinal,
        }
    }

    fn sort_columns() -> Vec<ColumnCatalog> {
        vec![col("region", 0), col("user_id", 1), col("ts", 2)]
    }

    fn ident(name: &str) -> Expr {
        Expr::Identifier(sqlparser::ast::Ident {
            value: name.to_string(),
            quote_style: None,
        })
    }

    fn string_lit(value: &str) -> Expr {
        Expr::Value(Value::SingleQuotedString(value.to_string()))
    }

    fn eq_expr(column: &str, value: &str) -> Expr {
        Expr::BinaryOp {
            left: Box::new(ident(column)),
            op: BinaryOperator::Eq,
            right: Box::new(string_lit(value)),
        }
    }

    #[test]
    fn test_collect_prefix_single_column() {
        let expr = eq_expr("region", "US");
        let result = collect_sort_key_prefixes(Some(&expr), &sort_columns()).unwrap();
        let prefixes = result.expect("prefixes");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].values, vec!["US".to_string()]);
    }

    #[test]
    fn test_collect_prefix_full_key() {
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("region", "US")),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("user_id", "42")),
        };
        let result = collect_sort_key_prefixes(Some(&expr), &sort_columns()).unwrap();
        let prefixes = result.expect("prefixes");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].values, vec!["US".to_string(), "42".to_string()]);
    }

    #[test]
    fn test_collect_prefix_with_or() {
        let left = Expr::BinaryOp {
            left: Box::new(eq_expr("region", "US")),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("user_id", "42")),
        };
        let right = Expr::BinaryOp {
            left: Box::new(eq_expr("region", "US")),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("user_id", "43")),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
        let result = collect_sort_key_prefixes(Some(&expr), &sort_columns()).unwrap();
        let prefixes = result.expect("prefixes");
        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0].values, vec!["US".to_string(), "42".to_string()]);
        assert_eq!(prefixes[1].values, vec!["US".to_string(), "43".to_string()]);
    }

    #[test]
    fn test_collect_prefix_ignores_trailing_columns() {
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("region", "US")),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(ident("user_id")),
                op: BinaryOperator::Gt,
                right: Box::new(string_lit("42")),
            }),
        };
        let result = collect_sort_key_prefixes(Some(&expr), &sort_columns()).unwrap();
        let prefixes = result.expect("prefixes");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].values, vec!["US".to_string()]);
    }

    #[test]
    fn test_collect_prefix_rejects_missing_leading_column() {
        let expr = eq_expr("user_id", "42");
        let result = collect_sort_key_prefixes(Some(&expr), &sort_columns()).unwrap();
        assert!(result.is_none());
    }
}
