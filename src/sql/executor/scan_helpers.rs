use super::SqlExecutionError;
use super::helpers::{collect_expr_column_names, column_name_from_expr, expr_to_string};
use super::values::compare_strs;
use crate::metadata_store::ColumnCatalog;
use sqlparser::ast::{BinaryOperator, Expr};
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
