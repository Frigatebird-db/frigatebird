use crate::metadata_store::ColumnCatalog;
use crate::sql::executor::SqlExecutor;
use crate::sql::runtime::SqlExecutionError;
use crate::sql::runtime::helpers::{
    collect_expr_column_names, column_name_from_expr, expr_to_string,
};
use crate::sql::runtime::scan_helpers::{locate_rows_by_sort_prefixes, locate_rows_by_sort_tuple};
use crate::sql::runtime::values::compare_strs;
use crate::sql::types::DataType;
use sqlparser::ast::{BinaryOperator, Expr, Value};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

pub(crate) fn plan_row_ids_from_sort_keys(
    executor: &SqlExecutor,
    table_name: &str,
    sort_columns: &[ColumnCatalog],
    selection_expr: Option<&Expr>,
) -> Result<Option<Vec<u64>>, SqlExecutionError> {
    let sort_key_filters = collect_sort_key_filters(selection_expr, sort_columns)?;
    let sort_key_prefixes = if selection_expr.is_some() {
        collect_sort_key_prefixes(selection_expr, sort_columns)?
    } else {
        None
    };

    let mut key_values = Vec::with_capacity(sort_columns.len());
    let row_ids = if let Some(filters) = sort_key_filters {
        for column in sort_columns {
            let value = filters.get(&column.name).cloned().ok_or_else(|| {
                SqlExecutionError::Unsupported(format!(
                    "requires equality predicate for ORDER BY column {}",
                    column.name
                ))
            })?;
            key_values.push(value);
        }
        Some(locate_rows_by_sort_tuple(
            executor.page_handler().as_ref(),
            executor.page_directory().as_ref(),
            table_name,
            sort_columns,
            &key_values,
        )?)
    } else if let Some(prefixes) = sort_key_prefixes {
        Some(locate_rows_by_sort_prefixes(
            executor.page_handler().as_ref(),
            executor.page_directory().as_ref(),
            table_name,
            sort_columns,
            &prefixes,
        )?)
    } else {
        None
    };

    Ok(row_ids)
}

pub(crate) fn plan_row_ids_for_select(
    executor: &SqlExecutor,
    table_name: &str,
    sort_columns: &[ColumnCatalog],
    selection_expr: Option<&Expr>,
    column_types: &HashMap<String, DataType>,
) -> Result<Option<Vec<u64>>, SqlExecutionError> {
    if selection_expr.is_some_and(|expr| {
        selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
    }) {
        return Ok(None);
    }
    plan_row_ids_from_sort_keys(executor, table_name, sort_columns, selection_expr)
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

pub(crate) fn collect_sort_key_filters(
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

pub(crate) fn collect_sort_key_prefixes(
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

fn selection_uses_numeric_literal_on_string_sort(
    expr: &Expr,
    sort_columns: &[ColumnCatalog],
    column_types: &HashMap<String, DataType>,
) -> bool {
    let sort_names: std::collections::HashSet<&str> =
        sort_columns.iter().map(|col| col.name.as_str()).collect();
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                if let Some(name) = crate::sql::runtime::helpers::column_name_from_expr(left) {
                    if sort_names.contains(name.as_str()) {
                        if is_numeric_literal(right) {
                            return matches!(column_types.get(&name), Some(DataType::String));
                        }
                    }
                }
                if let Some(name) = crate::sql::runtime::helpers::column_name_from_expr(right) {
                    if sort_names.contains(name.as_str()) {
                        if is_numeric_literal(left) {
                            return matches!(column_types.get(&name), Some(DataType::String));
                        }
                    }
                }
            }
            selection_uses_numeric_literal_on_string_sort(left, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(right, sort_columns, column_types)
        }
        Expr::Nested(inner) => {
            selection_uses_numeric_literal_on_string_sort(inner, sort_columns, column_types)
        }
        Expr::UnaryOp { expr, .. } => {
            selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(low, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(high, sort_columns, column_types)
        }
        Expr::InList { expr, list, .. } => {
            if selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types) {
                return true;
            }
            list.iter().any(|item| {
                selection_uses_numeric_literal_on_string_sort(item, sort_columns, column_types)
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|op| {
                selection_uses_numeric_literal_on_string_sort(op, sort_columns, column_types)
            }) || conditions.iter().any(|cond| {
                selection_uses_numeric_literal_on_string_sort(cond, sort_columns, column_types)
            }) || results.iter().any(|res| {
                selection_uses_numeric_literal_on_string_sort(res, sort_columns, column_types)
            }) || else_result.as_ref().is_some_and(|expr| {
                selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
            })
        }
        _ => false,
    }
}

fn is_numeric_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(Value::Number(_, _)) => true,
        Expr::UnaryOp { op, expr } => {
            matches!(
                op,
                sqlparser::ast::UnaryOperator::Plus | sqlparser::ast::UnaryOperator::Minus
            ) && matches!(**expr, Expr::Value(Value::Number(_, _)))
        }
        _ => false,
    }
}
