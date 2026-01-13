use crate::metadata_store::ColumnCatalog;
use crate::sql::executor::scan_helpers::{collect_sort_key_filters, collect_sort_key_prefixes};
use crate::sql::executor::{SqlExecutionError, SqlExecutor};
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Value};
use std::collections::HashMap;

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
        Some(executor.locate_rows_by_sort_tuple(
            table_name,
            sort_columns,
            &key_values,
        )?)
    } else if let Some(prefixes) = sort_key_prefixes {
        Some(executor.locate_rows_by_sort_prefixes(
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
                if let Some(name) = crate::sql::executor::helpers::column_name_from_expr(left) {
                    if sort_names.contains(name.as_str()) {
                        if is_numeric_literal(right) {
                            return matches!(column_types.get(&name), Some(DataType::String));
                        }
                    }
                }
                if let Some(name) = crate::sql::executor::helpers::column_name_from_expr(right) {
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
        Expr::Between { expr, low, high, .. } => {
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
            operand
                .as_ref()
                .is_some_and(|op| {
                    selection_uses_numeric_literal_on_string_sort(op, sort_columns, column_types)
                })
                || conditions.iter().any(|cond| {
                    selection_uses_numeric_literal_on_string_sort(cond, sort_columns, column_types)
                })
                || results.iter().any(|res| {
                    selection_uses_numeric_literal_on_string_sort(res, sort_columns, column_types)
                })
                || else_result.as_ref().is_some_and(|expr| {
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
            matches!(op, sqlparser::ast::UnaryOperator::Plus | sqlparser::ast::UnaryOperator::Minus)
                && matches!(**expr, Expr::Value(Value::Number(_, _)))
        }
        _ => false,
    }
}
