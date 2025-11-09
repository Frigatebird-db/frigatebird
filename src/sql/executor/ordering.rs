use super::SqlExecutionError;
use super::aggregates::{AggregateDataset, MaterializedColumns};
use super::batch::{ColumnarBatch, ColumnarPage, ColumnData};
use super::expressions::{evaluate_expression_on_batch, evaluate_row_expr, evaluate_scalar_expression};
use super::values::{ScalarValue, compare_scalar_values, compare_strs, format_float};
use crate::metadata_store::TableCatalog;
use sqlparser::ast::Expr;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone)]
pub(super) struct OrderClause {
    pub(super) expr: Expr,
    pub(super) descending: bool,
    pub(super) nulls: NullsPlacement,
}

#[derive(Clone, Copy)]
pub(super) enum NullsPlacement {
    Default,
    First,
    Last,
}

pub(super) fn sort_rows_logical(
    clauses: &[OrderClause],
    materialized: &MaterializedColumns,
    column_ordinals: &HashMap<String, usize>,
    rows: &mut Vec<u64>,
) -> Result<(), SqlExecutionError> {
    if clauses.is_empty() || rows.len() <= 1 {
        return Ok(());
    }

    let dataset = AggregateDataset {
        rows: rows.as_slice(),
        materialized,
        column_ordinals,
        masked_exprs: None,
        prefer_exact_numeric: false,
    };

    let mut keyed: Vec<(OrderKey, u64)> = Vec::with_capacity(rows.len());
    for &row_idx in rows.iter() {
        let key = build_row_order_key(clauses, row_idx, &dataset)?;
        keyed.push((key, row_idx));
    }

    keyed.sort_unstable_by(|left, right| compare_order_keys(&left.0, &right.0, clauses));

    rows.clear();
    rows.extend(keyed.into_iter().map(|(_, row)| row));

    Ok(())
}

#[derive(Clone)]
pub(super) struct OrderKey {
    pub(super) values: Vec<ScalarValue>,
}

pub(super) fn build_row_order_key(
    clauses: &[OrderClause],
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<OrderKey, SqlExecutionError> {
    if clauses.is_empty() {
        return Ok(OrderKey { values: Vec::new() });
    }

    let mut values = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let value = evaluate_row_expr(&clause.expr, row_idx, dataset)?;
        values.push(value);
    }
    Ok(OrderKey { values })
}

pub(super) fn compare_order_keys(
    left: &OrderKey,
    right: &OrderKey,
    clauses: &[OrderClause],
) -> Ordering {
    for (idx, clause) in clauses.iter().enumerate() {
        let lhs = &left.values[idx];
        let rhs = &right.values[idx];
        let ord = compare_scalar_with_clause(lhs, rhs, clause);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(super) fn build_group_order_key(
    clauses: &[OrderClause],
    dataset: &AggregateDataset,
) -> Result<OrderKey, SqlExecutionError> {
    if clauses.is_empty() {
        return Ok(OrderKey { values: Vec::new() });
    }

    let mut values = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let value = evaluate_scalar_expression(&clause.expr, dataset)?;
        values.push(value);
    }
    Ok(OrderKey { values })
}

fn compare_scalar_with_clause(
    left: &ScalarValue,
    right: &ScalarValue,
    clause: &OrderClause,
) -> Ordering {
    let left_null = left.is_null();
    let right_null = right.is_null();

    if left_null || right_null {
        if left_null && right_null {
            return Ordering::Equal;
        }

        return match clause.nulls {
            NullsPlacement::First => {
                if left_null {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            NullsPlacement::Last => {
                if left_null {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            NullsPlacement::Default => {
                if clause.descending {
                    if left_null {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                } else if left_null {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
        };
    }

    let mut ord = compare_scalar_values(left, right).unwrap_or_else(|| {
        let left_str = scalar_to_string(left);
        let right_str = scalar_to_string(right);
        compare_strs(&left_str, &right_str)
    });
    if clause.descending {
        ord = ord.reverse();
    }
    ord
}

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => String::new(),
        ScalarValue::Int(value) => value.to_string(),
        ScalarValue::Float(value) => format_float(*value),
        ScalarValue::Text(text) => text.clone(),
        ScalarValue::Bool(value) => {
            if *value {
                "true".into()
            } else {
                "false".into()
            }
        }
    }
}

pub(super) fn build_order_keys_on_batch(
    clauses: &[OrderClause],
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<Vec<OrderKey>, SqlExecutionError> {
    if clauses.is_empty() || batch.num_rows == 0 {
        return Ok(vec![OrderKey { values: Vec::new() }; batch.num_rows]);
    }

    let mut evaluated_columns: Vec<ColumnarPage> = Vec::with_capacity(clauses.len());
    for clause in clauses {
        evaluated_columns.push(evaluate_expression_on_batch(&clause.expr, batch, catalog)?);
    }

    let mut keys = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut values = Vec::with_capacity(clauses.len());
        for column in &evaluated_columns {
            values.push(column_scalar_value(column, row_idx));
        }
        keys.push(OrderKey { values });
    }
    Ok(keys)
}

pub(super) fn sort_batch_in_memory(
    batch: &ColumnarBatch,
    clauses: &[OrderClause],
    catalog: &TableCatalog,
) -> Result<ColumnarBatch, SqlExecutionError> {
    if clauses.is_empty() || batch.num_rows <= 1 {
        return Ok(batch.clone());
    }

    let order_keys = build_order_keys_on_batch(clauses, batch, catalog)?;
    let mut indices: Vec<usize> = (0..batch.num_rows).collect();
    indices.sort_unstable_by(|&left, &right| {
        compare_order_keys(&order_keys[left], &order_keys[right], clauses)
    });
    Ok(batch.gather(&indices))
}

fn column_scalar_from_text(value: &str) -> ScalarValue {
    ScalarValue::Text(value.to_string())
}

fn column_scalar_value(page: &ColumnarPage, idx: usize) -> ScalarValue {
    if page.null_bitmap.is_set(idx) {
        return ScalarValue::Null;
    }
    match &page.data {
        ColumnData::Int64(values) => ScalarValue::Int(values[idx] as i128),
        ColumnData::Float64(values) => ScalarValue::Float(values[idx]),
        ColumnData::Text(values) => column_scalar_from_text(&values[idx]),
    }
}
