use super::SqlExecutionError;
use super::batch::{Bitmap, ColumnData, ColumnarPage};
use super::aggregates::{
    AggregateDataset, MaterializedColumns, evaluate_aggregate_function, is_aggregate_function,
};
use super::helpers::{column_name_from_expr, expr_to_string, is_null_value, like_match, regex_match};
use super::row_functions::evaluate_row_function;
use super::scalar_functions::evaluate_scalar_function;
use super::values::{
    CachedValue, ScalarValue, cached_to_scalar, combine_numeric, compare_scalar_values,
    compare_strs, is_encoded_null, scalar_from_f64,
};
use sqlparser::ast::{BinaryOperator, Expr, Function, UnaryOperator, Value};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

pub(super) fn evaluate_scalar_expression(
    expr: &Expr,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if dataset.is_expr_masked(expr) {
        return Ok(ScalarValue::Null);
    }
    match expr {
        Expr::Identifier(ident) => {
            for &row in dataset.rows {
                if let Some(value) = dataset.column_value(&ident.value, row) {
                    return Ok(cached_to_scalar(value));
                }
            }
            Ok(ScalarValue::Null)
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                for &row in dataset.rows {
                    if let Some(value) = dataset.column_value(&last.value, row) {
                        return Ok(cached_to_scalar(value));
                    }
                }
                Ok(ScalarValue::Null)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
        Expr::Value(Value::Number(n, _)) => Ok(match n.parse::<f64>() {
            Ok(num) => ScalarValue::Float(num),
            Err(_) => ScalarValue::Text(n.clone()),
        }),
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::Function(function) => {
            if is_aggregate_function(function) {
                evaluate_aggregate_function(function, dataset)
            } else {
                evaluate_scalar_function(function, dataset)
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = evaluate_scalar_expression(left, dataset)?;
            let rhs = evaluate_scalar_expression(right, dataset)?;
            match op {
                BinaryOperator::Plus => combine_numeric(&lhs, &rhs, |a, b| a + b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric addition".into())),
                BinaryOperator::Minus => {
                    combine_numeric(&lhs, &rhs, |a, b| a - b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric subtraction".into())
                    })
                }
                BinaryOperator::Multiply => {
                    combine_numeric(&lhs, &rhs, |a, b| a * b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric multiplication".into())
                    })
                }
                BinaryOperator::Divide => combine_numeric(&lhs, &rhs, |a, b| a / b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric division".into())),
                BinaryOperator::Modulo => combine_numeric(&lhs, &rhs, |a, b| a % b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric modulo".into())),
                BinaryOperator::And => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a && b))
                }
                BinaryOperator::Or => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a || b))
                }
                BinaryOperator::Xor => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a ^ b))
                }
                BinaryOperator::Eq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = match compare_scalar_values(&lhs, &rhs) {
                        Some(ord) => ord == Ordering::Greater,
                        None => false,
                    };
                    Ok(ScalarValue::Bool(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported operator in aggregate expression: {op:?}"
                ))),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => {
                    let num = value.as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "unsupported unary minus on non-numeric value".into(),
                        )
                    })?;
                    Ok(scalar_from_f64(-num))
                }
                UnaryOperator::Not => Ok(ScalarValue::Bool(!value.as_bool().unwrap_or(false))),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported unary operator {op:?}"
                ))),
            }
        }
        Expr::Nested(inner) => evaluate_scalar_expression(inner, dataset),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case_expression(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            dataset,
        ),
        Expr::Like {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, true),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::ILike {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, false),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => regex_match(&value, &pattern),
                _ => false,
            };
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = evaluate_scalar_expression(expr, dataset)?;
            let low_value = evaluate_scalar_expression(low, dataset)?;
            let high_value = evaluate_scalar_expression(high, dataset)?;
            let cmp_low = compare_scalar_values(&target, &low_value).unwrap_or(Ordering::Less);
            let cmp_high = compare_scalar_values(&target, &high_value).unwrap_or(Ordering::Greater);
            let between = (cmp_low == Ordering::Greater || cmp_low == Ordering::Equal)
                && (cmp_high == Ordering::Less || cmp_high == Ordering::Equal);
            Ok(ScalarValue::Bool(if *negated { !between } else { between }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = evaluate_scalar_expression(expr, dataset)?;
            let mut matches = false;
            for item in list {
                let candidate = evaluate_scalar_expression(item, dataset)?;
                if compare_scalar_values(&target, &candidate)
                    .map(|ord| ord == Ordering::Equal)
                    .unwrap_or(false)
                {
                    matches = true;
                    break;
                }
            }
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported expression in aggregate projection: {expr:?}"
        ))),
    }
}

fn evaluate_case_expression(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if conditions.len() != results.len() {
        return Err(SqlExecutionError::Unsupported(
            "CASE expression requires matching WHEN and THEN clauses".into(),
        ));
    }

    let operand_value = if let Some(expr) = operand {
        Some(evaluate_scalar_expression(expr, dataset)?)
    } else {
        None
    };

    for (condition, result) in conditions.iter().zip(results.iter()) {
        let matches = if let Some(op_value) = &operand_value {
            let cond_value = evaluate_scalar_expression(condition, dataset)?;
            compare_scalar_values(op_value, &cond_value)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or(false)
        } else {
            evaluate_scalar_expression(condition, dataset)?
                .as_bool()
                .unwrap_or(false)
        };

        if matches {
            return evaluate_scalar_expression(result, dataset);
        }
    }

    if let Some(else_expr) = else_result {
        evaluate_scalar_expression(else_expr, dataset)
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(super) fn evaluate_row_expr(
    expr: &Expr,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if dataset.is_expr_masked(expr) {
        return Ok(ScalarValue::Null);
    }
    match expr {
        Expr::Identifier(ident) => Ok(dataset
            .column_value(&ident.value, row_idx)
            .map(cached_to_scalar)
            .unwrap_or(ScalarValue::Null)),
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                Ok(dataset
                    .column_value(&last.value, row_idx)
                    .map(cached_to_scalar)
                    .unwrap_or(ScalarValue::Null))
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
        Expr::Value(Value::Number(n, _)) => Ok(match n.parse::<f64>() {
            Ok(num) => ScalarValue::Float(num),
            Err(_) => ScalarValue::Text(n.clone()),
        }),
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => {
                    let num = value.as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "unary minus requires numeric operand".into(),
                        )
                    })?;
                    Ok(scalar_from_f64(-num))
                }
                UnaryOperator::Not => Ok(ScalarValue::Bool(!value.as_bool().unwrap_or(false))),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported unary operator {op:?}"
                ))),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = evaluate_row_expr(left, row_idx, dataset)?;
            let rhs = evaluate_row_expr(right, row_idx, dataset)?;
            match op {
                BinaryOperator::Plus => combine_numeric(&lhs, &rhs, |a, b| a + b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric addition".into())),
                BinaryOperator::Minus => {
                    combine_numeric(&lhs, &rhs, |a, b| a - b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric subtraction".into())
                    })
                }
                BinaryOperator::Multiply => {
                    combine_numeric(&lhs, &rhs, |a, b| a * b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric multiplication".into())
                    })
                }
                BinaryOperator::Divide => combine_numeric(&lhs, &rhs, |a, b| a / b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric division".into())),
                BinaryOperator::Modulo => combine_numeric(&lhs, &rhs, |a, b| a % b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric modulo".into())),
                BinaryOperator::And => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) && rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) || rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) ^ rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Eq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater)
                        .unwrap_or(false);
                    Ok(ScalarValue::Bool(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported operator {op:?} in row expression"
                ))),
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, true),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::ILike {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, false),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => regex_match(&value, &pattern),
                _ => false,
            };
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = evaluate_row_expr(expr, row_idx, dataset)?;
            let low_value = evaluate_row_expr(low, row_idx, dataset)?;
            let high_value = evaluate_row_expr(high, row_idx, dataset)?;
            let cmp_low = compare_scalar_values(&target, &low_value).unwrap_or(Ordering::Less);
            let cmp_high = compare_scalar_values(&target, &high_value).unwrap_or(Ordering::Greater);
            let between = (cmp_low == Ordering::Greater || cmp_low == Ordering::Equal)
                && (cmp_high == Ordering::Less || cmp_high == Ordering::Equal);
            Ok(ScalarValue::Bool(if *negated { !between } else { between }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = evaluate_row_expr(expr, row_idx, dataset)?;
            let mut matches = false;
            for item in list {
                let candidate = evaluate_row_expr(item, row_idx, dataset)?;
                if compare_scalar_values(&target, &candidate)
                    .map(|ord| ord == Ordering::Equal)
                    .unwrap_or(false)
                {
                    matches = true;
                    break;
                }
            }
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::IsNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Bool(value.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Bool(!value.is_null()))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_row_case_expr(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            row_idx,
            dataset,
        ),
        Expr::Function(function) => {
            if function.over.is_some() {
                evaluate_window_function(function, row_idx, dataset)
            } else {
                evaluate_row_function(function, row_idx, dataset)
            }
        }
        Expr::Nested(inner) => evaluate_row_expr(inner, row_idx, dataset),
        Expr::Cast { expr, .. }
        | Expr::SafeCast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Convert { expr, .. } => evaluate_row_expr(expr, row_idx, dataset),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported row-level expression: {expr:?}"
        ))),
    }
}

fn evaluate_row_case_expr(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if conditions.len() != results.len() {
        return Err(SqlExecutionError::Unsupported(
            "CASE expression requires matching WHEN and THEN clauses".into(),
        ));
    }

    let operand_value = if let Some(expr) = operand {
        Some(evaluate_row_expr(expr, row_idx, dataset)?)
    } else {
        None
    };

    for (condition, result) in conditions.iter().zip(results.iter()) {
        let matches = if let Some(ref op_value) = operand_value {
            let cond_value = evaluate_row_expr(condition, row_idx, dataset)?;
            compare_scalar_values(op_value, &cond_value)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or(false)
        } else {
            let cond = evaluate_row_expr(condition, row_idx, dataset)?;
            let cond_bool = cond.as_bool().unwrap_or(false);
            cond_bool
        };

        if matches {
            return evaluate_row_expr(result, row_idx, dataset);
        }
    }

    if let Some(else_expr) = else_result {
        evaluate_row_expr(else_expr, row_idx, dataset)
    } else {
        Ok(ScalarValue::Null)
    }
}

fn evaluate_window_function(
    function: &Function,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let key = function.to_string();
    let position = dataset.row_position(row_idx).ok_or_else(|| {
        SqlExecutionError::Unsupported("window functions are not supported in this context".into())
    })?;
    dataset
        .window_value(&key, position)
        .cloned()
        .ok_or_else(|| SqlExecutionError::OperationFailed("window result out of bounds".into()))
}

pub(super) fn evaluate_selection_expr(
    expr: &Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &MaterializedColumns,
) -> Result<bool, SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                Ok(
                    evaluate_selection_expr(left, row_idx, column_ordinals, materialized)?
                        && evaluate_selection_expr(right, row_idx, column_ordinals, materialized)?,
                )
            }
            BinaryOperator::Or => {
                Ok(
                    evaluate_selection_expr(left, row_idx, column_ordinals, materialized)?
                        || evaluate_selection_expr(right, row_idx, column_ordinals, materialized)?,
                )
            }
            BinaryOperator::Eq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Equal
                })
            }
            BinaryOperator::NotEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Equal
                })
            }
            BinaryOperator::Gt => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Greater
                })
            }
            BinaryOperator::GtEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Less
                })
            }
            BinaryOperator::Lt => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Less
                })
            }
            BinaryOperator::LtEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Greater
                })
            }
            _ => Err(SqlExecutionError::Unsupported(format!(
                "unsupported binary operator in WHERE clause: {op:?}"
            ))),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_selection_expr(
                expr,
                row_idx,
                column_ordinals,
                materialized,
            )?),
            _ => Err(SqlExecutionError::Unsupported(format!(
                "unsupported unary operator {op:?}"
            ))),
        },
        Expr::Nested(inner) => {
            evaluate_selection_expr(inner, row_idx, column_ordinals, materialized)
        }
        Expr::Identifier(ident) => {
            evaluate_column_truthy(&ident.value, row_idx, column_ordinals, materialized)
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                evaluate_column_truthy(&last.value, row_idx, column_ordinals, materialized)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::IsNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(matches!(operand, OperandValue::Null))
        }
        Expr::IsNotNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(!matches!(operand, OperandValue::Null))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let low_value = resolve_operand(low, row_idx, column_ordinals, materialized)?;
            let high_value = resolve_operand(high, row_idx, column_ordinals, materialized)?;
            let result = match (&target, &low_value, &high_value) {
                (OperandValue::Text(target), OperandValue::Text(low), OperandValue::Text(high)) => {
                    let lower = compare_strs(target.as_ref(), low.as_ref());
                    let upper = compare_strs(target.as_ref(), high.as_ref());
                    (lower == Ordering::Greater || lower == Ordering::Equal)
                        && (upper == Ordering::Less || upper == Ordering::Equal)
                }
                _ => false,
            };
            Ok(if *negated { !result } else { result })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char: _,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    like_match(value.as_ref(), pattern.as_ref(), true)
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::ILike {
            expr,
            pattern,
            negated,
            escape_char: _,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    like_match(value.as_ref(), pattern.as_ref(), false)
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    regex_match(value.as_ref(), pattern.as_ref())
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let matches = match target {
                OperandValue::Text(value) => list.iter().any(|item| {
                    match resolve_operand(item, row_idx, column_ordinals, materialized) {
                        Ok(OperandValue::Text(candidate)) => {
                            compare_strs(value.as_ref(), candidate.as_ref()) == Ordering::Equal
                        }
                        _ => false,
                    }
                }),
                OperandValue::Null => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(*b),
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => Ok(n != "0"),
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => Ok(!s.is_empty()),
        Expr::Value(sqlparser::ast::Value::Null) => Ok(false),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported expression in WHERE clause: {expr:?}"
        ))),
    }
}

fn compare_operands(
    left: &Expr,
    right: &Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &MaterializedColumns,
    predicate: impl Fn(Ordering) -> bool,
) -> Result<bool, SqlExecutionError> {
    let left_value = resolve_operand(left, row_idx, column_ordinals, materialized)?;
    let right_value = resolve_operand(right, row_idx, column_ordinals, materialized)?;

    match (left_value, right_value) {
        (OperandValue::Text(lhs), OperandValue::Text(rhs)) => {
            Ok(predicate(compare_strs(lhs.as_ref(), rhs.as_ref())))
        }
        _ => Ok(false),
    }
}

enum OperandValue<'a> {
    Text(Cow<'a, str>),
    Null,
}

fn resolve_operand<'a>(
    expr: &'a Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &'a MaterializedColumns,
) -> Result<OperandValue<'a>, SqlExecutionError> {
    match expr {
        Expr::Identifier(ident) => {
            column_operand(&ident.value, row_idx, column_ordinals, materialized)
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                column_operand(&last.value, row_idx, column_ordinals, materialized)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
            Ok(OperandValue::Text(Cow::Borrowed(s.as_str())))
        }
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            Ok(OperandValue::Text(Cow::Borrowed(n.as_str())))
        }
        Expr::Value(sqlparser::ast::Value::Boolean(b)) => {
            Ok(OperandValue::Text(Cow::Owned(if *b {
                "true".into()
            } else {
                "false".into()
            })))
        }
        Expr::Value(sqlparser::ast::Value::Null) => Ok(OperandValue::Null),
        Expr::Nested(inner) => resolve_operand(inner, row_idx, column_ordinals, materialized),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let OperandValue::Text(value) =
                resolve_operand(expr, row_idx, column_ordinals, materialized)?
            {
                Ok(OperandValue::Text(Cow::Owned(format!("-{}", value))))
            } else {
                Ok(OperandValue::Null)
            }
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported operand expression: {expr:?}"
        ))),
    }
}

fn evaluate_column_truthy(
    column_name: &str,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &MaterializedColumns,
) -> Result<bool, SqlExecutionError> {
    let value = column_operand(column_name, row_idx, column_ordinals, materialized)?;
    Ok(match value {
        OperandValue::Null => false,
        OperandValue::Text(text) => literal_is_truthy(text.as_ref()),
    })
}

fn literal_is_truthy(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    if is_null_value(trimmed) {
        return false;
    }

    let lowered = trimmed.to_ascii_lowercase();
    !matches!(lowered.as_str(), "false" | "0" | "no" | "off" | "f" | "n")
}

fn column_operand<'a>(
    column_name: &str,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &'a MaterializedColumns,
) -> Result<OperandValue<'a>, SqlExecutionError> {
    let ordinal = column_ordinals.get(column_name).copied().ok_or_else(|| {
        SqlExecutionError::Unsupported(format!("unknown column referenced: {column_name}"))
    })?;
    let value = materialized
        .get(&ordinal)
        .and_then(|column_map| column_map.get(&row_idx));
    match value {
        Some(CachedValue::Null) => Ok(OperandValue::Null),
        Some(CachedValue::Text(text)) => Ok(OperandValue::Text(Cow::Borrowed(text.as_str()))),
        None => Ok(OperandValue::Null),
    }
}

pub(super) fn evaluate_selection_on_page(
    expr: &Expr,
    pages: &HashMap<String, &ColumnarPage>,
) -> Result<Bitmap, SqlExecutionError> {
    let num_rows = pages
        .values()
        .next()
        .map(|page| page.len())
        .unwrap_or(0);

    if num_rows == 0 {
        return Ok(Bitmap::new(0));
    }

    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let mut left_bitmap = evaluate_selection_on_page(left, pages)?;
                let right_bitmap = evaluate_selection_on_page(right, pages)?;
                left_bitmap.and(&right_bitmap);
                Ok(left_bitmap)
            }
            BinaryOperator::Or => {
                let mut left_bitmap = evaluate_selection_on_page(left, pages)?;
                let right_bitmap = evaluate_selection_on_page(right, pages)?;
                left_bitmap.or(&right_bitmap);
                Ok(left_bitmap)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let (column, literal, normalized_op) =
                    resolve_column_literal(left, right, op).ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "vectorized WHERE only supports column-to-literal predicates".into(),
                        )
                    })?;
                let page = pages.get(&column).ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "column {column} not available for vectorized evaluation"
                    ))
                })?;
                evaluate_column_comparison(&normalized_op, &literal, page)
            }
            _ => Err(SqlExecutionError::Unsupported(format!(
                "vectorized WHERE does not support operator {op:?}"
            ))),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let mut bitmap = evaluate_selection_on_page(expr, pages)?;
                bitmap.invert();
                Ok(bitmap)
            }
            _ => Err(SqlExecutionError::Unsupported(format!(
                "vectorized WHERE does not support unary operator {op:?}"
            ))),
        },
        Expr::Nested(inner) => evaluate_selection_on_page(inner, pages),
        Expr::IsNull(inner) => evaluate_null_predicate(inner, pages, true),
        Expr::IsNotNull(inner) => evaluate_null_predicate(inner, pages, false),
        _ => Err(SqlExecutionError::Unsupported(
            "predicate not supported by vectorized WHERE".into(),
        )),
    }
}

fn resolve_column_literal<'a>(
    left: &'a Expr,
    right: &'a Expr,
    op: &BinaryOperator,
) -> Option<(String, String, BinaryOperator)> {
    if let Some(column) = column_name_from_expr(left) {
        if let Ok(literal) = expr_to_string(right) {
            return Some((column, literal, op.clone()));
        }
    }

    if let Some(column) = column_name_from_expr(right) {
        if let Ok(literal) = expr_to_string(left) {
            let swapped = reverse_operator(op)?;
            return Some((column, literal, swapped));
        }
    }

    None
}

fn reverse_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator::*;
    match op {
        Gt => Some(Lt),
        GtEq => Some(LtEq),
        Lt => Some(Gt),
        LtEq => Some(GtEq),
        Eq => Some(Eq),
        NotEq => Some(NotEq),
        _ => None,
    }
}

fn evaluate_null_predicate(
    expr: &Expr,
    pages: &HashMap<String, &ColumnarPage>,
    expect_null: bool,
) -> Result<Bitmap, SqlExecutionError> {
    let column = column_name_from_expr(expr).ok_or_else(|| {
        SqlExecutionError::Unsupported("IS [NOT] NULL requires a column reference".into())
    })?;
    let page = pages.get(&column).ok_or_else(|| {
        SqlExecutionError::Unsupported(format!(
            "column {column} not available for vectorized NULL check"
        ))
    })?;
    let mut bitmap = Bitmap::new(page.len());
    for idx in 0..page.len() {
        if page.null_bitmap.is_set(idx) == expect_null {
            bitmap.set(idx);
        }
    }
    Ok(bitmap)
}

fn evaluate_column_comparison(
    op: &BinaryOperator,
    literal: &str,
    page: &ColumnarPage,
) -> Result<Bitmap, SqlExecutionError> {
    use BinaryOperator::*;
    if literal_is_null(literal) {
        return match op {
            Eq => {
                let mut bitmap = Bitmap::new(page.len());
                for idx in 0..page.len() {
                    if page.null_bitmap.is_set(idx) {
                        bitmap.set(idx);
                    }
                }
                Ok(bitmap)
            }
            NotEq => {
                let mut bitmap = Bitmap::new(page.len());
                for idx in 0..page.len() {
                    if !page.null_bitmap.is_set(idx) {
                        bitmap.set(idx);
                    }
                }
                Ok(bitmap)
            }
            _ => Err(SqlExecutionError::Unsupported(
                "comparisons between NULL and literals are not supported in vectorized mode".into(),
            )),
        };
    }

    match (&page.data, op) {
        (ColumnData::Int64(values), Eq)
        | (ColumnData::Int64(values), NotEq)
        | (ColumnData::Int64(values), Gt)
        | (ColumnData::Int64(values), GtEq)
        | (ColumnData::Int64(values), Lt)
        | (ColumnData::Int64(values), LtEq) => {
            let target = literal.parse::<i64>().map_err(|_| {
                SqlExecutionError::Unsupported(
                    "unable to parse literal as INTEGER for vectorized comparison".into(),
                )
            })?;
            Ok(build_numeric_bitmap(
                values,
                &page.null_bitmap,
                op,
                target as f64,
            ))
        }
        (ColumnData::Float64(values), Eq)
        | (ColumnData::Float64(values), NotEq)
        | (ColumnData::Float64(values), Gt)
        | (ColumnData::Float64(values), GtEq)
        | (ColumnData::Float64(values), Lt)
        | (ColumnData::Float64(values), LtEq) => {
            let target = literal.parse::<f64>().map_err(|_| {
                SqlExecutionError::Unsupported(
                    "unable to parse literal as FLOAT for vectorized comparison".into(),
                )
            })?;
            Ok(build_float_bitmap(
                values,
                &page.null_bitmap,
                op,
                target,
            ))
        }
        (ColumnData::Text(values), Eq)
        | (ColumnData::Text(values), NotEq)
        | (ColumnData::Text(values), Gt)
        | (ColumnData::Text(values), GtEq)
        | (ColumnData::Text(values), Lt)
        | (ColumnData::Text(values), LtEq) => {
            Ok(build_text_bitmap(values, &page.null_bitmap, op, literal))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "vectorized operator {op:?} not supported for column type"
        ))),
    }
}

fn literal_is_null(value: &str) -> bool {
    is_encoded_null(value) || value.eq_ignore_ascii_case("null")
}

fn build_numeric_bitmap(
    values: &[i64],
    nulls: &Bitmap,
    op: &BinaryOperator,
    target: f64,
) -> Bitmap {
    let mut bitmap = Bitmap::new(values.len());
    for (idx, value) in values.iter().enumerate() {
        if nulls.is_set(idx) {
            continue;
        }
        let lhs = *value as f64;
        if compare_floats(lhs, target, &op) {
            bitmap.set(idx);
        }
    }
    bitmap
}

fn build_float_bitmap(
    values: &[f64],
    nulls: &Bitmap,
    op: &BinaryOperator,
    target: f64,
) -> Bitmap {
    let mut bitmap = Bitmap::new(values.len());
    for (idx, value) in values.iter().enumerate() {
        if nulls.is_set(idx) {
            continue;
        }
        if compare_floats(*value, target, &op) {
            bitmap.set(idx);
        }
    }
    bitmap
}

fn build_text_bitmap(
    values: &[String],
    nulls: &Bitmap,
    op: &BinaryOperator,
    target: &str,
) -> Bitmap {
    let mut bitmap = Bitmap::new(values.len());
    for (idx, value) in values.iter().enumerate() {
        if nulls.is_set(idx) {
            continue;
        }
        let ordering = compare_strs(value, target);
        let matches = match *op {
            BinaryOperator::Eq => ordering == Ordering::Equal,
            BinaryOperator::NotEq => ordering != Ordering::Equal,
            BinaryOperator::Gt => ordering == Ordering::Greater,
            BinaryOperator::GtEq => ordering == Ordering::Greater || ordering == Ordering::Equal,
            BinaryOperator::Lt => ordering == Ordering::Less,
            BinaryOperator::LtEq => ordering == Ordering::Less || ordering == Ordering::Equal,
            _ => false,
        };
        if matches {
            bitmap.set(idx);
        }
    }
    bitmap
}

fn compare_floats(lhs: f64, rhs: f64, op: &BinaryOperator) -> bool {
    use BinaryOperator::*;
    match *op {
        Eq => (lhs - rhs).abs() < f64::EPSILON,
        NotEq => (lhs - rhs).abs() >= f64::EPSILON,
        Gt => lhs > rhs,
        GtEq => lhs >= rhs,
        Lt => lhs < rhs,
        LtEq => lhs <= rhs,
        _ => false,
    }
}
