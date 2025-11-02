use super::SqlExecutionError;
use super::aggregates::{
    AggregateDataset, MaterializedColumns, evaluate_aggregate_function, is_aggregate_function,
};
use super::helpers::{is_null_value, like_match, regex_match};
use super::row_functions::evaluate_row_function;
use super::scalar_functions::evaluate_scalar_function;
use super::values::{
    CachedValue, ScalarValue, cached_to_scalar, combine_numeric, compare_scalar_values,
    compare_strs, scalar_from_f64,
};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

pub(super) fn evaluate_scalar_expression(
    expr: &Expr,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    match expr {
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
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(SqlExecutionError::Unsupported(
            "aggregated SELECT cannot project plain columns".into(),
        )),
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
                    println!("row compare {:?} > {:?} => {}", lhs, rhs, result);
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
        Expr::Function(function) => evaluate_row_function(function, row_idx, dataset),
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
        Expr::IsNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(matches!(operand, OperandValue::Null)
                || matches!(
                operand,
                    OperandValue::Text(ref value) if is_null_value(value.as_ref())
                ))
        }
        Expr::IsNotNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(!matches!(operand, OperandValue::Null)
                && !matches!(
                    operand,
                    OperandValue::Text(ref value) if is_null_value(value.as_ref())
                ))
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
