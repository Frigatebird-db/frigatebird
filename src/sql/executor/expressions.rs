use super::SqlExecutionError;
use super::aggregates::{
    AggregateDataset, MaterializedColumns, evaluate_aggregate_function, is_aggregate_function,
};
use super::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use super::helpers::{like_match, regex_match};
use super::row_functions::evaluate_row_function;
use super::scalar_functions::evaluate_scalar_function;
use super::values::{
    ScalarValue, cached_to_scalar, cached_to_scalar_with_type, combine_numeric,
    compare_scalar_values, scalar_from_f64,
};
use crate::metadata_store::TableCatalog;
use sqlparser::ast::{
    BinaryOperator, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, UnaryOperator,
    Value,
};
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
                    let scalar = dataset
                        .column_type(&ident.value)
                        .map(|data_type| cached_to_scalar_with_type(value, data_type))
                        .unwrap_or_else(|| cached_to_scalar(value));
                    return Ok(scalar);
                }
            }
            Ok(ScalarValue::Null)
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                for &row in dataset.rows {
                    if let Some(value) = dataset.column_value(&last.value, row) {
                        let scalar = dataset
                            .column_type(&last.value)
                            .map(|data_type| cached_to_scalar_with_type(value, data_type))
                            .unwrap_or_else(|| cached_to_scalar(value));
                        return Ok(scalar);
                    }
                }
                Ok(ScalarValue::Null)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::String(s.clone())),
        Expr::Value(Value::Number(n, _)) => {
            if n.contains('.') || n.contains('e') || n.contains('E') {
                Ok(match n.parse::<f64>() {
                    Ok(num) => ScalarValue::Float64(num),
                    Err(_) => ScalarValue::String(n.clone()),
                })
            } else {
                Ok(match n.parse::<i64>() {
                    Ok(num) => ScalarValue::Int64(num),
                    Err(_) => ScalarValue::String(n.clone()),
                })
            }
        }
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Boolean(*b)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::Function(function) => {
            if is_aggregate_function(function) {
                evaluate_aggregate_function(function, dataset)
            } else {
                evaluate_scalar_function(function, dataset)
            }
        }
        Expr::Ceil { expr, field } => {
            if *field != DateTimeField::NoDateTime {
                return Err(SqlExecutionError::Unsupported(
                    "CEIL only supports numeric expressions".into(),
                ));
            }
            let value = evaluate_scalar_expression(expr, dataset)?;
            if value.is_null() {
                return Ok(ScalarValue::Null);
            }
            let num = value.as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(num.ceil()))
        }
        Expr::Floor { expr, field } => {
            if *field != DateTimeField::NoDateTime {
                return Err(SqlExecutionError::Unsupported(
                    "FLOOR only supports numeric expressions".into(),
                ));
            }
            let value = evaluate_scalar_expression(expr, dataset)?;
            if value.is_null() {
                return Ok(ScalarValue::Null);
            }
            let num = value.as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(num.floor()))
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
                    Ok(ScalarValue::Boolean(a && b))
                }
                BinaryOperator::Or => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Boolean(a || b))
                }
                BinaryOperator::Xor => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Boolean(a ^ b))
                }
                BinaryOperator::Eq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = match compare_scalar_values(&lhs, &rhs) {
                        Some(ord) => ord == Ordering::Greater,
                        None => false,
                    };
                    Ok(ScalarValue::Boolean(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Boolean(
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
                UnaryOperator::Not => Ok(ScalarValue::Boolean(!value.as_bool().unwrap_or(false))),
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
            negated,
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
        }
        Expr::ILike {
            expr,
            pattern,
            negated,
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !between
            } else {
                between
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
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
            .map(|value| {
                dataset
                    .column_type(&ident.value)
                    .map(|data_type| cached_to_scalar_with_type(value, data_type))
                    .unwrap_or_else(|| cached_to_scalar(value))
            })
            .unwrap_or(ScalarValue::Null)),
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                Ok(dataset
                    .column_value(&last.value, row_idx)
                    .map(|value| {
                        dataset
                            .column_type(&last.value)
                            .map(|data_type| cached_to_scalar_with_type(value, data_type))
                            .unwrap_or_else(|| cached_to_scalar(value))
                    })
                    .unwrap_or(ScalarValue::Null))
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::String(s.clone())),
        Expr::Value(Value::Number(n, _)) => Ok(match n.parse::<f64>() {
            Ok(num) => ScalarValue::Float64(num),
            Err(_) => ScalarValue::String(n.clone()),
        }),
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Boolean(*b)),
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
                UnaryOperator::Not => Ok(ScalarValue::Boolean(!value.as_bool().unwrap_or(false))),
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
                BinaryOperator::And => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) && rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) || rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) ^ rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Eq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater)
                        .unwrap_or(false);
                    Ok(ScalarValue::Boolean(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Boolean(
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
            negated,
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
        }
        Expr::ILike {
            expr,
            pattern,
            negated,
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !between
            } else {
                between
            }))
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
            Ok(ScalarValue::Boolean(if *negated {
                !matches
            } else {
                matches
            }))
        }
        Expr::IsNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Boolean(value.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Boolean(!value.is_null()))
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
                Err(SqlExecutionError::Unsupported(
                    "window functions are not supported in row expressions".into(),
                ))
            } else {
                evaluate_row_function(function, row_idx, dataset)
            }
        }
        Expr::Ceil { expr, field } => {
            if *field != DateTimeField::NoDateTime {
                return Err(SqlExecutionError::Unsupported(
                    "CEIL only supports numeric expressions".into(),
                ));
            }
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            if value.is_null() {
                return Ok(ScalarValue::Null);
            }
            let num = value.as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(num.ceil()))
        }
        Expr::Floor { expr, field } => {
            if *field != DateTimeField::NoDateTime {
                return Err(SqlExecutionError::Unsupported(
                    "FLOOR only supports numeric expressions".into(),
                ));
            }
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            if value.is_null() {
                return Ok(ScalarValue::Null);
            }
            let num = value.as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(num.floor()))
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

pub(crate) fn evaluate_expression_on_batch(
    expr: &Expr,
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<ColumnarPage, SqlExecutionError> {
    match expr {
        Expr::Identifier(ident) => resolve_batch_column(&ident.value, batch, catalog),
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                resolve_batch_column(&last.value, batch, catalog)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(text)) => {
            Ok(ColumnarPage::from_literal_text(text, batch.num_rows))
        }
        Expr::Value(Value::Number(raw, _)) => {
            let value = raw.parse::<f64>().map_err(|_| {
                SqlExecutionError::Unsupported(format!("unable to parse numeric literal '{raw}'"))
            })?;
            Ok(ColumnarPage::from_literal_f64(value, batch.num_rows))
        }
        Expr::Value(Value::Boolean(flag)) => {
            Ok(ColumnarPage::from_literal_bool(*flag, batch.num_rows))
        }
        Expr::Value(Value::Null) => Ok(ColumnarPage::from_nulls(batch.num_rows)),
        Expr::BinaryOp { left, op, right } => {
            let left_page = evaluate_expression_on_batch(left, batch, catalog)?;
            let right_page = evaluate_expression_on_batch(right, batch, catalog)?;
            match op {
                BinaryOperator::Plus => {
                    vectorized_numeric_binary_op(&left_page, &right_page, |a, b| a + b)
                }
                BinaryOperator::Minus => {
                    vectorized_numeric_binary_op(&left_page, &right_page, |a, b| a - b)
                }
                BinaryOperator::Multiply => {
                    vectorized_numeric_binary_op(&left_page, &right_page, |a, b| a * b)
                }
                BinaryOperator::Divide => {
                    vectorized_numeric_binary_op(&left_page, &right_page, |a, b| a / b)
                }
                BinaryOperator::Eq => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord == Ordering::Equal
                    })
                }
                BinaryOperator::NotEq => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord != Ordering::Equal
                    })
                }
                BinaryOperator::Gt => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord == Ordering::Greater
                    })
                }
                BinaryOperator::GtEq => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord == Ordering::Greater || ord == Ordering::Equal
                    })
                }
                BinaryOperator::Lt => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord == Ordering::Less
                    })
                }
                BinaryOperator::LtEq => {
                    vectorized_numeric_comparison_op(&left_page, &right_page, |ord| {
                        ord == Ordering::Less || ord == Ordering::Equal
                    })
                }
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "operator {op:?} is not supported in vectorized projection"
                ))),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let page = evaluate_expression_on_batch(expr, batch, catalog)?;
            match op {
                UnaryOperator::Plus => Ok(page),
                UnaryOperator::Minus => vectorized_numeric_unary_op(&page, |value| -value),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unary operator {op:?} is not supported in vectorized projection"
                ))),
            }
        }
        Expr::IsNull(inner) => {
            let page = evaluate_expression_on_batch(inner, batch, catalog)?;
            let mut values = Vec::with_capacity(page.num_rows);
            for idx in 0..page.num_rows {
                values.push(page.null_bitmap.is_set(idx));
            }
            Ok(ColumnarPage {
                page_metadata: String::new(),
                data: ColumnData::Boolean(values),
                null_bitmap: Bitmap::new(page.num_rows),
                num_rows: page.num_rows,
            })
        }
        Expr::IsNotNull(inner) => {
            let page = evaluate_expression_on_batch(inner, batch, catalog)?;
            let mut values = Vec::with_capacity(page.num_rows);
            for idx in 0..page.num_rows {
                values.push(!page.null_bitmap.is_set(idx));
            }
            Ok(ColumnarPage {
                page_metadata: String::new(),
                data: ColumnData::Boolean(values),
                null_bitmap: Bitmap::new(page.num_rows),
                num_rows: page.num_rows,
            })
        }
        Expr::Nested(inner) => evaluate_expression_on_batch(inner, batch, catalog),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case_expression_vectorized(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            batch,
            catalog,
        ),
        Expr::Function(function) => evaluate_scalar_function_on_batch(function, batch, catalog),
        _ => Err(SqlExecutionError::Unsupported(
            "expression not supported by vectorized projection".into(),
        )),
    }
}

fn evaluate_scalar_function_on_batch(
    function: &Function,
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<ColumnarPage, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    match name.as_str() {
        "COALESCE" => evaluate_coalesce_function(function, batch, catalog),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "function {name} is not supported in vectorized projection"
        ))),
    }
}

fn evaluate_coalesce_function(
    function: &Function,
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<ColumnarPage, SqlExecutionError> {
    if function.args.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "COALESCE requires at least one argument".into(),
        ));
    }

    let mut arg_pages = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        let expr = match arg {
            FunctionArg::Unnamed(item) | FunctionArg::Named { arg: item, .. } => match item {
                FunctionArgExpr::Expr(expr) => expr,
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "COALESCE arguments must be expressions".into(),
                    ));
                }
            },
        };
        arg_pages.push(evaluate_expression_on_batch(expr, batch, catalog)?);
    }

    let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut selected = None;
        for page in &arg_pages {
            if !page.null_bitmap.is_set(row_idx) {
                selected = page.value_as_string(row_idx);
                if selected.is_some() {
                    break;
                }
            }
        }
        values.push(selected);
    }

    Ok(column_from_strings(values))
}

fn evaluate_case_expression_vectorized(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<ColumnarPage, SqlExecutionError> {
    if conditions.len() != results.len() {
        return Err(SqlExecutionError::Unsupported(
            "CASE expression requires matching WHEN/THEN clauses".into(),
        ));
    }

    let result_pages: Vec<ColumnarPage> = results
        .iter()
        .map(|expr| evaluate_expression_on_batch(expr, batch, catalog))
        .collect::<Result<_, _>>()?;
    let else_page = if let Some(expr) = else_result {
        Some(evaluate_expression_on_batch(expr, batch, catalog)?)
    } else {
        None
    };

    let numeric_output = result_pages.iter().all(page_is_numeric)
        && else_page.as_ref().map_or(true, page_is_numeric);

    if numeric_output {
        let mut values: Vec<f64> = Vec::with_capacity(batch.num_rows);
        let mut null_bitmap = Bitmap::new(batch.num_rows);
        if let Some(op_expr) = operand {
            let operand_page = evaluate_expression_on_batch(op_expr, batch, catalog)?;
            let condition_pages: Vec<ColumnarPage> = conditions
                .iter()
                .map(|expr| evaluate_expression_on_batch(expr, batch, catalog))
                .collect::<Result<_, _>>()?;
            for row_idx in 0..batch.num_rows {
                let operand_value = operand_page.value_as_string(row_idx);
                let mut selected = None;
                let mut matched = false;
                if operand_value.is_some() {
                    for (cond_page, result_page) in
                        condition_pages.iter().zip(result_pages.iter())
                    {
                        let cond_value = cond_page.value_as_string(row_idx);
                        if cond_value.is_some() && cond_value == operand_value {
                            matched = true;
                            selected = page_numeric_value(result_page, row_idx);
                            break;
                        }
                    }
                }
                if !matched {
                    selected = else_page
                        .as_ref()
                        .and_then(|page| page_numeric_value(page, row_idx));
                }
                match selected {
                    Some(value) => values.push(value),
                    None => {
                        null_bitmap.set(row_idx);
                        values.push(0.0);
                    }
                }
            }
        } else {
            let condition_pages: Vec<ColumnarPage> = conditions
                .iter()
                .map(|expr| evaluate_expression_on_batch(expr, batch, catalog))
                .collect::<Result<_, _>>()?;
            for row_idx in 0..batch.num_rows {
                let mut selected = None;
                let mut matched = false;
                for (cond_page, result_page) in
                    condition_pages.iter().zip(result_pages.iter())
                {
                    if page_value_truthy(cond_page, row_idx) {
                        matched = true;
                        selected = page_numeric_value(result_page, row_idx);
                        break;
                    }
                }
                if !matched {
                    selected = else_page
                        .as_ref()
                        .and_then(|page| page_numeric_value(page, row_idx));
                }
                match selected {
                    Some(value) => values.push(value),
                    None => {
                        null_bitmap.set(row_idx);
                        values.push(0.0);
                    }
                }
            }
        }

        return Ok(ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Float64(values),
            null_bitmap,
            num_rows: batch.num_rows,
        });
    }

    let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows);
    if let Some(op_expr) = operand {
        let operand_page = evaluate_expression_on_batch(op_expr, batch, catalog)?;
        let condition_pages: Vec<ColumnarPage> = conditions
            .iter()
            .map(|expr| evaluate_expression_on_batch(expr, batch, catalog))
            .collect::<Result<_, _>>()?;
        for row_idx in 0..batch.num_rows {
            let operand_value = operand_page.value_as_string(row_idx);
            let mut selected = None;
            let mut matched = false;
            if operand_value.is_some() {
                for (cond_page, result_page) in condition_pages.iter().zip(result_pages.iter()) {
                    let cond_value = cond_page.value_as_string(row_idx);
                    if cond_value.is_some() && cond_value == operand_value {
                        matched = true;
                        selected = result_page.value_as_string(row_idx);
                        break;
                    }
                }
            }
            if !matched {
                selected = else_page
                    .as_ref()
                    .and_then(|page| page.value_as_string(row_idx));
            }
            values.push(selected);
        }
    } else {
        let condition_pages: Vec<ColumnarPage> = conditions
            .iter()
            .map(|expr| evaluate_expression_on_batch(expr, batch, catalog))
            .collect::<Result<_, _>>()?;
        for row_idx in 0..batch.num_rows {
            let mut selected = None;
            let mut matched = false;
            for (cond_page, result_page) in condition_pages.iter().zip(result_pages.iter()) {
                if page_value_truthy(cond_page, row_idx) {
                    matched = true;
                    selected = result_page.value_as_string(row_idx);
                    break;
                }
            }
            if !matched {
                selected = else_page
                    .as_ref()
                    .and_then(|page| page.value_as_string(row_idx));
            }
            values.push(selected);
        }
    }

    Ok(column_from_strings(values))
}

fn page_value_truthy(page: &ColumnarPage, row_idx: usize) -> bool {
    if page.null_bitmap.is_set(row_idx) {
        return false;
    }

    match &page.data {
        ColumnData::Int64(values) => values[row_idx] != 0,
        ColumnData::Float64(values) => values[row_idx] != 0.0,
        ColumnData::Text(col) => {
            let value = col.get_string(row_idx);
            let trimmed = value.trim();
            matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "true" | "t" | "1" | "yes" | "y"
            )
        }
        ColumnData::Boolean(values) => values[row_idx],
        ColumnData::Timestamp(values) => values[row_idx] != 0,
        ColumnData::Dictionary(dict) => {
            let value = dict.get_string(row_idx);
            let trimmed = value.trim();
            matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "true" | "t" | "1" | "yes" | "y"
            )
        }
    }
}

fn page_is_numeric(page: &ColumnarPage) -> bool {
    matches!(page.data, ColumnData::Int64(_) | ColumnData::Float64(_))
}

fn page_numeric_value(page: &ColumnarPage, row_idx: usize) -> Option<f64> {
    if page.null_bitmap.is_set(row_idx) {
        return None;
    }
    match &page.data {
        ColumnData::Int64(values) => values.get(row_idx).copied().map(|value| value as f64),
        ColumnData::Float64(values) => values.get(row_idx).copied(),
        _ => None,
    }
}

fn column_from_strings(values: Vec<Option<String>>) -> ColumnarPage {
    let len = values.len();
    // Estimate 16 bytes per string
    let mut col = BytesColumn::with_capacity(len, len * 16);
    let mut bitmap = Bitmap::new(len);
    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Some(text) => col.push(&text),
            None => {
                bitmap.set(idx);
                col.push("");
            }
        }
    }
    ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Text(col),
        null_bitmap: bitmap,
        num_rows: len,
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

fn resolve_batch_column(
    name: &str,
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<ColumnarPage, SqlExecutionError> {
    if let Some(&ordinal) = batch.aliases.get(name) {
        return batch.columns.get(&ordinal).cloned().ok_or_else(|| {
            SqlExecutionError::OperationFailed(format!(
                "missing computed column {name} in vectorized batch"
            ))
        });
    }

    let column = catalog
        .column(name)
        .ok_or_else(|| SqlExecutionError::ColumnMismatch {
            table: catalog.name.clone(),
            column: name.to_string(),
        })?;
    batch.columns.get(&column.ordinal).cloned().ok_or_else(|| {
        SqlExecutionError::OperationFailed(format!("column {name} missing from vectorized batch"))
    })
}

fn vectorized_numeric_binary_op<F>(
    left: &ColumnarPage,
    right: &ColumnarPage,
    op: F,
) -> Result<ColumnarPage, SqlExecutionError>
where
    F: Fn(f64, f64) -> f64,
{
    let len = left.len();
    if right.len() != len {
        return Err(SqlExecutionError::Unsupported(
            "vectorized expressions require operands with equal lengths".into(),
        ));
    }
    let mut values = Vec::with_capacity(len);
    let mut null_bitmap = Bitmap::new(len);
    for idx in 0..len {
        if left.null_bitmap.is_set(idx) || right.null_bitmap.is_set(idx) {
            null_bitmap.set(idx);
            values.push(0.0);
            continue;
        }
        let lhs = numeric_value_at(left, idx)?;
        let rhs = numeric_value_at(right, idx)?;
        values.push(op(lhs, rhs));
    }
    Ok(ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Float64(values),
        null_bitmap,
        num_rows: len,
    })
}

fn vectorized_numeric_unary_op<F>(
    page: &ColumnarPage,
    op: F,
) -> Result<ColumnarPage, SqlExecutionError>
where
    F: Fn(f64) -> f64,
{
    let len = page.len();
    let mut values = Vec::with_capacity(len);
    let null_bitmap = page.null_bitmap.clone();
    for idx in 0..len {
        if page.null_bitmap.is_set(idx) {
            values.push(0.0);
            continue;
        }
        let value = numeric_value_at(page, idx)?;
        values.push(op(value));
    }
    Ok(ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Float64(values),
        null_bitmap,
        num_rows: len,
    })
}

fn vectorized_numeric_comparison_op<F>(
    left: &ColumnarPage,
    right: &ColumnarPage,
    predicate: F,
) -> Result<ColumnarPage, SqlExecutionError>
where
    F: Fn(Ordering) -> bool,
{
    let len = left.len();
    if right.len() != len {
        return Err(SqlExecutionError::Unsupported(
            "vectorized expressions require operands with equal lengths".into(),
        ));
    }

    let mut col = BytesColumn::with_capacity(len, len * 5); // "true"/"false" ~5 chars
    let mut null_bitmap = Bitmap::new(len);
    for idx in 0..len {
        if left.null_bitmap.is_set(idx) || right.null_bitmap.is_set(idx) {
            null_bitmap.set(idx);
            col.push("");
            continue;
        }
        let lhs = numeric_value_at(left, idx)?;
        let rhs = numeric_value_at(right, idx)?;
        let matches = lhs
            .partial_cmp(&rhs)
            .map(|ord| predicate(ord))
            .unwrap_or(false);
        col.push(&bool_to_text(matches));
    }

    Ok(ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Text(col),
        null_bitmap,
        num_rows: len,
    })
}

fn numeric_value_at(page: &ColumnarPage, idx: usize) -> Result<f64, SqlExecutionError> {
    match &page.data {
        ColumnData::Int64(values) => values
            .get(idx)
            .copied()
            .map(|value| value as f64)
            .ok_or_else(|| {
                SqlExecutionError::OperationFailed(
                    "vectorized expression index out of bounds".into(),
                )
            }),
        ColumnData::Float64(values) => values.get(idx).copied().ok_or_else(|| {
            SqlExecutionError::OperationFailed("vectorized expression index out of bounds".into())
        }),
        ColumnData::Text(values) => values
            .get_string(idx)
            .parse::<f64>()
            .map_err(|_| {
                SqlExecutionError::Unsupported(
                    "vectorized expression requires numeric operands".into(),
                )
            }),
        ColumnData::Dictionary(values) => values
            .get_string(idx)
            .parse::<f64>()
            .map_err(|_| {
                SqlExecutionError::Unsupported(
                    "vectorized expression requires numeric operands".into(),
                )
            }),
        _ => Err(SqlExecutionError::Unsupported(
            "vectorized expression requires numeric operands".into(),
        )),
    }
}

fn bool_to_text(value: bool) -> String {
    if value { "true".into() } else { "false".into() }
}
