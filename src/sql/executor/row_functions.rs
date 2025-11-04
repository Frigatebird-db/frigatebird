use super::SqlExecutionError;
use super::aggregates::AggregateDataset;
use super::expressions::evaluate_row_expr;
use super::helpers::parse_interval_seconds;
use super::values::{ScalarValue, scalar_from_f64};
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, Value};

pub(super) fn evaluate_row_function(
    function: &Function,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    match name.as_str() {
        "TIME_BUCKET" => return evaluate_time_bucket_row(function, row_idx, dataset),
        "DATE_TRUNC" => return evaluate_date_trunc_row(function, row_idx, dataset),
        _ => {}
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => args.push(evaluate_row_expr(expr, row_idx, dataset)?),
            _ => {
                return Err(SqlExecutionError::Unsupported(format!(
                    "unsupported argument for function {name}"
                )));
            }
        }
    }

    match name.as_str() {
        "ABS" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ABS requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.abs()))
        }
        "ROUND" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ROUND requires numeric argument".into())
            })?;
            let digits = args
                .get(1)
                .and_then(|v| v.as_i128())
                .unwrap_or(0)
                .clamp(-18, 18) as i32;
            let factor = 10_f64.powi(digits);
            Ok(scalar_from_f64((value * factor).round() / factor))
        }
        "CEIL" | "CEILING" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ceil()))
        }
        "FLOOR" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.floor()))
        }
        "EXP" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("EXP requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.exp()))
        }
        "LN" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LN requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ln()))
        }
        "LOG" => {
            let result = match args.len() {
                1 => {
                    let value = args[0].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric argument".into())
                    })?;
                    value.ln()
                }
                2 => {
                    let base = args[0].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric base".into())
                    })?;
                    let value = args[1].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric argument".into())
                    })?;
                    value.log(base)
                }
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "LOG requires one or two arguments".into(),
                    ));
                }
            };
            Ok(scalar_from_f64(result))
        }
        "POWER" => {
            let base = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric base".into())
            })?;
            let exponent = args.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric exponent".into())
            })?;
            Ok(scalar_from_f64(base.powf(exponent)))
        }
        "WIDTH_BUCKET" => {
            if args.len() != 4 {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET requires four arguments".into(),
                ));
            }
            // Return NULL if the value is NULL
            if args[0].is_null() {
                return Ok(ScalarValue::Null);
            }
            let value = args[0].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric value".into())
            })?;
            let low = args[1].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric low".into())
            })?;
            let high = args[2].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric high".into())
            })?;
            let buckets = args[3].as_i128().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires integer bucket count".into())
            })?;

            if buckets <= 0 || high <= low {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET arguments out of range".into(),
                ));
            }

            let bucket = if value < low {
                0
            } else if value >= high {
                buckets + 1
            } else {
                let step = (high - low) / buckets as f64;
                ((value - low) / step).floor() as i128 + 1
            };
            Ok(ScalarValue::Int(bucket))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported row-level function {name}"
        ))),
    }
}

pub(super) fn evaluate_time_bucket_row(
    function: &Function,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.len() < 2 {
        return Err(SqlExecutionError::Unsupported(
            "TIME_BUCKET requires at least interval and value arguments".into(),
        ));
    }

    let interval_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "TIME_BUCKET interval must be a literal".into(),
            ));
        }
    };
    let interval = parse_interval_seconds(interval_expr, "TIME_BUCKET interval")?;
    if interval <= 0.0 {
        return Err(SqlExecutionError::Unsupported(
            "TIME_BUCKET interval must be positive".into(),
        ));
    }

    let value_expr = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "TIME_BUCKET value must be an expression".into(),
            ));
        }
    };

    let value = evaluate_row_expr(value_expr, row_idx, dataset)?;
    let numeric = match value.as_f64() {
        Some(v) => v,
        None => {
            return Err(SqlExecutionError::Unsupported(
                "DATE_TRUNC requires numeric timestamp values".into(),
            ));
        }
    };

    let origin = if function.args.len() >= 3 {
        match &function.args[2] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => evaluate_row_expr(expr, row_idx, dataset)?
                .as_f64()
                .unwrap_or(0.0),
            _ => 0.0,
        }
    } else {
        0.0
    };

    let bucket = ((numeric - origin) / interval).floor() * interval + origin;
    Ok(scalar_from_f64(bucket))
}

pub(super) fn evaluate_date_trunc_row(
    function: &Function,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.len() != 2 {
        return Err(SqlExecutionError::Unsupported(
            "DATE_TRUNC requires unit and value arguments".into(),
        ));
    }

    let unit = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => extract_literal_string(expr).ok_or_else(|| {
            SqlExecutionError::Unsupported("DATE_TRUNC unit must be a string literal".into())
        })?,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "DATE_TRUNC unit must be a literal".into(),
            ));
        }
    };

    let value_expr = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "DATE_TRUNC value must be an expression".into(),
            ));
        }
    };

    let value = evaluate_row_expr(value_expr, row_idx, dataset)?;
    let numeric = match value.as_f64() {
        Some(v) => v,
        None => return Ok(ScalarValue::Null),
    };

    let unit_seconds = match unit.to_lowercase().as_str() {
        "second" | "seconds" | "sec" | "s" => 1.0,
        "minute" | "minutes" | "min" | "mins" => 60.0,
        "hour" | "hours" | "hr" | "hrs" => 3_600.0,
        "day" | "days" => 86_400.0,
        "week" | "weeks" => 604_800.0,
        _ => {
            return Err(SqlExecutionError::Unsupported(format!(
                "DATE_TRUNC unit '{unit}' is not supported"
            )));
        }
    };

    let truncated = (numeric / unit_seconds).floor() * unit_seconds;
    Ok(scalar_from_f64(truncated))
}

fn extract_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(text)) => Some(text.clone()),
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}
