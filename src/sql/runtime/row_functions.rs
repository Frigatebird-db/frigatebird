use super::SqlExecutionError;
use super::aggregates::AggregateDataset;
use super::expressions::evaluate_row_expr;
use super::helpers::parse_interval_seconds;
use super::values::{ScalarValue, scalar_from_f64};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, Value};

pub(crate) fn evaluate_row_function(
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
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ABS requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.abs()))
        }
        "ROUND" => {
            if args.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "ROUND requires numeric argument".into(),
                ));
            }
            if dataset.prefer_exact_numeric() {
                return Ok(args.into_iter().next().unwrap());
            }
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ROUND requires numeric argument".into())
            })?;
            let digits = args
                .get(1)
                .and_then(|v| match v {
                    ScalarValue::Int64(i) => Some(*i),
                    ScalarValue::Float64(f) if f.is_finite() && f.fract().abs() < f64::EPSILON => {
                        Some(*f as i64)
                    }
                    ScalarValue::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                })
                .unwrap_or(0)
                .clamp(-18, 18) as i32;
            let factor = 10_f64.powi(digits);
            Ok(scalar_from_f64((value * factor).round() / factor))
        }
        "CEIL" | "CEILING" => {
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ceil()))
        }
        "FLOOR" => {
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.floor()))
        }
        "EXP" => {
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("EXP requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.exp()))
        }
        "LN" => {
            let value = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
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
                    value.log10()
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
            let base = args.first().and_then(|v| v.as_f64()).ok_or_else(|| {
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
            let buckets = match &args[3] {
                ScalarValue::Int64(i) => Some(*i),
                ScalarValue::Float64(f) if f.is_finite() && f.fract().abs() < f64::EPSILON => {
                    Some(*f as i64)
                }
                ScalarValue::String(s) => s.parse::<i64>().ok(),
                _ => None,
            }
            .ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires integer bucket count".into())
            })?;

            if buckets <= 0 || !high.is_finite() || !low.is_finite() || high <= low {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET arguments out of range".into(),
                ));
            }

            let bucket = if value < low {
                0
            } else if value > high {
                buckets + 1
            } else if (value - high).abs() < f64::EPSILON {
                buckets
            } else {
                let step = (high - low) / buckets as f64;
                (((value - low) / step).floor() as i64) + 1
            };
            Ok(ScalarValue::Int64(bucket))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported row-level function {name}"
        ))),
    }
}

pub(crate) fn evaluate_time_bucket_row(
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
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }
    let (value_seconds, value_kind) = scalar_to_seconds(&value).ok_or_else(|| {
        SqlExecutionError::Unsupported(
            "TIME_BUCKET value must be numeric or a timestamp literal".into(),
        )
    })?;

    let origin_seconds = if function.args.len() >= 3 {
        match &function.args[2] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => {
                let origin_value = evaluate_row_expr(expr, row_idx, dataset)?;
                if origin_value.is_null() {
                    0.0
                } else {
                    scalar_to_seconds(&origin_value)
                        .ok_or_else(|| {
                            SqlExecutionError::Unsupported(
                                "TIME_BUCKET origin must be numeric or a timestamp literal".into(),
                            )
                        })?
                        .0
                }
            }
            _ => 0.0,
        }
    } else {
        0.0
    };

    let bucket = ((value_seconds - origin_seconds) / interval).floor() * interval + origin_seconds;
    Ok(format_time_value(bucket, value_kind))
}

pub(crate) fn evaluate_date_trunc_row(
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
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }

    let (value_seconds, value_kind) = scalar_to_seconds(&value).ok_or_else(|| {
        SqlExecutionError::Unsupported("DATE_TRUNC requires numeric or timestamp inputs".into())
    })?;

    let truncated_seconds = if let Some(dt) = seconds_to_datetime(value_seconds) {
        let truncated = truncate_datetime(&unit, dt)?;
        datetime_to_seconds(&truncated)
    } else {
        let unit_seconds = unit_to_seconds(&unit)?;
        (value_seconds / unit_seconds).floor() * unit_seconds
    };

    Ok(format_time_value(truncated_seconds, value_kind))
}

fn truncate_datetime(unit: &str, dt: NaiveDateTime) -> Result<NaiveDateTime, SqlExecutionError> {
    let lowered = unit.to_lowercase();
    let truncated = match lowered.as_str() {
        "second" | "seconds" | "sec" | "s" => dt.with_nanosecond(0),
        "minute" | "minutes" | "min" | "mins" => {
            dt.with_second(0).and_then(|v| v.with_nanosecond(0))
        }
        "hour" | "hours" | "hr" | "hrs" => dt
            .with_minute(0)
            .and_then(|v| v.with_second(0))
            .and_then(|v| v.with_nanosecond(0)),
        "day" | "days" => dt.date().and_hms_opt(0, 0, 0),
        "week" | "weeks" => {
            let weekday = dt.weekday().num_days_from_monday() as i64;
            let start_date: NaiveDate = dt.date() - Duration::days(weekday);
            start_date.and_hms_opt(0, 0, 0)
        }
        _ => {
            return Err(SqlExecutionError::Unsupported(format!(
                "DATE_TRUNC unit '{unit}' is not supported"
            )));
        }
    };

    truncated.ok_or_else(|| {
        SqlExecutionError::Unsupported(format!("failed to truncate timestamp for unit '{unit}'"))
    })
}

fn unit_to_seconds(unit: &str) -> Result<f64, SqlExecutionError> {
    match unit.to_lowercase().as_str() {
        "second" | "seconds" | "sec" | "s" => Ok(1.0),
        "minute" | "minutes" | "min" | "mins" => Ok(60.0),
        "hour" | "hours" | "hr" | "hrs" => Ok(3_600.0),
        "day" | "days" => Ok(86_400.0),
        "week" | "weeks" => Ok(604_800.0),
        other => Err(SqlExecutionError::Unsupported(format!(
            "DATE_TRUNC unit '{other}' is not supported"
        ))),
    }
}

#[derive(Clone, Copy)]
enum TimeValueKind {
    DateTime,
    Numeric,
    Timestamp,
}

fn scalar_to_seconds(value: &ScalarValue) -> Option<(f64, TimeValueKind)> {
    match value {
        ScalarValue::Int64(v) => numeric_to_seconds(*v as f64),
        ScalarValue::Float64(v) => numeric_to_seconds(*v),
        ScalarValue::Boolean(v) => Some((if *v { 1.0 } else { 0.0 }, TimeValueKind::Numeric)),
        ScalarValue::String(text) => parse_timestamp(text)
            .map(|ts| (ts.and_utc().timestamp() as f64, TimeValueKind::Timestamp))
            .or_else(|| text.parse::<f64>().ok().and_then(numeric_to_seconds)),
        ScalarValue::Timestamp(ts) => Some((*ts as f64 / 1_000_000.0, TimeValueKind::Timestamp)),
        ScalarValue::Null => None,
    }
}

fn numeric_to_seconds(value: f64) -> Option<(f64, TimeValueKind)> {
    if !value.is_finite() {
        return None;
    }
    let abs = value.abs();
    if abs >= 1_000_000_000_000.0 {
        Some((value / 1_000_000.0, TimeValueKind::Timestamp))
    } else {
        Some((value, TimeValueKind::Numeric))
    }
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").ok()
}

fn seconds_to_datetime(seconds: f64) -> Option<NaiveDateTime> {
    let secs = seconds.trunc() as i64;
    let nanos = ((seconds - secs as f64) * 1_000_000_000.0).round();
    let nanos = nanos.clamp(0.0, 999_999_999.0) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
}

fn datetime_to_seconds(dt: &NaiveDateTime) -> f64 {
    let utc = dt.and_utc();
    utc.timestamp() as f64 + utc.timestamp_subsec_nanos() as f64 / 1_000_000_000.0
}

fn format_time_value(seconds: f64, kind: TimeValueKind) -> ScalarValue {
    match kind {
        TimeValueKind::Numeric => scalar_from_f64(seconds),
        TimeValueKind::DateTime | TimeValueKind::Timestamp => seconds_to_datetime(seconds)
            .map(|dt| ScalarValue::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
            .unwrap_or_else(|| scalar_from_f64(seconds)),
    }
}

fn extract_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(text)) => Some(text.clone()),
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}
