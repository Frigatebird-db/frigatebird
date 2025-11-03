use super::SqlExecutionError;
use super::aggregates::AggregateDataset;
use super::expressions::evaluate_scalar_expression;
use super::row_functions::{evaluate_date_trunc_row, evaluate_time_bucket_row};
use super::values::{ScalarValue, scalar_from_f64};
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

pub(super) fn evaluate_scalar_function(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    if matches!(name.as_str(), "TIME_BUCKET" | "DATE_TRUNC") {
        if let Some(&row_idx) = dataset.rows.first() {
            return if name == "TIME_BUCKET" {
                evaluate_time_bucket_row(function, row_idx, dataset)
            } else {
                evaluate_date_trunc_row(function, row_idx, dataset)
            };
        } else {
            return Ok(ScalarValue::Null);
        }
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => args.push(evaluate_scalar_expression(expr, dataset)?),
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
        "LN" | "LOG" if args.len() == 1 => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LN requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ln()))
        }
        "LOG" if args.len() == 2 => {
            let base = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LOG requires numeric base".into())
            })?;
            let value = args.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LOG requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.log(base)))
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

            if buckets <= 0 || !high.is_finite() || !low.is_finite() || high <= low {
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
            "unsupported scalar function {name}"
        ))),
    }
}
