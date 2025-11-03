use super::SqlExecutionError;
use super::expressions::{evaluate_row_expr, evaluate_scalar_expression};
use super::helpers::collect_expr_column_ordinals;
use super::values::{CachedValue, ScalarValue, scalar_from_f64};
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, SelectItem};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) type MaterializedColumns = HashMap<usize, HashMap<u64, CachedValue>>;
pub(super) type WindowResultMap = HashMap<String, Vec<ScalarValue>>;

pub(super) struct AggregateProjectionPlan {
    pub(super) outputs: Vec<AggregateProjection>,
    pub(super) required_ordinals: BTreeSet<usize>,
    pub(super) headers: Vec<String>,
}

pub(super) struct AggregateProjection {
    pub(super) expr: Expr,
}

pub(super) struct AggregateDataset<'a> {
    pub(super) rows: &'a [u64],
    pub(super) materialized: &'a MaterializedColumns,
    pub(super) column_ordinals: &'a HashMap<String, usize>,
    pub(super) row_positions: Option<&'a HashMap<u64, usize>>,
    pub(super) window_results: Option<&'a WindowResultMap>,
    pub(super) masked_exprs: Option<&'a [Expr]>,
}

impl<'a> AggregateDataset<'a> {
    pub(super) fn column_value(&self, column: &str, row_idx: u64) -> Option<&CachedValue> {
        self.column_ordinals
            .get(column)
            .and_then(|ordinal| self.materialized.get(ordinal))
            .and_then(|map| map.get(&row_idx))
    }

    pub(super) fn row_position(&self, row_idx: u64) -> Option<usize> {
        self.row_positions
            .and_then(|positions| positions.get(&row_idx).copied())
    }

    pub(super) fn window_value(&self, key: &str, position: usize) -> Option<&ScalarValue> {
        self.window_results
            .and_then(|map| map.get(key))
            .and_then(|values| values.get(position))
    }

    pub(super) fn is_expr_masked(&self, expr: &Expr) -> bool {
        self.masked_exprs
            .map(|masked| masked.iter().any(|masked_expr| masked_expr == expr))
            .unwrap_or(false)
    }
}

pub(super) fn select_item_contains_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_contains_aggregate(expr),
        SelectItem::ExprWithAlias { expr, .. } => expr_contains_aggregate(expr),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }
}

pub(super) fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => {
            if is_aggregate_function(function) {
                return true;
            }
            function.args.iter().any(|arg| match arg {
                FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => match arg {
                    FunctionArgExpr::Expr(expr) => expr_contains_aggregate(expr),
                    FunctionArgExpr::QualifiedWildcard(_) => false,
                    FunctionArgExpr::Wildcard => false,
                },
            }) || function
                .order_by
                .iter()
                .any(|order| expr_contains_aggregate(&order.expr))
                || function
                    .filter
                    .as_ref()
                    .map(|filter| expr_contains_aggregate(filter.as_ref()))
                    .unwrap_or(false)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => expr_contains_aggregate(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|expr| expr_contains_aggregate(expr))
                .unwrap_or(false)
                || conditions.iter().any(expr_contains_aggregate)
                || results.iter().any(expr_contains_aggregate)
                || else_result
                    .as_ref()
                    .map(|expr| expr_contains_aggregate(expr))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::Exists { .. } | Expr::Subquery(_) => false,
        _ => false,
    }
}

pub(super) fn is_aggregate_function(function: &Function) -> bool {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    matches!(
        name.as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "VARIANCE_POP"
            | "VARIANCE_SAMP"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "PERCENTILE_CONT"
            | "APPROX_QUANTILE"
            | "SUMIF"
            | "AVGIF"
            | "COUNTIF"
    )
}

pub(super) fn plan_aggregate_projection(
    items: &[SelectItem],
    column_ordinals: &HashMap<String, usize>,
    table: &str,
) -> Result<AggregateProjectionPlan, SqlExecutionError> {
    let mut outputs = Vec::with_capacity(items.len());
    let mut headers = Vec::with_capacity(items.len());
    let mut required_ordinals = BTreeSet::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(SqlExecutionError::Unsupported(
                    "aggregate SELECT does not support wildcard projections".into(),
                ));
            }
            SelectItem::UnnamedExpr(expr) => {
                let label = expr.to_string();
                let expr_clone = expr.clone();
                let mut ordinals =
                    collect_expr_column_ordinals(&expr_clone, column_ordinals, table)?;
                collect_function_order_ordinals(
                    &expr_clone,
                    column_ordinals,
                    table,
                    &mut ordinals,
                )?;
                required_ordinals.extend(ordinals.iter().copied());
                headers.push(label);
                outputs.push(AggregateProjection { expr: expr_clone });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let label = alias.value.clone();
                let expr_clone = expr.clone();
                let mut ordinals =
                    collect_expr_column_ordinals(&expr_clone, column_ordinals, table)?;
                collect_function_order_ordinals(
                    &expr_clone,
                    column_ordinals,
                    table,
                    &mut ordinals,
                )?;
                required_ordinals.extend(ordinals.iter().copied());
                headers.push(label);
                outputs.push(AggregateProjection { expr: expr_clone });
            }
        }
    }

    Ok(AggregateProjectionPlan {
        outputs,
        required_ordinals,
        headers,
    })
}

pub(super) fn collect_function_order_ordinals(
    expr: &Expr,
    column_ordinals: &HashMap<String, usize>,
    table: &str,
    out: &mut BTreeSet<usize>,
) -> Result<(), SqlExecutionError> {
    match expr {
        Expr::Function(function) => {
            for order in &function.order_by {
                let ordinals = collect_expr_column_ordinals(&order.expr, column_ordinals, table)?;
                out.extend(ordinals);
            }
            if let Some(filter) = &function.filter {
                let ordinals = collect_expr_column_ordinals(filter, column_ordinals, table)?;
                out.extend(ordinals);
                collect_function_order_ordinals(filter, column_ordinals, table, out)?;
            }
            for function_arg in &function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            collect_function_order_ordinals(inner, column_ordinals, table, out)?;
                        }
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_function_order_ordinals(left, column_ordinals, table, out)?;
            collect_function_order_ordinals(right, column_ordinals, table, out)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            collect_function_order_ordinals(low, column_ordinals, table, out)?;
            collect_function_order_ordinals(high, column_ordinals, table, out)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            for item in list {
                collect_function_order_ordinals(item, column_ordinals, table, out)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_function_order_ordinals(operand, column_ordinals, table, out)?;
            }
            for cond in conditions {
                collect_function_order_ordinals(cond, column_ordinals, table, out)?;
            }
            for res in results {
                collect_function_order_ordinals(res, column_ordinals, table, out)?;
            }
            if let Some(else_res) = else_result {
                collect_function_order_ordinals(else_res, column_ordinals, table, out)?;
            }
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            collect_function_order_ordinals(pattern, column_ordinals, table, out)?;
        }
        _ => {}
    }

    Ok(())
}

pub(super) fn evaluate_aggregate_outputs(
    plan: &AggregateProjectionPlan,
    dataset: &AggregateDataset,
) -> Result<Vec<Option<String>>, SqlExecutionError> {
    let mut row = Vec::with_capacity(plan.outputs.len());
    for output in &plan.outputs {
        let value = evaluate_scalar_expression(&output.expr, dataset)?;
        row.push(value.into_option_string());
    }
    Ok(row)
}

pub(super) fn evaluate_aggregate_function(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    match name.as_str() {
        "COUNT" => evaluate_count(function, dataset),
        "SUM" | "AVG" | "MIN" | "MAX" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" | "VARIANCE_POP"
        | "VARIANCE_SAMP" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" => {
            evaluate_numeric_aggregate(name.as_str(), function, dataset)
        }
        "PERCENTILE_CONT" => evaluate_percentile_cont(function, dataset),
        "APPROX_QUANTILE" => evaluate_approx_quantile(function, dataset),
        "SUMIF" => evaluate_sum_if(function, dataset),
        "AVGIF" => evaluate_avg_if(function, dataset),
        "COUNTIF" => evaluate_count_if(function, dataset),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported aggregate function {name}"
        ))),
    }
}

fn evaluate_count(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let filter = function.filter.as_deref();
    if function.args.is_empty()
        || matches!(
            function.args.get(0),
            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
        )
    {
        let mut count: i128 = 0;
        for &row in dataset.rows {
            if row_passes_filter(filter, row, dataset)? {
                count += 1;
            }
        }
        return Ok(ScalarValue::Int(count));
    }

    let expr = extract_single_argument(function)?;
    if function.distinct {
        let mut set: HashSet<String> = HashSet::new();
        for &row in dataset.rows {
            if !row_passes_filter(filter, row, dataset)? {
                continue;
            }
            let value = evaluate_row_expr(expr, row, dataset)?;
            if let Some(text) = value.into_option_string() {
                set.insert(text);
            }
        }
        Ok(ScalarValue::Int(set.len() as i128))
    } else {
        let mut count: i128 = 0;
        for &row in dataset.rows {
            if !row_passes_filter(filter, row, dataset)? {
                continue;
            }
            let value = evaluate_row_expr(expr, row, dataset)?;
            if !value.is_null() {
                count += 1;
            }
        }
        Ok(ScalarValue::Int(count))
    }
}

fn evaluate_numeric_aggregate(
    name: &str,
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let expr = extract_single_argument(function)?;
    let filter = function.filter.as_deref();
    let mut count: i128 = 0;
    let mut sum = 0.0;
    let mut min_value: Option<f64> = None;
    let mut max_value: Option<f64> = None;
    let mut mean = 0.0;
    let mut m2 = 0.0;

    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        let value = evaluate_row_expr(expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            min_value = Some(min_value.map(|m| m.min(num)).unwrap_or(num));
            max_value = Some(max_value.map(|m| m.max(num)).unwrap_or(num));
            count += 1;
            sum += num;

            let delta = num - mean;
            mean += delta / count as f64;
            let delta2 = num - mean;
            m2 += delta * delta2;
        }
    }

    match name {
        "SUM" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                Ok(scalar_from_f64(sum))
            }
        }
        "AVG" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                Ok(scalar_from_f64(sum / count as f64))
            }
        }
        "MIN" => Ok(min_value.map(scalar_from_f64).unwrap_or(ScalarValue::Null)),
        "MAX" => Ok(max_value.map(scalar_from_f64).unwrap_or(ScalarValue::Null)),
        "VARIANCE" | "VAR_POP" | "VARIANCE_POP" | "STDDEV" | "STDDEV_POP" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                let variance = m2 / count as f64;
                if name.starts_with("STDDEV") {
                    Ok(scalar_from_f64(variance.sqrt()))
                } else {
                    Ok(scalar_from_f64(variance))
                }
            }
        }
        "VAR_SAMP" | "VARIANCE_SAMP" | "STDDEV_SAMP" => {
            if count <= 1 {
                Ok(ScalarValue::Null)
            } else {
                let variance = m2 / (count as f64 - 1.0);
                if name.starts_with("STDDEV") {
                    Ok(scalar_from_f64(variance.sqrt()))
                } else {
                    Ok(scalar_from_f64(variance))
                }
            }
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported numeric aggregate {name}"
        ))),
    }
}

fn evaluate_percentile_cont(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont requires at least one argument".into(),
        ));
    }

    let percent_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "percentile_cont requires literal percentile argument".into(),
            ));
        }
    };

    let percent = evaluate_scalar_expression(percent_expr, dataset)?
        .as_f64()
        .ok_or_else(|| {
            SqlExecutionError::Unsupported("percentile_cont requires numeric percentile".into())
        })?;
    if !(0.0..=1.0).contains(&percent) {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont percentile must be between 0 and 1".into(),
        ));
    }

    let order_expr = if let Some(order) = function.order_by.first() {
        &order.expr
    } else if function.args.len() >= 2 {
        match &function.args[1] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => expr,
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "percentile_cont requires an expression to order".into(),
                ));
            }
        }
    } else {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont requires an ORDER BY expression".into(),
        ));
    };
    let filter = function.filter.as_deref();
    let mut values: Vec<f64> = Vec::with_capacity(dataset.rows.len());
    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        let value = evaluate_row_expr(order_expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            values.push(num);
        }
    }

    if values.is_empty() {
        return Ok(ScalarValue::Null);
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let len = values.len() as f64;
    let position = percent * (len - 1.0);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;

    if lower == upper {
        Ok(scalar_from_f64(values[lower]))
    } else {
        let lower_value = values[lower];
        let upper_value = values[upper];
        let fraction = position - lower as f64;
        Ok(scalar_from_f64(
            lower_value + (upper_value - lower_value) * fraction,
        ))
    }
}

fn evaluate_approx_quantile(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.len() < 2 {
        return Err(SqlExecutionError::Unsupported(
            "approx_quantile requires at least two arguments".into(),
        ));
    }

    let value_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "approx_quantile requires an expression argument".into(),
            ));
        }
    };

    let quantile_expr = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "approx_quantile requires numeric quantile argument".into(),
            ));
        }
    };

    let quantile = evaluate_scalar_expression(quantile_expr, dataset)?
        .as_f64()
        .ok_or_else(|| {
            SqlExecutionError::Unsupported("approx_quantile quantile must be numeric".into())
        })?;
    if !(0.0..=1.0).contains(&quantile) {
        return Err(SqlExecutionError::Unsupported(
            "approx_quantile quantile must be between 0 and 1".into(),
        ));
    }

    let filter = function.filter.as_deref();
    let mut values: Vec<f64> = Vec::with_capacity(dataset.rows.len());
    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        let value = evaluate_row_expr(value_expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            values.push(num);
        }
    }

    if values.is_empty() {
        return Ok(ScalarValue::Null);
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let position = (values.len() - 1) as f64 * quantile;
    let lower = position.floor() as usize;
    Ok(scalar_from_f64(values[lower]))
}

fn evaluate_sum_if(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.len() < 2 {
        return Err(SqlExecutionError::Unsupported(
            "sumIf requires a value and a condition".into(),
        ));
    }

    let value_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "sumIf value argument must be an expression".into(),
            ));
        }
    };

    let condition_expr = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "sumIf condition must be an expression".into(),
            ));
        }
    };

    let filter = function.filter.as_deref();
    let mut sum = 0.0;
    let mut matched = false;

    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        if !evaluate_condition(condition_expr, row, dataset)? {
            continue;
        }
        let value = evaluate_row_expr(value_expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            sum += num;
            matched = true;
        }
    }

    if matched {
        Ok(scalar_from_f64(sum))
    } else {
        Ok(ScalarValue::Null)
    }
}

fn evaluate_avg_if(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.len() < 2 {
        return Err(SqlExecutionError::Unsupported(
            "avgIf requires a value and a condition".into(),
        ));
    }

    let value_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "avgIf value argument must be an expression".into(),
            ));
        }
    };

    let condition_expr = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "avgIf condition must be an expression".into(),
            ));
        }
    };

    let filter = function.filter.as_deref();
    let mut sum = 0.0;
    let mut count = 0i128;

    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        if !evaluate_condition(condition_expr, row, dataset)? {
            continue;
        }
        let value = evaluate_row_expr(value_expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            sum += num;
            count += 1;
        }
    }

    if count == 0 {
        Ok(ScalarValue::Null)
    } else {
        Ok(scalar_from_f64(sum / count as f64))
    }
}

fn evaluate_count_if(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "countIf requires a condition expression".into(),
        ));
    }

    let condition_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "countIf condition must be an expression".into(),
            ));
        }
    };

    let filter = function.filter.as_deref();
    let mut count: i128 = 0;
    for &row in dataset.rows {
        if !row_passes_filter(filter, row, dataset)? {
            continue;
        }
        if evaluate_condition(condition_expr, row, dataset)? {
            count += 1;
        }
    }

    Ok(ScalarValue::Int(count))
}

fn row_passes_filter(
    filter: Option<&Expr>,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<bool, SqlExecutionError> {
    if let Some(expr) = filter {
        let value = evaluate_row_expr(expr, row_idx, dataset)?;
        Ok(value.as_bool().unwrap_or(false))
    } else {
        Ok(true)
    }
}

fn evaluate_condition(
    expr: &Expr,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<bool, SqlExecutionError> {
    let value = evaluate_row_expr(expr, row_idx, dataset)?;
    Ok(value.as_bool().unwrap_or(false))
}

pub(super) fn extract_single_argument<'a>(
    function: &'a Function,
) -> Result<&'a Expr, SqlExecutionError> {
    if function.args.len() != 1 {
        return Err(SqlExecutionError::Unsupported(format!(
            "function {} requires exactly one argument",
            function.name
        )));
    }
    match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => Ok(expr),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported argument for function {}",
            function.name
        ))),
    }
}
