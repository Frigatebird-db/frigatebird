use super::aggregates::{AggregateDataset, MaterializedColumns, WindowResultMap};
use super::expressions::{evaluate_row_expr, evaluate_scalar_expression};
use super::grouping_helpers::evaluate_group_key;
use super::helpers::{collect_expr_column_ordinals, parse_interval_seconds};
use super::ordering::{
    NullsPlacement, OrderClause, OrderKey, build_row_order_key, compare_order_keys,
};
use super::values::{ScalarValue, scalar_from_f64};
use super::{
    GroupKey, RangePreceding, SqlExecutionError, SumWindowFrame, WindowFunctionKind,
    WindowFunctionPlan,
};
use crate::metadata_store::ColumnCatalog;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, OrderByExpr, SelectItem, Value, WindowFrameBound,
    WindowFrameUnits, WindowType,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

pub(super) fn plan_order_clauses(
    order_by: &[OrderByExpr],
    alias_map: Option<&HashMap<String, Expr>>,
) -> Result<Vec<OrderClause>, SqlExecutionError> {
    let mut clauses = Vec::with_capacity(order_by.len());
    for clause in order_by {
        let nulls = match clause.nulls_first {
            Some(true) => NullsPlacement::First,
            Some(false) => NullsPlacement::Last,
            None => NullsPlacement::Default,
        };
        let mut expr = clause.expr.clone();
        if let Some(map) = alias_map {
            expr = rewrite_aliases(&expr, map);
        }
        clauses.push(OrderClause {
            expr,
            descending: clause.asc == Some(false),
            nulls,
        });
    }
    Ok(clauses)
}

fn rewrite_aliases(expr: &Expr, alias_map: &HashMap<String, Expr>) -> Expr {
    use sqlparser::ast::Expr::*;

    match expr {
        Identifier(ident) => alias_map
            .get(&ident.value)
            .cloned()
            .unwrap_or_else(|| Identifier(ident.clone())),
        CompoundIdentifier(idents) => {
            if idents.len() == 1 {
                if let Some(replacement) = alias_map.get(&idents[0].value) {
                    return replacement.clone();
                }
            }
            CompoundIdentifier(idents.clone())
        }
        BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_aliases(left, alias_map)),
            op: op.clone(),
            right: Box::new(rewrite_aliases(right, alias_map)),
        },
        UnaryOp { op, expr } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_aliases(expr, alias_map)),
        },
        Nested(inner) => Expr::Nested(Box::new(rewrite_aliases(inner, alias_map))),
        Function(function) => {
            let mut function = function.clone();
            for arg in &mut function.args {
                match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            *inner = rewrite_aliases(inner, alias_map);
                        }
                    }
                }
            }
            if let Some(filter) = &mut function.filter {
                *filter = Box::new(rewrite_aliases(filter, alias_map));
            }
            for order in &mut function.order_by {
                order.expr = rewrite_aliases(&order.expr, alias_map);
            }
            if let Some(WindowType::WindowSpec(spec)) = &mut function.over {
                for expr in &mut spec.partition_by {
                    *expr = rewrite_aliases(expr, alias_map);
                }
                for order in &mut spec.order_by {
                    order.expr = rewrite_aliases(&order.expr, alias_map);
                }
            }
            Expr::Function(function)
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| Box::new(rewrite_aliases(expr, alias_map))),
            conditions: conditions
                .iter()
                .map(|expr| rewrite_aliases(expr, alias_map))
                .collect(),
            results: results
                .iter()
                .map(|expr| rewrite_aliases(expr, alias_map))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|expr| Box::new(rewrite_aliases(expr, alias_map))),
        },
        _ => expr.clone(),
    }
}

pub(super) fn collect_window_function_plans(
    items: &[SelectItem],
) -> Result<Vec<WindowFunctionPlan>, SqlExecutionError> {
    let mut plans = Vec::new();
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };

        collect_window_plans_from_expr(expr, &mut plans)?;
    }
    Ok(plans)
}

pub(super) fn collect_window_plans_from_expr(
    expr: &Expr,
    plans: &mut Vec<WindowFunctionPlan>,
) -> Result<(), SqlExecutionError> {
    if let Some(plan) = extract_window_plan(expr)? {
        plans.push(plan);
    }

    use Expr::*;
    match expr {
        BinaryOp { left, right, .. } => {
            collect_window_plans_from_expr(left, plans)?;
            collect_window_plans_from_expr(right, plans)?;
        }
        UnaryOp { expr, .. } => collect_window_plans_from_expr(expr, plans)?,
        Nested(inner) => collect_window_plans_from_expr(inner, plans)?,
        Between {
            expr, low, high, ..
        } => {
            collect_window_plans_from_expr(expr, plans)?;
            collect_window_plans_from_expr(low, plans)?;
            collect_window_plans_from_expr(high, plans)?;
        }
        InList { expr, list, .. } => {
            collect_window_plans_from_expr(expr, plans)?;
            for item in list {
                collect_window_plans_from_expr(item, plans)?;
            }
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_window_plans_from_expr(op, plans)?;
            }
            for cond in conditions {
                collect_window_plans_from_expr(cond, plans)?;
            }
            for res in results {
                collect_window_plans_from_expr(res, plans)?;
            }
            if let Some(else_expr) = else_result {
                collect_window_plans_from_expr(else_expr, plans)?;
            }
        }
        Function(function) => {
            for function_arg in &function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            collect_window_plans_from_expr(inner, plans)?;
                        }
                    }
                }
            }
            if let Some(filter) = &function.filter {
                collect_window_plans_from_expr(filter, plans)?;
            }
            for order in &function.order_by {
                collect_window_plans_from_expr(&order.expr, plans)?;
            }
        }
        Cast { expr, .. }
        | SafeCast { expr, .. }
        | TryCast { expr, .. }
        | Convert { expr, .. }
        | Extract { expr, .. }
        | Collate { expr, .. }
        | Ceil { expr, .. }
        | Floor { expr, .. }
        | AtTimeZone {
            timestamp: expr, ..
        } => collect_window_plans_from_expr(expr, plans)?,
        Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            collect_window_plans_from_expr(expr, plans)?;
            if let Some(item) = trim_what {
                collect_window_plans_from_expr(item, plans)?;
            }
            if let Some(chars) = trim_characters {
                for item in chars {
                    collect_window_plans_from_expr(item, plans)?;
                }
            }
        }
        Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_window_plans_from_expr(expr, plans)?;
            if let Some(item) = substring_from {
                collect_window_plans_from_expr(item, plans)?;
            }
            if let Some(item) = substring_for {
                collect_window_plans_from_expr(item, plans)?;
            }
        }
        _ => {}
    }

    Ok(())
}

pub(super) fn parse_usize_literal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse::<usize>().ok(),
        _ => None,
    }
}

pub(super) fn extract_window_plan(
    expr: &Expr,
) -> Result<Option<WindowFunctionPlan>, SqlExecutionError> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };

    let over = match &function.over {
        Some(WindowType::WindowSpec(spec)) => spec,
        Some(WindowType::NamedWindow(_)) => {
            return Err(SqlExecutionError::Unsupported(
                "named windows are not supported yet".into(),
            ));
        }
        None => return Ok(None),
    };

    if function.filter.is_some() || function.distinct || !function.order_by.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "window functions with FILTER, DISTINCT, or inner ORDER BY are not supported yet"
                .into(),
        ));
    }

    let key = expr.to_string();
    let function_name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    match function_name.as_str() {
        "ROW_NUMBER" => {
            if !function.args.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "ROW_NUMBER() does not accept arguments".into(),
                ));
            }
            if over.window_frame.is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "ROW_NUMBER() does not support explicit window frames yet".into(),
                ));
            }
            Ok(Some(WindowFunctionPlan {
                key,
                kind: WindowFunctionKind::RowNumber,
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: None,
                default_expr: None,
            }))
        }
        "RANK" => {
            if !function.args.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "RANK() does not accept arguments".into(),
                ));
            }
            if over.order_by.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "RANK() requires ORDER BY clause".into(),
                ));
            }
            if over.window_frame.is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "RANK() does not support custom window frames yet".into(),
                ));
            }
            Ok(Some(WindowFunctionPlan {
                key,
                kind: WindowFunctionKind::Rank,
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: None,
                default_expr: None,
            }))
        }
        "DENSE_RANK" => {
            if !function.args.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "DENSE_RANK() does not accept arguments".into(),
                ));
            }
            if over.order_by.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "DENSE_RANK() requires ORDER BY clause".into(),
                ));
            }
            if over.window_frame.is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "DENSE_RANK() does not support custom window frames yet".into(),
                ));
            }
            Ok(Some(WindowFunctionPlan {
                key,
                kind: WindowFunctionKind::DenseRank,
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: None,
                default_expr: None,
            }))
        }
        "SUM" => {
            if function.args.len() != 1 {
                return Err(SqlExecutionError::Unsupported(
                    "SUM window function expects exactly one argument".into(),
                ));
            }
            let arg_expr = match &function.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr.clone(),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => expr.clone(),
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "SUM window function expects expression argument".into(),
                    ));
                }
            };

            let frame = if let Some(frame) = &over.window_frame {
                match frame.units {
                    WindowFrameUnits::Rows => {
                        let preceding;
                        match &frame.start_bound {
                            WindowFrameBound::Preceding(None) => preceding = None,
                            WindowFrameBound::Preceding(Some(expr)) => {
                                let value = parse_usize_literal(expr).ok_or_else(|| {
                                    SqlExecutionError::Unsupported(
                                        "SUM window frame PRECEDING must be a non-negative integer literal"
                                            .into(),
                                    )
                                })?;
                                preceding = Some(value as usize);
                            }
                            _ => {
                                return Err(SqlExecutionError::Unsupported(
                                    "SUM window frame must start at UNBOUNDED or N PRECEDING"
                                        .into(),
                                ));
                            }
                        }
                        if let Some(end_bound) = &frame.end_bound {
                            match end_bound {
                                WindowFrameBound::CurrentRow => {}
                                _ => {
                                    return Err(SqlExecutionError::Unsupported(
                                        "SUM window frame must end at CURRENT ROW".into(),
                                    ));
                                }
                            }
                        }
                        SumWindowFrame::Rows { preceding }
                    }
                    WindowFrameUnits::Range => {
                        if over.order_by.len() != 1 {
                            return Err(SqlExecutionError::Unsupported(
                                "SUM RANGE frame currently supports exactly one ORDER BY expression"
                                    .into(),
                            ));
                        }
                        let preceding = match &frame.start_bound {
                            WindowFrameBound::Preceding(None) => RangePreceding::Unbounded,
                            WindowFrameBound::Preceding(Some(expr)) => {
                                let seconds = parse_interval_seconds(expr, "SUM RANGE frame")?;
                                if seconds < 0.0 {
                                    return Err(SqlExecutionError::Unsupported(
                                        "SUM RANGE frame requires non-negative interval".into(),
                                    ));
                                }
                                RangePreceding::Value(seconds)
                            }
                            _ => {
                                return Err(SqlExecutionError::Unsupported(
                                    "SUM RANGE frame must start at INTERVAL PRECEDING or UNBOUNDED"
                                        .into(),
                                ));
                            }
                        };
                        if let Some(end_bound) = &frame.end_bound {
                            match end_bound {
                                WindowFrameBound::CurrentRow => {}
                                _ => {
                                    return Err(SqlExecutionError::Unsupported(
                                        "SUM RANGE frame must end at CURRENT ROW".into(),
                                    ));
                                }
                            }
                        }
                        SumWindowFrame::Range { preceding }
                    }
                    _ => {
                        return Err(SqlExecutionError::Unsupported(
                            "SUM window frame supports only ROWS or RANGE units".into(),
                        ));
                    }
                }
            } else {
                SumWindowFrame::Rows { preceding: None }
            };

            if over.order_by.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "SUM window function requires ORDER BY clause".into(),
                ));
            }

            Ok(Some(WindowFunctionPlan {
                key,
                kind: WindowFunctionKind::Sum { frame },
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: Some(arg_expr),
                default_expr: None,
            }))
        }
        "LAG" | "LEAD" => {
            if over.order_by.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "LAG/LEAD require ORDER BY clause".into(),
                ));
            }
            if over.window_frame.is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "LAG/LEAD do not support explicit window frames".into(),
                ));
            }
            if function.args.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "LAG/LEAD require at least one argument".into(),
                ));
            }

            let value_expr = match &function.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr.clone(),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => expr.clone(),
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "LAG/LEAD argument must be an expression".into(),
                    ));
                }
            };

            let offset = if function.args.len() >= 2 {
                match &function.args[1] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => parse_usize_literal(expr).ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "LAG/LEAD offset must be a non-negative integer literal".into(),
                        )
                    })?,
                    _ => {
                        return Err(SqlExecutionError::Unsupported(
                            "LAG/LEAD offset must be a literal".into(),
                        ));
                    }
                }
            } else {
                1
            };

            let default_expr = if function.args.len() >= 3 {
                match &function.args[2] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => Some(expr.clone()),
                    _ => {
                        return Err(SqlExecutionError::Unsupported(
                            "LAG/LEAD default must be an expression".into(),
                        ));
                    }
                }
            } else {
                None
            };

            if function.args.len() > 3 {
                return Err(SqlExecutionError::Unsupported(
                    "LAG/LEAD support at most three arguments".into(),
                ));
            }

            let kind = if function_name == "LAG" {
                WindowFunctionKind::Lag { offset }
            } else {
                WindowFunctionKind::Lead { offset }
            };

            Ok(Some(WindowFunctionPlan {
                key,
                kind,
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: Some(value_expr),
                default_expr,
            }))
        }
        "FIRST_VALUE" | "LAST_VALUE" => {
            if function.args.len() != 1 {
                return Err(SqlExecutionError::Unsupported(
                    "FIRST_VALUE/LAST_VALUE expect exactly one argument".into(),
                ));
            }
            if over.order_by.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "FIRST_VALUE/LAST_VALUE require ORDER BY clause".into(),
                ));
            }
            if over.window_frame.is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "FIRST_VALUE/LAST_VALUE do not support custom window frames yet".into(),
                ));
            }

            let arg_expr = match &function.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr.clone(),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => expr.clone(),
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "FIRST_VALUE/LAST_VALUE argument must be an expression".into(),
                    ));
                }
            };

            let kind = if function_name == "FIRST_VALUE" {
                WindowFunctionKind::FirstValue
            } else {
                WindowFunctionKind::LastValue
            };

            Ok(Some(WindowFunctionPlan {
                key,
                kind,
                partition_by: over.partition_by.clone(),
                order_by: over.order_by.clone(),
                arg: Some(arg_expr),
                default_expr: None,
            }))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "window function {function_name} is not supported yet"
        ))),
    }
}

pub(super) fn compute_window_results(
    plans: &[WindowFunctionPlan],
    rows: &[u64],
    materialized: &MaterializedColumns,
    column_ordinals: &HashMap<String, usize>,
) -> Result<WindowResultMap, SqlExecutionError> {
    let mut results: WindowResultMap = WindowResultMap::new();
    let mut processed: HashSet<String> = HashSet::new();
    let base_dataset = AggregateDataset {
        rows,
        materialized,
        column_ordinals,
        row_positions: None,
        window_results: None,
        masked_exprs: None,
        prefer_exact_numeric: false,
    };

    for plan in plans {
        if !processed.insert(plan.key.clone()) {
            continue;
        }

        let mut partitions: HashMap<GroupKey, Vec<usize>> = HashMap::new();
        let mut key_order: Vec<GroupKey> = Vec::new();

        for (idx, &row_idx) in rows.iter().enumerate() {
            let key = if plan.partition_by.is_empty() {
                GroupKey { values: Vec::new() }
            } else {
                evaluate_group_key(&plan.partition_by, row_idx, &base_dataset)?
            };

            match partitions.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(idx);
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(vec![idx]);
                    key_order.push(key);
                }
            }
        }

        let mut values: Vec<ScalarValue> = vec![ScalarValue::Null; rows.len()];
        let order_clauses = plan_order_clauses(&plan.order_by, None)?;

        for key in key_order {
            let indices = partitions.get(&key).expect("partition entries must exist");

            let mut sorted_positions: Vec<usize> = indices.clone();
            let mut sorted_keys: Vec<OrderKey> = Vec::new();
            if !order_clauses.is_empty() {
                let mut keyed: Vec<(OrderKey, usize)> = Vec::with_capacity(indices.len());
                for &position in indices {
                    let row_idx = rows[position];
                    let key = build_row_order_key(&order_clauses, row_idx, &base_dataset)?;
                    keyed.push((key, position));
                }
                keyed.sort_unstable_by(|left, right| {
                    compare_order_keys(&left.0, &right.0, &order_clauses)
                });
                sorted_positions = Vec::with_capacity(keyed.len());
                sorted_keys = Vec::with_capacity(keyed.len());
                for (order_key, pos) in keyed {
                    sorted_keys.push(order_key);
                    sorted_positions.push(pos);
                }
            }

            match plan.kind {
                WindowFunctionKind::RowNumber => {
                    for (rank, position) in sorted_positions.iter().enumerate() {
                        values[*position] = ScalarValue::Int((rank + 1) as i128);
                    }
                }
                WindowFunctionKind::Rank => {
                    let mut current_rank: i64 = 1;
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        if idx > 0 {
                            if compare_order_keys(
                                &sorted_keys[idx - 1],
                                &sorted_keys[idx],
                                &order_clauses,
                            ) != Ordering::Equal
                            {
                                current_rank = (idx + 1) as i64;
                            }
                        }
                        values[*position] = ScalarValue::Int(current_rank as i128);
                    }
                }
                WindowFunctionKind::DenseRank => {
                    let mut current_rank: i64 = 1;
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        if idx > 0
                            && compare_order_keys(
                                &sorted_keys[idx - 1],
                                &sorted_keys[idx],
                                &order_clauses,
                            ) != Ordering::Equal
                        {
                            current_rank += 1;
                        }
                        values[*position] = ScalarValue::Int(current_rank as i128);
                    }
                }
                WindowFunctionKind::Sum { ref frame } => {
                    let arg_expr = plan.arg.as_ref().expect("sum window must have argument");
                    let mut running_sum = 0.0;
                    let mut non_null_count: usize = 0;
                    let mut window: VecDeque<Option<f64>> = VecDeque::new();
                    match frame {
                        SumWindowFrame::Rows { preceding } => {
                            for position in sorted_positions.iter() {
                                let row_idx = rows[*position];
                                let value = evaluate_row_expr(arg_expr, row_idx, &base_dataset)?;
                                let numeric = value.as_f64();
                                if let Some(num) = numeric {
                                    running_sum += num;
                                    non_null_count += 1;
                                }
                                if let Some(limit) = preceding {
                                    window.push_back(numeric);
                                    while window.len() > limit + 1 {
                                        if let Some(front) = window.pop_front() {
                                            if let Some(num) = front {
                                                running_sum -= num;
                                                non_null_count -= 1;
                                            }
                                        }
                                    }
                                }
                                if non_null_count > 0 {
                                    values[*position] = scalar_from_f64(running_sum);
                                } else {
                                    values[*position] = ScalarValue::Null;
                                }
                            }
                        }
                        SumWindowFrame::Range { preceding } => {
                            if sorted_keys.is_empty() {
                                return Err(SqlExecutionError::Unsupported(
                                    "SUM RANGE frame requires ORDER BY clause".into(),
                                ));
                            }
                            let mut order_values: Vec<f64> = Vec::with_capacity(sorted_keys.len());
                            for key in &sorted_keys {
                                let value = key
                                    .values
                                    .get(0)
                                    .and_then(|scalar| scalar.as_f64())
                                    .ok_or_else(|| {
                                        SqlExecutionError::Unsupported(
                                            "SUM RANGE frame requires numeric ORDER BY expression"
                                                .into(),
                                        )
                                    })?;
                                order_values.push(value);
                            }

                            let span = match preceding {
                                RangePreceding::Unbounded => None,
                                RangePreceding::Value(value) => Some(*value),
                            };
                            let mut indexed_window: VecDeque<(usize, Option<f64>)> =
                                VecDeque::new();

                            for (idx, position) in sorted_positions.iter().enumerate() {
                                let row_idx = rows[*position];
                                let value = evaluate_row_expr(arg_expr, row_idx, &base_dataset)?;
                                let numeric = value.as_f64();
                                if let Some(num) = numeric {
                                    running_sum += num;
                                    non_null_count += 1;
                                }
                                indexed_window.push_back((idx, numeric));

                                if let Some(span) = span {
                                    let current_order = order_values[idx];
                                    while let Some(&(front_idx, front_value)) =
                                        indexed_window.front()
                                    {
                                        let front_order = order_values[front_idx];
                                        if current_order - front_order > span {
                                            if let Some(num) = front_value {
                                                running_sum -= num;
                                                non_null_count -= 1;
                                            }
                                            indexed_window.pop_front();
                                        } else {
                                            break;
                                        }
                                    }
                                }

                                if non_null_count > 0 {
                                    values[*position] = scalar_from_f64(running_sum);
                                } else {
                                    values[*position] = ScalarValue::Null;
                                }
                            }
                        }
                    }
                }
                WindowFunctionKind::Lag { offset } => {
                    let arg_expr = plan.arg.as_ref().expect("lag window must have argument");
                    let mut evaluated: Vec<ScalarValue> =
                        Vec::with_capacity(sorted_positions.len());
                    for position in &sorted_positions {
                        let row_idx = rows[*position];
                        evaluated.push(evaluate_row_expr(arg_expr, row_idx, &base_dataset)?);
                    }
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        let value = if idx >= offset {
                            evaluated[idx - offset].clone()
                        } else if let Some(default_expr) = &plan.default_expr {
                            evaluate_row_expr(default_expr, rows[*position], &base_dataset)?
                        } else {
                            ScalarValue::Null
                        };
                        values[*position] = value;
                    }
                }
                WindowFunctionKind::Lead { offset } => {
                    let arg_expr = plan.arg.as_ref().expect("lead window must have argument");
                    let mut evaluated: Vec<ScalarValue> =
                        Vec::with_capacity(sorted_positions.len());
                    for position in &sorted_positions {
                        let row_idx = rows[*position];
                        evaluated.push(evaluate_row_expr(arg_expr, row_idx, &base_dataset)?);
                    }
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        let value = if idx + offset < evaluated.len() {
                            evaluated[idx + offset].clone()
                        } else if let Some(default_expr) = &plan.default_expr {
                            evaluate_row_expr(default_expr, rows[*position], &base_dataset)?
                        } else {
                            ScalarValue::Null
                        };
                        values[*position] = value;
                    }
                }
                WindowFunctionKind::FirstValue => {
                    let arg_expr = plan.arg.as_ref().expect("FIRST_VALUE requires argument");
                    let mut cached: Option<ScalarValue> = None;
                    for position in sorted_positions.iter() {
                        if cached.is_none() {
                            let row_idx = rows[*position];
                            cached = Some(evaluate_row_expr(arg_expr, row_idx, &base_dataset)?);
                        }
                        values[*position] = cached.clone().unwrap_or(ScalarValue::Null);
                    }
                }
                WindowFunctionKind::LastValue => {
                    let arg_expr = plan.arg.as_ref().expect("LAST_VALUE requires argument");
                    let mut evaluated: Vec<ScalarValue> =
                        Vec::with_capacity(sorted_positions.len());
                    for position in &sorted_positions {
                        let row_idx = rows[*position];
                        evaluated.push(evaluate_row_expr(arg_expr, row_idx, &base_dataset)?);
                    }
                    let last_value = evaluated.last().cloned().unwrap_or(ScalarValue::Null);
                    for position in sorted_positions.iter() {
                        values[*position] = last_value.clone();
                    }
                }
            }
        }

        results.insert(plan.key.clone(), values);
    }

    Ok(results)
}
