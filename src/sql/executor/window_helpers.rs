use super::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use super::expressions::evaluate_expression_on_batch;
use super::grouping_helpers::evaluate_group_keys_on_batch;
use super::helpers::parse_interval_seconds;
use super::ordering::{
    NullsPlacement, OrderClause, OrderKey, build_order_keys_on_batch, compare_order_keys,
};
use super::values::{ScalarValue, scalar_from_f64};
use super::{
    GroupKey, RangePreceding, SqlExecutionError, SumWindowFrame, WindowFunctionKind,
    WindowFunctionPlan,
};
use crate::metadata_store::TableCatalog;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, OrderByExpr, SelectItem, Value, WindowFrameBound,
    WindowFrameUnits, WindowType,
};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
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
                result_ordinal: 0,
                result_alias: String::new(),
                display_alias: None,
            }))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "window function {function_name} is not supported yet"
        ))),
    }
}

pub(super) fn rewrite_window_expressions(
    expr: &mut Expr,
    alias_map: &HashMap<String, String>,
) -> Result<(), SqlExecutionError> {
    use sqlparser::ast::Expr::*;

    if alias_map.is_empty() {
        return Ok(());
    }

    match expr {
        Function(function) if function.over.is_some() => {
            let key = function.to_string();
            let alias = alias_map.get(&key).ok_or_else(|| {
                SqlExecutionError::OperationFailed(format!(
                    "missing window column for expression {key}"
                ))
            })?;
            *expr = Identifier(Ident::new(alias.clone()));
            Ok(())
        }
        BinaryOp { left, right, .. } => {
            rewrite_window_expressions(left, alias_map)?;
            rewrite_window_expressions(right, alias_map)
        }
        UnaryOp { expr, .. } => rewrite_window_expressions(expr, alias_map),
        Nested(inner) => rewrite_window_expressions(inner, alias_map),
        Between {
            expr, low, high, ..
        } => {
            rewrite_window_expressions(expr, alias_map)?;
            rewrite_window_expressions(low, alias_map)?;
            rewrite_window_expressions(high, alias_map)
        }
        InList { expr, list, .. } => {
            rewrite_window_expressions(expr, alias_map)?;
            for item in list {
                rewrite_window_expressions(item, alias_map)?;
            }
            Ok(())
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                rewrite_window_expressions(op, alias_map)?;
            }
            for cond in conditions {
                rewrite_window_expressions(cond, alias_map)?;
            }
            for res in results {
                rewrite_window_expressions(res, alias_map)?;
            }
            if let Some(else_expr) = else_result {
                rewrite_window_expressions(else_expr, alias_map)?;
            }
            Ok(())
        }
        Function(function) => {
            for function_arg in &mut function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            rewrite_window_expressions(inner, alias_map)?;
                        }
                    }
                }
            }
            if let Some(filter) = &mut function.filter {
                rewrite_window_expressions(filter, alias_map)?;
            }
            for order in &mut function.order_by {
                rewrite_window_expressions(&mut order.expr, alias_map)?;
            }
            if let Some(WindowType::WindowSpec(spec)) = &mut function.over {
                for expr in &mut spec.partition_by {
                    rewrite_window_expressions(expr, alias_map)?;
                }
                for order in &mut spec.order_by {
                    rewrite_window_expressions(&mut order.expr, alias_map)?;
                }
            }
            Ok(())
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
        } => rewrite_window_expressions(expr, alias_map),
        Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            rewrite_window_expressions(expr, alias_map)?;
            if let Some(item) = trim_what {
                rewrite_window_expressions(item, alias_map)?;
            }
            if let Some(chars) = trim_characters {
                for item in chars {
                    rewrite_window_expressions(item, alias_map)?;
                }
            }
            Ok(())
        }
        Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            rewrite_window_expressions(expr, alias_map)?;
            if let Some(item) = substring_from {
                rewrite_window_expressions(item, alias_map)?;
            }
            if let Some(item) = substring_for {
                rewrite_window_expressions(item, alias_map)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn ensure_common_partition(
    plans: &[WindowFunctionPlan],
) -> Result<Vec<Expr>, SqlExecutionError> {
    if plans.is_empty() {
        return Ok(Vec::new());
    }

    let reference = plans[0].partition_by.clone();
    for plan in plans.iter().skip(1) {
        if plan.partition_by != reference {
            return Err(SqlExecutionError::Unsupported(
                "window functions with different PARTITION BY clauses are not supported in vectorized mode".into(),
            ));
        }
    }

    Ok(reference)
}

pub(super) struct WindowOperator<'a, I>
where
    I: Iterator<Item = ColumnarBatch>,
{
    input: I,
    catalog: &'a TableCatalog,
    window_plans: Vec<WindowFunctionPlan>,
    partition_exprs: Vec<Expr>,
    partition_buffer: ColumnarBatch,
    current_partition_key: Option<GroupKey>,
    pending_partitions: VecDeque<ColumnarBatch>,
}

impl<'a, I> WindowOperator<'a, I>
where
    I: Iterator<Item = ColumnarBatch>,
{
    pub fn new(
        input: I,
        window_plans: Vec<WindowFunctionPlan>,
        partition_exprs: Vec<Expr>,
        catalog: &'a TableCatalog,
    ) -> Self {
        Self {
            input,
            catalog,
            window_plans,
            partition_exprs,
            partition_buffer: ColumnarBatch::new(),
            current_partition_key: None,
            pending_partitions: VecDeque::new(),
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        if let Some(batch) = self.pending_partitions.pop_front() {
            return Ok(Some(batch));
        }

        while let Some(batch) = self.input.next() {
            self.consume_batch(batch)?;
            if let Some(output) = self.pending_partitions.pop_front() {
                return Ok(Some(output));
            }
        }

        if self.partition_buffer.num_rows > 0 {
            let partition = std::mem::take(&mut self.partition_buffer);
            self.current_partition_key = None;
            let processed = self.process_partition(partition)?;
            return Ok(Some(processed));
        }

        Ok(None)
    }

    fn consume_batch(&mut self, batch: ColumnarBatch) -> Result<(), SqlExecutionError> {
        if batch.num_rows == 0 {
            return Ok(());
        }

        let keys = if self.partition_exprs.is_empty() {
            vec![GroupKey::empty(); batch.num_rows]
        } else {
            evaluate_group_keys_on_batch(&self.partition_exprs, &batch, self.catalog)?
        };

        let mut start = 0;
        while start < batch.num_rows {
            let current_key = keys[start].clone();
            let mut end = start + 1;
            while end < batch.num_rows && keys[end] == current_key {
                end += 1;
            }
            let chunk = batch.slice(start, end);
            self.push_chunk(current_key, chunk)?;
            start = end;
        }
        Ok(())
    }

    fn push_chunk(&mut self, key: GroupKey, chunk: ColumnarBatch) -> Result<(), SqlExecutionError> {
        match &self.current_partition_key {
            None => {
                self.current_partition_key = Some(key);
                self.partition_buffer = chunk;
            }
            Some(current) if *current == key => {
                self.partition_buffer.append(&chunk);
            }
            Some(_) => {
                let completed = std::mem::replace(&mut self.partition_buffer, chunk);
                let processed = self.process_partition(completed)?;
                self.pending_partitions.push_back(processed);
                self.current_partition_key = Some(key);
            }
        }
        Ok(())
    }

    fn process_partition(
        &self,
        mut batch: ColumnarBatch,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        if batch.num_rows == 0 {
            return Ok(batch);
        }

        for plan in &self.window_plans {
            let order_clauses = plan_order_clauses(&plan.order_by, None)?;
            let (sorted_positions, sorted_keys) =
                sorted_positions_for_plan(&batch, &order_clauses, self.catalog)?;
            let mut values = vec![ScalarValue::Null; batch.num_rows];

            match &plan.kind {
                WindowFunctionKind::RowNumber => {
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        values[*position] = ScalarValue::Int64((idx + 1) as i64);
                    }
                    let column = scalars_to_column(values, WindowResultType::Int)?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::Rank => {
                    let mut current_rank: i64 = 1;
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        if idx > 0
                            && !order_clauses.is_empty()
                            && compare_order_keys(
                                &sorted_keys[idx - 1],
                                &sorted_keys[idx],
                                &order_clauses,
                            ) != Ordering::Equal
                        {
                            current_rank = (idx + 1) as i64;
                        }
                        values[*position] = ScalarValue::Int64(current_rank as i64);
                    }
                    let column = scalars_to_column(values, WindowResultType::Int)?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::DenseRank => {
                    let mut current_rank: i64 = 1;
                    for (idx, position) in sorted_positions.iter().enumerate() {
                        if idx > 0
                            && !order_clauses.is_empty()
                            && compare_order_keys(
                                &sorted_keys[idx - 1],
                                &sorted_keys[idx],
                                &order_clauses,
                            ) != Ordering::Equal
                        {
                            current_rank += 1;
                        }
                        values[*position] = ScalarValue::Int64(current_rank as i64);
                    }
                    let column = scalars_to_column(values, WindowResultType::Int)?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::Sum { frame } => {
                    let arg_expr = plan.arg.as_ref().expect("SUM requires argument");
                    let arg_page = evaluate_expression_on_batch(arg_expr, &batch, self.catalog)?;
                    let numeric_values = scalar_column_to_numeric(&arg_page)?;

                    match frame {
                        SumWindowFrame::Rows { preceding } => {
                            for (idx, position) in sorted_positions.iter().enumerate() {
                                let start = preceding
                                    .map(|limit| idx.saturating_sub(limit))
                                    .unwrap_or(0);
                                let mut running_sum = 0.0;
                                let mut non_null = 0;
                                for candidate_idx in start..=idx {
                                    if let Some(value) =
                                        numeric_values[sorted_positions[candidate_idx]]
                                    {
                                        running_sum += value;
                                        non_null += 1;
                                    }
                                }
                                if non_null > 0 {
                                    values[*position] = scalar_from_f64(running_sum);
                                }
                            }
                        }
                        SumWindowFrame::Range { preceding } => {
                            if order_clauses.is_empty() {
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
                                            "SUM RANGE frame requires numeric ORDER BY".into(),
                                        )
                                    })?;
                                order_values.push(value);
                            }

                            let span = match preceding {
                                RangePreceding::Unbounded => None,
                                RangePreceding::Value(value) => Some(*value),
                            };
                            let mut running_sum = 0.0;
                            let mut non_null_count = 0;
                            let mut indexed_window: VecDeque<(usize, Option<f64>)> =
                                VecDeque::new();

                            for (idx, position) in sorted_positions.iter().enumerate() {
                                let numeric = numeric_values[*position];
                                if let Some(value) = numeric {
                                    running_sum += value;
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
                                            if let Some(value) = front_value {
                                                running_sum -= value;
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
                                }
                            }
                        }
                    }

                    let column = scalars_to_column(values, WindowResultType::Float)?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::Lag { offset } => {
                    let arg_expr = plan.arg.as_ref().expect("LAG requires argument");
                    let arg_page = evaluate_expression_on_batch(arg_expr, &batch, self.catalog)?;
                    let arg_values = columnar_page_to_scalars(&arg_page);
                    let default_values = if let Some(default_expr) = &plan.default_expr {
                        let default_page =
                            evaluate_expression_on_batch(default_expr, &batch, self.catalog)?;
                        Some(columnar_page_to_scalars(&default_page))
                    } else {
                        None
                    };

                    for (idx, position) in sorted_positions.iter().enumerate() {
                        let value = if idx >= *offset {
                            arg_values[sorted_positions[idx - offset]].clone()
                        } else if let Some(defaults) = &default_values {
                            defaults[*position].clone()
                        } else {
                            ScalarValue::Null
                        };
                        values[*position] = value;
                    }
                    let column = scalars_to_column(values, WindowResultType::Template(&arg_page))?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::Lead { offset } => {
                    let arg_expr = plan.arg.as_ref().expect("LEAD requires argument");
                    let arg_page = evaluate_expression_on_batch(arg_expr, &batch, self.catalog)?;
                    let arg_values = columnar_page_to_scalars(&arg_page);
                    let default_values = if let Some(default_expr) = &plan.default_expr {
                        let default_page =
                            evaluate_expression_on_batch(default_expr, &batch, self.catalog)?;
                        Some(columnar_page_to_scalars(&default_page))
                    } else {
                        None
                    };

                    for (idx, position) in sorted_positions.iter().enumerate() {
                        let value = if idx + offset < sorted_positions.len() {
                            arg_values[sorted_positions[idx + offset]].clone()
                        } else if let Some(defaults) = &default_values {
                            defaults[*position].clone()
                        } else {
                            ScalarValue::Null
                        };
                        values[*position] = value;
                    }
                    let column = scalars_to_column(values, WindowResultType::Template(&arg_page))?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::FirstValue => {
                    let arg_expr = plan.arg.as_ref().expect("FIRST_VALUE requires argument");
                    let arg_page = evaluate_expression_on_batch(arg_expr, &batch, self.catalog)?;
                    let arg_values = columnar_page_to_scalars(&arg_page);
                    let first_idx = sorted_positions.first().copied().unwrap_or(0);
                    let first_value = arg_values
                        .get(first_idx)
                        .cloned()
                        .unwrap_or(ScalarValue::Null);
                    for position in sorted_positions.iter() {
                        values[*position] = first_value.clone();
                    }
                    let column = scalars_to_column(values, WindowResultType::Template(&arg_page))?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
                WindowFunctionKind::LastValue => {
                    let arg_expr = plan.arg.as_ref().expect("LAST_VALUE requires argument");
                    let arg_page = evaluate_expression_on_batch(arg_expr, &batch, self.catalog)?;
                    let arg_values = columnar_page_to_scalars(&arg_page);
                    let last_idx = sorted_positions.last().copied().unwrap_or(0);
                    let last_value = arg_values
                        .get(last_idx)
                        .cloned()
                        .unwrap_or(ScalarValue::Null);
                    for position in sorted_positions.iter() {
                        values[*position] = last_value.clone();
                    }
                    let column = scalars_to_column(values, WindowResultType::Template(&arg_page))?;
                    batch.columns.insert(plan.result_ordinal, column);
                }
            }

            batch
                .aliases
                .insert(plan.result_alias.clone(), plan.result_ordinal);
            if let Some(alias) = &plan.display_alias {
                batch.aliases.insert(alias.clone(), plan.result_ordinal);
            }
        }

        Ok(batch)
    }
}

enum WindowResultType<'a> {
    Int,
    Float,
    Template(&'a ColumnarPage),
}

enum TemplateOutputKind {
    Int,
    Float,
    Text,
}

fn sorted_positions_for_plan(
    batch: &ColumnarBatch,
    clauses: &[OrderClause],
    catalog: &TableCatalog,
) -> Result<(Vec<usize>, Vec<OrderKey>), SqlExecutionError> {
    let mut positions: Vec<usize> = (0..batch.num_rows).collect();
    if clauses.is_empty() {
        return Ok((positions, Vec::new()));
    }

    let keys = build_order_keys_on_batch(clauses, batch, catalog)?;
    positions.sort_by(|left, right| {
        let ordering = compare_order_keys(&keys[*left], &keys[*right], clauses);
        if ordering == Ordering::Equal {
            left.cmp(right)
        } else {
            ordering
        }
    });
    let mut sorted_keys = Vec::with_capacity(batch.num_rows);
    for &pos in &positions {
        sorted_keys.push(keys[pos].clone());
    }
    Ok((positions, sorted_keys))
}

fn column_scalar_value(page: &ColumnarPage, idx: usize) -> ScalarValue {
    if page.null_bitmap.is_set(idx) {
        return ScalarValue::Null;
    }
    match &page.data {
        ColumnData::Int64(values) => ScalarValue::Int64(values[idx]),
        ColumnData::Float64(values) => ScalarValue::Float64(values[idx]),
        // Legacy bridge: allocates String for compatibility
        ColumnData::Text(col) => ScalarValue::String(col.get_string(idx)),
        ColumnData::Boolean(values) => ScalarValue::Boolean(values[idx]),
        ColumnData::Timestamp(values) => ScalarValue::Timestamp(values[idx]),
        // Legacy bridge: allocates String for compatibility
        ColumnData::Dictionary(dict) => ScalarValue::String(dict.get_string(idx)),
    }
}

fn scalar_column_to_numeric(page: &ColumnarPage) -> Result<Vec<Option<f64>>, SqlExecutionError> {
    let mut values = Vec::with_capacity(page.len());
    match &page.data {
        ColumnData::Int64(ints) => {
            for (idx, value) in ints.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    values.push(None);
                } else {
                    values.push(Some(*value as f64));
                }
            }
        }
        ColumnData::Float64(floats) => {
            for (idx, value) in floats.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    values.push(None);
                } else {
                    values.push(Some(*value));
                }
            }
        }
        ColumnData::Text(col) => {
            for idx in 0..col.len() {
                if page.null_bitmap.is_set(idx) {
                    values.push(None);
                } else {
                    let s = col.get_string(idx);
                    let parsed = s.parse::<f64>().map_err(|_| {
                        SqlExecutionError::Unsupported(
                            "SUM window requires numeric argument".into(),
                        )
                    })?;
                    values.push(Some(parsed));
                }
            }
        }
        ColumnData::Dictionary(dict) => {
            for idx in 0..dict.len() {
                if page.null_bitmap.is_set(idx) {
                    values.push(None);
                } else {
                    let s = dict.get_string(idx);
                    let parsed = s.parse::<f64>().map_err(|_| {
                        SqlExecutionError::Unsupported(
                            "SUM window requires numeric argument".into(),
                        )
                    })?;
                    values.push(Some(parsed));
                }
            }
        }
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "SUM window requires numeric argument".into(),
            ));
        }
    }
    Ok(values)
}

fn columnar_page_to_scalars(page: &ColumnarPage) -> Vec<ScalarValue> {
    let mut values = Vec::with_capacity(page.len());
    for idx in 0..page.len() {
        values.push(column_scalar_value(page, idx));
    }
    values
}

fn scalars_to_column(
    values: Vec<ScalarValue>,
    kind: WindowResultType<'_>,
) -> Result<ColumnarPage, SqlExecutionError> {
    let len = values.len();
    let mut null_bitmap = Bitmap::new(len);
    match kind {
        WindowResultType::Int => {
            let mut data = Vec::with_capacity(len);
            for (idx, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::Null => {
                        null_bitmap.set(idx);
                        data.push(0);
                    }
                    ScalarValue::Int64(int_value) => data.push(*int_value as i64),
                    _ => {
                        return Err(SqlExecutionError::Unsupported(
                            "expected integer window result".into(),
                        ));
                    }
                }
            }
            Ok(ColumnarPage {
                page_metadata: String::new(),
                data: ColumnData::Int64(data),
                null_bitmap,
                num_rows: len,
            })
        }
        WindowResultType::Float => {
            let mut data = Vec::with_capacity(len);
            for (idx, value) in values.iter().enumerate() {
                match value {
                    ScalarValue::Null => {
                        null_bitmap.set(idx);
                        data.push(0.0);
                    }
                    ScalarValue::Float64(float_value) => data.push(*float_value),
                    ScalarValue::Int64(int_value) => data.push(*int_value as f64),
                    _ => {
                        return Err(SqlExecutionError::Unsupported(
                            "expected numeric window result".into(),
                        ));
                    }
                }
            }
            Ok(ColumnarPage {
                page_metadata: String::new(),
                data: ColumnData::Float64(data),
                null_bitmap,
                num_rows: len,
            })
        }
        WindowResultType::Template(template) => match &template.data {
            ColumnData::Int64(_)
            | ColumnData::Float64(_)
            | ColumnData::Text(_)
            | ColumnData::Boolean(_)
            | ColumnData::Timestamp(_)
            | ColumnData::Dictionary(_) => {
                let target_kind = determine_template_kind(template, &values);
                match target_kind {
                    TemplateOutputKind::Int => {
                        let mut data = Vec::with_capacity(len);
                        for (idx, value) in values.iter().enumerate() {
                            match value {
                                ScalarValue::Null => {
                                    null_bitmap.set(idx);
                                    data.push(0);
                                }
                                ScalarValue::Int64(int_value) => data.push(*int_value as i64),
                                _ => {
                                    return Err(SqlExecutionError::Unsupported(
                                        "window result type mismatch".into(),
                                    ));
                                }
                            }
                        }
                        Ok(ColumnarPage {
                            page_metadata: String::new(),
                            data: ColumnData::Int64(data),
                            null_bitmap,
                            num_rows: len,
                        })
                    }
                    TemplateOutputKind::Float => {
                        let mut data = Vec::with_capacity(len);
                        for (idx, value) in values.iter().enumerate() {
                            match value {
                                ScalarValue::Null => {
                                    null_bitmap.set(idx);
                                    data.push(0.0);
                                }
                                ScalarValue::Float64(float_value) => data.push(*float_value),
                                ScalarValue::Int64(int_value) => data.push(*int_value as f64),
                                _ => {
                                    return Err(SqlExecutionError::Unsupported(
                                        "window result type mismatch".into(),
                                    ));
                                }
                            }
                        }
                        Ok(ColumnarPage {
                            page_metadata: String::new(),
                            data: ColumnData::Float64(data),
                            null_bitmap,
                            num_rows: len,
                        })
                    }
                    TemplateOutputKind::Text => {
                        let mut col = BytesColumn::with_capacity(len, len * 16);
                        for (idx, value) in values.iter().enumerate() {
                            match value {
                                ScalarValue::Null => {
                                    null_bitmap.set(idx);
                                    col.push("");
                                }
                                ScalarValue::String(text) => col.push(text),
                                _ => col.push(&scalar_to_string(value)),
                            }
                        }
                        Ok(ColumnarPage {
                            page_metadata: String::new(),
                            data: ColumnData::Text(col),
                            null_bitmap,
                            num_rows: len,
                        })
                    }
                }
            }
        },
    }
}

fn determine_template_kind(template: &ColumnarPage, values: &[ScalarValue]) -> TemplateOutputKind {
    if matches!(template.data, ColumnData::Text(_) | ColumnData::Boolean(_))
        || values
            .iter()
            .any(|value| matches!(value, ScalarValue::String(_) | ScalarValue::Boolean(_)))
    {
        return TemplateOutputKind::Text;
    }
    if matches!(template.data, ColumnData::Float64(_))
        || values
            .iter()
            .any(|value| matches!(value, ScalarValue::Float64(_)))
    {
        return TemplateOutputKind::Float;
    }
    TemplateOutputKind::Int
}

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::Float64(value) => value.to_string(),
        ScalarValue::String(text) => text.clone(),
        ScalarValue::Boolean(flag) => {
            if *flag {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        ScalarValue::Timestamp(ts) => ts.to_string(),
        ScalarValue::Null => "NULL".to_string(),
    }
}
