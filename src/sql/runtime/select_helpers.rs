use super::SqlExecutionError;
use super::aggregates::AggregateProjectionPlan;
use super::executor_types::{ProjectionItem, ProjectionPlan};
use super::helpers::column_name_from_expr;
use super::ordering::OrderClause;
use crate::metadata_store::ColumnCatalog;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, OrderByExpr, Value, WindowType,
};
use std::collections::HashMap;

pub(crate) fn build_projection_alias_map(
    plan: &ProjectionPlan,
    columns: &[ColumnCatalog],
) -> HashMap<String, Expr> {
    let mut map = HashMap::new();
    for (idx, header) in plan.headers.iter().enumerate() {
        match plan
            .items
            .get(idx)
            .expect("projection items and headers must align")
        {
            ProjectionItem::Direct { ordinal } => {
                if let Some(column) = columns.get(*ordinal) {
                    if header != &column.name {
                        map.insert(
                            header.clone(),
                            Expr::Identifier(Ident::new(column.name.clone())),
                        );
                    }
                }
            }
            ProjectionItem::Computed { expr } => {
                map.insert(header.clone(), expr.clone());
            }
        }
    }
    map
}

pub(crate) fn build_aggregate_alias_map(plan: &AggregateProjectionPlan) -> HashMap<String, Expr> {
    let mut map = HashMap::new();
    for (label, output) in plan.headers.iter().zip(plan.outputs.iter()) {
        map.insert(label.clone(), output.expr.clone());
    }
    map
}

pub(crate) fn projection_expressions_from_plan(
    plan: &ProjectionPlan,
    columns: &[ColumnCatalog],
) -> Vec<Expr> {
    plan.items
        .iter()
        .map(|item| match item {
            ProjectionItem::Direct { ordinal } => {
                let column = columns
                    .get(*ordinal)
                    .expect("projection direct ordinal must reference a column");
                Expr::Identifier(Ident::new(column.name.clone()))
            }
            ProjectionItem::Computed { expr } => expr.clone(),
        })
        .collect()
}

pub(crate) fn resolve_group_by_exprs(
    group_by: &GroupByExpr,
    projection_exprs: &[Expr],
) -> Result<GroupByExpr, SqlExecutionError> {
    match group_by {
        GroupByExpr::All => Ok(GroupByExpr::All),
        GroupByExpr::Expressions(exprs) => {
            if exprs.is_empty() {
                return Ok(GroupByExpr::Expressions(Vec::new()));
            }
            let mut resolved = Vec::with_capacity(exprs.len());
            for expr in exprs {
                resolved.push(resolve_projection_reference(expr, projection_exprs)?);
            }
            Ok(GroupByExpr::Expressions(resolved))
        }
    }
}

pub(crate) fn determine_group_by_strategy(
    group_by: &GroupByExpr,
    sort_columns: &[ColumnCatalog],
    order_clauses: &[OrderClause],
) -> Result<GroupByStrategy, SqlExecutionError> {
    let group_columns = match group_by {
        GroupByExpr::All => return Ok(GroupByStrategy::SortPrefix),
        GroupByExpr::Expressions(exprs) => {
            if exprs.is_empty() {
                return Ok(GroupByStrategy::SortPrefix);
            }
            let mut names = Vec::with_capacity(exprs.len());
            for expr in exprs {
                if let Some(name) = column_name_from_expr(expr) {
                    names.push(name);
                } else {
                    return Ok(GroupByStrategy::Hash);
                }
            }
            names
        }
    };

    let matches_sort_prefix = group_columns.iter().enumerate().all(|(idx, name)| {
        sort_columns
            .get(idx)
            .map(|column| column.name == *name)
            .unwrap_or(false)
    });
    if matches_sort_prefix {
        return Ok(GroupByStrategy::SortPrefix);
    }

    let order_matches = group_columns.len() <= order_clauses.len()
        && group_columns.iter().enumerate().all(|(idx, name)| {
            order_clauses
                .get(idx)
                .and_then(|clause| column_name_from_expr(&clause.expr))
                .map(|order_name| order_name == *name)
                .unwrap_or(false)
        });

    if order_matches {
        return Ok(GroupByStrategy::OrderAligned);
    }

    Ok(GroupByStrategy::Hash)
}

pub(crate) enum GroupByStrategy {
    SortPrefix,
    OrderAligned,
    Hash,
}

impl GroupByStrategy {
    pub(crate) fn prefer_exact_numeric(&self) -> bool {
        matches!(self, GroupByStrategy::OrderAligned)
    }
}

pub(crate) fn resolve_order_by_exprs(
    clauses: &[OrderByExpr],
    projection_exprs: &[Expr],
) -> Result<Vec<OrderByExpr>, SqlExecutionError> {
    let mut resolved = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let mut rewritten = clause.clone();
        rewritten.expr = resolve_projection_reference(&clause.expr, projection_exprs)?;
        resolved.push(rewritten);
    }
    Ok(resolved)
}

fn resolve_projection_reference(
    expr: &Expr,
    projection_exprs: &[Expr],
) -> Result<Expr, SqlExecutionError> {
    match expr {
        Expr::Value(Value::Number(value, _)) => {
            let position = value.parse::<usize>().map_err(|_| {
                SqlExecutionError::Unsupported(format!(
                    "invalid projection index reference `{value}`"
                ))
            })?;
            if position == 0 || position > projection_exprs.len() {
                return Err(SqlExecutionError::Unsupported(format!(
                    "projection index {position} is out of range"
                )));
            }
            Ok(projection_exprs[position - 1].clone())
        }
        _ => Ok(expr.clone()),
    }
}

pub(crate) fn rewrite_aliases_in_expr(expr: &Expr, alias_map: &HashMap<String, Expr>) -> Expr {
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
            left: Box::new(rewrite_aliases_in_expr(left, alias_map)),
            op: op.clone(),
            right: Box::new(rewrite_aliases_in_expr(right, alias_map)),
        },
        UnaryOp { op, expr } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_aliases_in_expr(expr, alias_map)),
        },
        Nested(inner) => Expr::Nested(Box::new(rewrite_aliases_in_expr(inner, alias_map))),
        Function(function) => {
            let mut function = function.clone();
            for arg in &mut function.args {
                match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            *inner = rewrite_aliases_in_expr(inner, alias_map);
                        }
                    }
                }
            }
            if let Some(filter) = &mut function.filter {
                *filter = Box::new(rewrite_aliases_in_expr(filter, alias_map));
            }
            for order in &mut function.order_by {
                order.expr = rewrite_aliases_in_expr(&order.expr, alias_map);
            }
            if let Some(WindowType::WindowSpec(spec)) = &mut function.over {
                for expr in &mut spec.partition_by {
                    *expr = rewrite_aliases_in_expr(expr, alias_map);
                }
                for order in &mut spec.order_by {
                    order.expr = rewrite_aliases_in_expr(&order.expr, alias_map);
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
                .map(|expr| Box::new(rewrite_aliases_in_expr(expr, alias_map))),
            conditions: conditions
                .iter()
                .map(|expr| rewrite_aliases_in_expr(expr, alias_map))
                .collect(),
            results: results
                .iter()
                .map(|expr| rewrite_aliases_in_expr(expr, alias_map))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|expr| Box::new(rewrite_aliases_in_expr(expr, alias_map))),
        },
        _ => expr.clone(),
    }
}
