use super::aggregates::AggregateFunctionPlan;
use super::SqlExecutionError;
use sqlparser::ast::{Expr, Value};
use std::collections::HashMap;

pub(crate) fn find_group_expr_index(expr: &Expr, group_exprs: &[Expr]) -> Option<usize> {
    group_exprs.iter().position(|candidate| candidate == expr)
}

pub(crate) fn literal_value(expr: &Expr) -> Option<Option<String>> {
    match expr {
        Expr::Value(Value::Null) => Some(None),
        Expr::Value(Value::Boolean(flag)) => {
            Some(Some(if *flag { "true".into() } else { "false".into() }))
        }
        Expr::Value(Value::Number(value, _)) => Some(Some(value.clone())),
        Expr::Value(Value::SingleQuotedString(text)) => Some(Some(text.clone())),
        _ => None,
    }
}

pub(crate) fn ensure_aggregate_plan_for_expr(
    expr: &Expr,
    aggregate_plans: &mut Vec<AggregateFunctionPlan>,
    aggregate_lookup: &mut HashMap<String, usize>,
) -> Result<Option<usize>, SqlExecutionError> {
    let key = expr.to_string();
    if let Some(&idx) = aggregate_lookup.get(&key) {
        return Ok(Some(idx));
    }
    if let Some(plan) = AggregateFunctionPlan::from_expr(expr)? {
        let idx = aggregate_plans.len();
        aggregate_plans.push(plan);
        aggregate_lookup.insert(key, idx);
        Ok(Some(idx))
    } else {
        Ok(None)
    }
}
