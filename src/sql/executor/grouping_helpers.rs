use super::aggregates::AggregateDataset;
use super::batch::ColumnarBatch;
use super::expressions::{
    evaluate_expression_on_batch, evaluate_row_expr, evaluate_scalar_expression,
};
use super::values::ScalarValue;
use super::{GroupByInfo, GroupKey, GroupingSetPlan, SqlExecutionError};
use crate::metadata_store::TableCatalog;
use sqlparser::ast::{Expr, GroupByExpr};

pub(super) fn validate_group_by(
    group_by: &GroupByExpr,
) -> Result<Option<GroupByInfo>, SqlExecutionError> {
    match group_by {
        GroupByExpr::All => Ok(None),
        GroupByExpr::Expressions(exprs) => {
            if exprs.is_empty() {
                return Ok(None);
            }

            let grouping_sets = expand_group_by_sets(exprs)?;
            if grouping_sets.is_empty() {
                return Ok(None);
            }

            let mut all_exprs: Vec<Expr> = Vec::new();
            for set in &grouping_sets {
                for expr in set {
                    if !all_exprs.iter().any(|existing| existing == expr) {
                        all_exprs.push(expr.clone());
                    }
                }
            }

            let mut plans: Vec<GroupingSetPlan> = Vec::with_capacity(grouping_sets.len());
            for set in grouping_sets {
                let masked = all_exprs
                    .iter()
                    .filter(|candidate| !set.iter().any(|expr| expr == *candidate))
                    .cloned()
                    .collect();
                plans.push(GroupingSetPlan {
                    expressions: set,
                    masked_exprs: masked,
                });
            }

            Ok(Some(GroupByInfo { sets: plans }))
        }
    }
}

fn expand_group_by_sets(exprs: &[Expr]) -> Result<Vec<Vec<Expr>>, SqlExecutionError> {
    let mut sets: Vec<Vec<Expr>> = vec![Vec::new()];

    for expr in exprs {
        match expr {
            Expr::Rollup(items) => {
                let rollup_sets = expand_rollup_sets(items);
                sets = combine_group_sets(&sets, &rollup_sets);
            }
            Expr::GroupingSets(grouping_sets) => {
                sets = combine_group_sets(&sets, grouping_sets);
            }
            Expr::Cube(_) => {
                return Err(SqlExecutionError::Unsupported(
                    "CUBE grouping is not supported yet".into(),
                ));
            }
            other => {
                let addition = vec![vec![other.clone()]];
                sets = combine_group_sets(&sets, &addition);
            }
        }
    }

    Ok(sets)
}

fn combine_group_sets(current: &[Vec<Expr>], additions: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    let mut result = Vec::with_capacity(current.len() * additions.len().max(1));
    for base in current {
        if additions.is_empty() {
            result.push(base.clone());
            continue;
        }
        for addition in additions {
            let mut combined = base.clone();
            combined.extend(addition.iter().cloned());
            result.push(combined);
        }
    }
    if result.is_empty() {
        current.to_vec()
    } else {
        result
    }
}

fn expand_rollup_sets(groups: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    if groups.is_empty() {
        return vec![Vec::new()];
    }

    let mut cumulative: Vec<Vec<Expr>> = Vec::with_capacity(groups.len());
    let mut running: Vec<Expr> = Vec::new();
    for group in groups {
        running.extend(group.iter().cloned());
        cumulative.push(running.clone());
    }

    let mut result = Vec::with_capacity(groups.len() + 1);
    for idx in (0..cumulative.len()).rev() {
        result.push(cumulative[idx].clone());
    }
    result.push(Vec::new());
    result
}

pub(super) fn evaluate_group_key(
    expressions: &[Expr],
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<GroupKey, SqlExecutionError> {
    let mut values = Vec::with_capacity(expressions.len());
    for expr in expressions {
        let scalar = evaluate_row_expr(expr, row_idx, dataset)?;
        values.push(scalar.into_option_string());
    }
    Ok(GroupKey { values })
}

pub(super) fn evaluate_having(
    having: &Option<Expr>,
    dataset: &AggregateDataset,
) -> Result<bool, SqlExecutionError> {
    if let Some(expr) = having {
        let scalar = evaluate_scalar_expression(expr, dataset)?;
        Ok(scalar.as_bool().unwrap_or(false))
    } else {
        Ok(true)
    }
}

pub(super) fn evaluate_group_keys_on_batch(
    group_by_exprs: &[Expr],
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<Vec<GroupKey>, SqlExecutionError> {
    if batch.num_rows == 0 {
        return Ok(Vec::new());
    }

    if group_by_exprs.is_empty() {
        return Ok(vec![GroupKey::empty(); batch.num_rows]);
    }

    let mut key_columns = Vec::with_capacity(group_by_exprs.len());
    for expr in group_by_exprs {
        key_columns.push(evaluate_expression_on_batch(expr, batch, catalog)?);
    }

    let mut keys = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut values = Vec::with_capacity(key_columns.len());
        for column in &key_columns {
            values.push(column.get_value_as_string(row_idx));
        }
        keys.push(GroupKey::from_values(values));
    }
    Ok(keys)
}
