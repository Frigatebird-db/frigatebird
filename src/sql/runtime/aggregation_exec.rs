use super::{SelectResult, SqlExecutionError};
use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::page_handler::PageHandler;
use crate::sql::runtime::aggregates::{
    AggregateDataset, AggregateFunctionPlan, AggregateProjectionPlan, evaluate_aggregate_outputs,
};
use crate::sql::runtime::aggregation_helpers::ensure_aggregate_plan_for_expr;
use crate::sql::runtime::batch::ColumnarBatch;
use crate::sql::runtime::executor_types::{AggregatedRow, GroupKey};
use crate::sql::runtime::executor_utils::rows_to_batch;
use crate::sql::runtime::expressions::evaluate_scalar_expression;
use crate::sql::runtime::grouping_helpers::{evaluate_group_keys_on_batch, evaluate_having};
use crate::sql::runtime::helpers::{parse_limit, parse_offset};
use crate::sql::runtime::ordering::{
    OrderClause, OrderKey, build_group_order_key, compare_order_keys,
};
use crate::sql::runtime::projection_helpers::materialize_columns;
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Offset};
use std::collections::{BTreeSet, HashMap, HashSet};

pub(crate) fn execute_grouping_set_aggregation_rows_from_batch(
    page_handler: &PageHandler,
    batch: &ColumnarBatch,
    table: &str,
    catalog: &TableCatalog,
    columns: &[ColumnCatalog],
    aggregate_plan: &AggregateProjectionPlan,
    group_exprs: &[Expr],
    required_ordinals: &BTreeSet<usize>,
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
    prefer_exact_numeric: bool,
    having: Option<&Expr>,
    qualify_expr: Option<&Expr>,
    order_clauses: &[OrderClause],
    masked_exprs: Option<&[Expr]>,
) -> Result<Vec<AggregatedRow>, SqlExecutionError> {
    let group_keys = evaluate_group_keys_on_batch(group_exprs, batch, catalog)?;

    let mut seen_group_order: HashSet<GroupKey> = HashSet::new();
    let mut group_order: Vec<GroupKey> = Vec::new();
    for key in &group_keys {
        if seen_group_order.insert(key.clone()) {
            group_order.push(key.clone());
        }
    }

    let row_ids = batch.row_ids.clone();
    let materialized =
        materialize_columns(page_handler, table, columns, required_ordinals, &row_ids)?;
    let mut group_rows: HashMap<GroupKey, Vec<u64>> = HashMap::new();
    for (idx, key) in group_keys.iter().enumerate() {
        let row_id = *row_ids
            .get(idx)
            .ok_or_else(|| SqlExecutionError::OperationFailed("missing row id".into()))?;
        group_rows.entry(key.clone()).or_default().push(row_id);
    }
    if group_exprs.is_empty() && group_order.is_empty() {
        let key = GroupKey::empty();
        group_rows.insert(key.clone(), Vec::new());
        group_order.push(key);
    }

    let mut aggregate_plans: Vec<AggregateFunctionPlan> = Vec::new();
    let mut aggregate_expr_lookup: HashMap<String, usize> = HashMap::new();
    for projection in &aggregate_plan.outputs {
        match ensure_aggregate_plan_for_expr(
            &projection.expr,
            &mut aggregate_plans,
            &mut aggregate_expr_lookup,
        ) {
            Ok(_) | Err(SqlExecutionError::Unsupported(_)) => {}
            Err(err) => return Err(err),
        };
    }

    let mut aggregated_rows: Vec<AggregatedRow> = Vec::new();
    let having_expr = having.cloned();
    for key in group_order {
        let rows = group_rows.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let dataset = AggregateDataset {
            rows,
            materialized: &materialized,
            column_ordinals,
            column_types,
            masked_exprs,
            prefer_exact_numeric,
        };

        if !evaluate_having(&having_expr, &dataset)? {
            continue;
        }
        if let Some(expr) = qualify_expr {
            let qualifies = evaluate_scalar_expression(expr, &dataset)?;
            if !qualifies.as_bool().unwrap_or(false) {
                continue;
            }
        }

        let order_key = if order_clauses.is_empty() {
            OrderKey { values: Vec::new() }
        } else {
            build_group_order_key(order_clauses, &dataset)?
        };

        let output_row = evaluate_aggregate_outputs(aggregate_plan, &dataset)?;
        aggregated_rows.push(AggregatedRow {
            order_key,
            values: output_row,
        });
    }

    Ok(aggregated_rows)
}

pub(crate) fn finalize_aggregation_rows(
    mut aggregated_rows: Vec<AggregatedRow>,
    order_clauses: &[OrderClause],
    distinct_flag: bool,
    limit_expr: Option<Expr>,
    offset_expr: Option<Offset>,
    result_columns: Vec<String>,
) -> Result<SelectResult, SqlExecutionError> {
    if distinct_flag {
        let mut seen: HashSet<Vec<Option<String>>> = HashSet::new();
        aggregated_rows.retain(|row| seen.insert(row.values.clone()));
    }

    if !order_clauses.is_empty() {
        aggregated_rows.sort_unstable_by(|left, right| {
            compare_order_keys(&left.order_key, &right.order_key, order_clauses)
        });
    }

    let offset = parse_offset(offset_expr)?;
    let limit = parse_limit(limit_expr)?;
    let start_idx = offset.min(aggregated_rows.len());
    let end_idx = if let Some(limit) = limit {
        start_idx.saturating_add(limit).min(aggregated_rows.len())
    } else {
        aggregated_rows.len()
    };

    let final_rows: Vec<Vec<Option<String>>> = aggregated_rows[start_idx..end_idx]
        .iter()
        .map(|row| row.values.clone())
        .collect();
    let batch = rows_to_batch(final_rows);
    let batches = if batch.num_rows == 0 {
        Vec::new()
    } else {
        vec![batch]
    };

    Ok(SelectResult {
        columns: result_columns,
        batches,
    })
}
