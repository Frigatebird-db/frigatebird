use super::aggregates::{
    AggregateDataset, AggregateFunctionPlan, AggregateProjectionPlan, evaluate_aggregate_outputs,
};
use super::expressions::evaluate_scalar_expression;
use super::grouping_helpers::{evaluate_group_keys_on_batch, evaluate_having};
use super::helpers::{parse_limit, parse_offset};
use super::ordering::{OrderClause, OrderKey, build_group_order_key, compare_order_keys};
use super::projection_helpers::materialize_columns;
use super::scan_stream::merge_stream_to_batch;
use super::{
    AggregatedRow, GroupKey, SelectResult, SqlExecutionError, SqlExecutor,
    VectorAggregationOutput, ensure_aggregate_plan_for_expr, find_group_expr_index, literal_value,
    rows_to_batch,
};
use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Offset};
use std::collections::{BTreeSet, HashMap, HashSet};

impl SqlExecutor {
    pub(super) fn execute_vectorized_aggregation(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        aggregate_plan: &AggregateProjectionPlan,
        group_exprs: &[Expr],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        prefer_exact_numeric: bool,
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        having: Option<&Expr>,
        qualify_expr: Option<&Expr>,
        order_clauses: &[OrderClause],
        distinct_flag: bool,
        row_ids: Option<Vec<u64>>,
    ) -> Result<SelectResult, SqlExecutionError> {
        let mut aggregated_rows = self.execute_grouping_set_aggregation_rows(
            table,
            catalog,
            columns,
            aggregate_plan,
            group_exprs,
            required_ordinals,
            selection_expr,
            selection_physical_expr,
            selection_applied_in_scan,
            column_ordinals,
            column_types,
            prefer_exact_numeric,
            having,
            qualify_expr,
            order_clauses,
            None,
            row_ids,
        )?;

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

    pub(super) fn execute_grouping_set_aggregation_rows(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        aggregate_plan: &AggregateProjectionPlan,
        group_exprs: &[Expr],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        prefer_exact_numeric: bool,
        having: Option<&Expr>,
        qualify_expr: Option<&Expr>,
        order_clauses: &[OrderClause],
        masked_exprs: Option<&[Expr]>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<Vec<AggregatedRow>, SqlExecutionError> {
        let stream = self.build_scan_stream(
            table,
            columns,
            required_ordinals,
            selection_physical_expr,
            column_ordinals,
            catalog.rows_per_page_group,
            row_ids,
        )?;
        let mut batch = merge_stream_to_batch(stream)?;

        if !selection_applied_in_scan {
            if let Some(expr) = selection_expr {
                batch = self.apply_filter_expr(
                    batch,
                    expr,
                    selection_physical_expr,
                    catalog,
                    table,
                    columns,
                    column_ordinals,
                    column_types,
                )?;
            }
        }

        let group_keys = evaluate_group_keys_on_batch(group_exprs, &batch, catalog)?;

        let mut seen_group_order: HashSet<GroupKey> = HashSet::new();
        let mut group_order: Vec<GroupKey> = Vec::new();
        for key in &group_keys {
            if seen_group_order.insert(key.clone()) {
                group_order.push(key.clone());
            }
        }

        let row_ids = batch.row_ids.clone();
        let materialized = materialize_columns(
            &self.page_handler,
            table,
            columns,
            required_ordinals,
            &row_ids,
        )?;
        let mut group_rows: HashMap<GroupKey, Vec<u64>> = HashMap::new();
        for (idx, key) in group_keys.iter().enumerate() {
            let row_id = *row_ids.get(idx).ok_or_else(|| {
                SqlExecutionError::OperationFailed("missing row id".into())
            })?;
            group_rows.entry(key.clone()).or_default().push(row_id);
        }
        if group_exprs.is_empty() && group_order.is_empty() {
            let key = GroupKey::empty();
            group_rows.insert(key.clone(), Vec::new());
            group_order.push(key);
        }

        let mut aggregate_plans: Vec<AggregateFunctionPlan> = Vec::new();
        let mut aggregate_expr_lookup: HashMap<String, usize> = HashMap::new();
        let mut output_kinds: Vec<VectorAggregationOutput> =
            Vec::with_capacity(aggregate_plan.outputs.len());

        for projection in &aggregate_plan.outputs {
            match ensure_aggregate_plan_for_expr(
                &projection.expr,
                &mut aggregate_plans,
                &mut aggregate_expr_lookup,
            ) {
                Ok(Some(slot_index)) => {
                    output_kinds.push(VectorAggregationOutput::Aggregate { slot_index });
                    continue;
                }
                Ok(None) => {}
                Err(SqlExecutionError::Unsupported(_)) => {
                    output_kinds.push(VectorAggregationOutput::ScalarExpr {
                        expr: projection.expr.clone(),
                    });
                    continue;
                }
                Err(err) => return Err(err),
            }

            if let Some(idx) = find_group_expr_index(&projection.expr, group_exprs) {
                output_kinds.push(VectorAggregationOutput::GroupExpr { group_index: idx });
                continue;
            }

            if let Some(value) = literal_value(&projection.expr) {
                output_kinds.push(VectorAggregationOutput::Literal { value });
                continue;
            }

            output_kinds.push(VectorAggregationOutput::ScalarExpr {
                expr: projection.expr.clone(),
            });
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
}
