use super::{
    NullsPlacement, OrderClause, SelectResult, SqlExecutionError, SqlExecutor,
    chunk_batch, deduplicate_batches, merge_batches, parse_limit, parse_offset,
};
use super::executor_types::ProjectionPlan;
use super::scan_stream::merge_stream_to_batch;
use super::window_helpers::{
    WindowFunctionPlan, assign_window_display_aliases, rewrite_projection_plan_for_windows,
    rewrite_window_expressions,
};
use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Offset};
use std::collections::{BTreeSet, HashMap};

impl SqlExecutor {
    pub(super) fn execute_vectorized_projection(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        projection_plan: &ProjectionPlan,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
        qualify_expr: Option<&Expr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<SelectResult, SqlExecutionError> {
        let stream = self.build_scan_stream(
            table,
            columns,
            required_ordinals,
            selection_physical_expr,
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

        if batch.num_rows == 0 || batch.columns.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        let mut processed_batch = if let Some(expr) = qualify_expr {
            let filtered = self.apply_filter_expr(
                batch,
                expr,
                None,
                catalog,
                table,
                columns,
                column_ordinals,
                column_types,
            )?;
            if filtered.num_rows == 0 {
                return Ok(SelectResult {
                    columns: result_columns,
                    batches: Vec::new(),
                });
            }
            filtered
        } else {
            batch
        };

        if !order_clauses.is_empty() {
            let sorted_batches =
                self.execute_sort(std::iter::once(processed_batch), order_clauses, catalog)?;
            processed_batch = merge_batches(sorted_batches);
        }

        let final_batch = self.build_projection_batch(&processed_batch, projection_plan, catalog)?;

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        if distinct_flag {
            let deduped = deduplicate_batches(vec![final_batch], projection_plan.items.len());
            let limited_batches = self.apply_limit_offset(deduped.into_iter(), offset, limit)?;
            return Ok(SelectResult {
                columns: result_columns,
                batches: limited_batches,
            });
        }

        let limited_batches =
            self.apply_limit_offset(std::iter::once(final_batch), offset, limit)?;
        Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        })
    }

    pub(super) fn execute_vectorized_window_query(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        mut projection_plan: ProjectionPlan,
        mut window_plans: Vec<WindowFunctionPlan>,
        partition_exprs: Vec<Expr>,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
        mut qualify_expr: Option<Expr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<SelectResult, SqlExecutionError> {
        if projection_plan.items.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "projection required for window queries".into(),
            ));
        }

        assign_window_display_aliases(window_plans.as_mut_slice(), &projection_plan);
        let mut alias_map: HashMap<String, String> = HashMap::with_capacity(window_plans.len());
        let mut next_ordinal = columns
            .iter()
            .map(|column| column.ordinal)
            .max()
            .unwrap_or(0)
            + 1;
        for (idx, plan) in window_plans.iter_mut().enumerate() {
            let alias = format!("__window_col_{idx}");
            plan.result_ordinal = next_ordinal;
            plan.result_alias = alias.clone();
            alias_map.insert(plan.key.clone(), alias);
            next_ordinal += 1;
        }

        rewrite_projection_plan_for_windows(&mut projection_plan, &alias_map)?;
        if let Some(expr) = qualify_expr.as_mut() {
            rewrite_window_expressions(expr, &alias_map)?;
        }
        let mut final_order_clauses = order_clauses.to_vec();
        for clause in &mut final_order_clauses {
            rewrite_window_expressions(&mut clause.expr, &alias_map)?;
        }

        let stream = self.build_scan_stream(
            table,
            columns,
            required_ordinals,
            selection_physical_expr,
            row_ids,
        )?;
        let mut batch = merge_stream_to_batch(stream)?;
        if batch.num_rows == 0 {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

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
                if batch.num_rows == 0 {
                    return Ok(SelectResult {
                        columns: result_columns,
                        batches: Vec::new(),
                    });
                }
            }
        }

        if !partition_exprs.is_empty() {
            let partition_clauses: Vec<OrderClause> = partition_exprs
                .iter()
                .map(|expr| OrderClause {
                    expr: expr.clone(),
                    descending: false,
                    nulls: NullsPlacement::Default,
                })
                .collect();
            batch = super::ordering::sort_batch_in_memory(&batch, &partition_clauses, catalog)?;
        }

        let chunks = chunk_batch(&batch, super::WINDOW_BATCH_CHUNK_SIZE);
        let mut operator = super::window_helpers::WindowOperator::new(
            chunks.into_iter(),
            window_plans,
            partition_exprs,
            catalog,
        );
        let mut processed_batches = Vec::new();
        while let Some(processed) = operator.next_batch()? {
            processed_batches.push(processed);
        }
        if processed_batches.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }
        let mut processed_batch = merge_batches(processed_batches);

        if let Some(expr) = qualify_expr {
            processed_batch = self.apply_qualify_filter(processed_batch, &expr, catalog)?;
            if processed_batch.num_rows == 0 {
                return Ok(SelectResult {
                    columns: result_columns,
                    batches: Vec::new(),
                });
            }
        }

        if !final_order_clauses.is_empty() {
            let sorted_batches = self.execute_sort(
                std::iter::once(processed_batch),
                &final_order_clauses,
                catalog,
            )?;
            processed_batch = merge_batches(sorted_batches);
        }

        let final_batch =
            self.build_projection_batch(&processed_batch, &projection_plan, catalog)?;
        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        if distinct_flag {
            let deduped = deduplicate_batches(vec![final_batch], projection_plan.items.len());
            let limited_batches = self.apply_limit_offset(deduped.into_iter(), offset, limit)?;
            return Ok(SelectResult {
                columns: result_columns,
                batches: limited_batches,
            });
        }

        let limited_batches =
            self.apply_limit_offset(std::iter::once(final_batch), offset, limit)?;
        Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        })
    }
}
