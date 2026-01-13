use super::{
    OrderClause, SelectResult, SqlExecutionError, SqlExecutor, merge_batches,
    parse_limit, parse_offset,
};
use super::batch::ColumnarBatch;
use super::executor_types::ProjectionPlan;
use super::scan_stream::collect_stream_batches;
use super::window_helpers::{
    WindowFunctionPlan, assign_window_display_aliases, rewrite_projection_plan_for_windows,
    rewrite_window_expressions,
};
use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::pipeline::operators::{
    DistinctOperator, FilterOperator, LimitOperator, PipelineOperator, ProjectOperator,
    SortOperator, WindowOperator as PipelineWindowOperator,
};
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
        let mut batches = collect_stream_batches(stream)?;

        if !selection_applied_in_scan {
            if let Some(expr) = selection_expr {
                let mut filtered = Vec::new();
                for batch in batches {
                    let mut filter = FilterOperator::new(
                        self,
                        expr,
                        selection_physical_expr,
                        catalog,
                        table,
                        columns,
                        column_ordinals,
                        column_types,
                    );
                    let results = filter.execute(batch)?;
                    for result in results {
                        if result.num_rows > 0 {
                            filtered.push(result);
                        }
                    }
                }
                batches = filtered;
            }
        }

        if batches.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        let mut processed_batches = if let Some(expr) = qualify_expr {
            let mut filtered = Vec::new();
            for batch in batches {
                let mut filter = FilterOperator::new(
                    self,
                    expr,
                    None,
                    catalog,
                    table,
                    columns,
                    column_ordinals,
                    column_types,
                );
                let results = filter.execute(batch)?;
                for result in results {
                    if result.num_rows > 0 {
                        filtered.push(result);
                    }
                }
            }
            filtered
        } else {
            batches
        };

        if processed_batches.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        if !order_clauses.is_empty() {
            let mut sorter = SortOperator::new(self, order_clauses, catalog);
            processed_batches = sorter.execute_batches(processed_batches)?;
        }

        let mut project = ProjectOperator::new(self, projection_plan, catalog);
        let mut projected = Vec::new();
        for batch in processed_batches {
            let results = project.execute(batch)?;
            for result in results {
                if result.num_rows > 0 {
                    projected.push(result);
                }
            }
        }

        if projected.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        if distinct_flag {
            let mut distinct = DistinctOperator::new(projection_plan.items.len());
            let deduped = distinct.execute_batches(projected)?;
            let mut limiter = LimitOperator::new(self, offset, limit);
            let limited_batches = limiter.execute_batches(deduped)?;
            return Ok(SelectResult {
                columns: result_columns,
                batches: limited_batches,
            });
        }

        let mut limiter = LimitOperator::new(self, offset, limit);
        let limited_batches = limiter.execute_batches(projected)?;
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
        let mut batches = collect_stream_batches(stream)?;
        if batches.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        if !selection_applied_in_scan {
            if let Some(expr) = selection_expr {
                let mut filtered = Vec::new();
                for batch in batches {
                    let mut filter = FilterOperator::new(
                        self,
                        expr,
                        selection_physical_expr,
                        catalog,
                        table,
                        columns,
                        column_ordinals,
                        column_types,
                    );
                    let results = filter.execute(batch)?;
                    for result in results {
                        if result.num_rows > 0 {
                            filtered.push(result);
                        }
                    }
                }
                batches = filtered;
                if batches.is_empty() {
                    return Ok(SelectResult {
                        columns: result_columns,
                        batches: Vec::new(),
                    });
                }
            }
        }

        let mut base_batch = merge_batches(batches);
        if base_batch.num_rows == 0 {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        let mut window_operator =
            PipelineWindowOperator::new(window_plans, partition_exprs, catalog);
        let processed_batches = window_operator.execute(base_batch)?;
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
            let mut sorter = SortOperator::new(self, &final_order_clauses, catalog);
            let sorted_batches = sorter.execute(processed_batch)?;
            processed_batch = merge_batches(sorted_batches);
        }

        let mut project = ProjectOperator::new(self, &projection_plan, catalog);
        let mut results = project.execute(processed_batch)?;
        let final_batch = results.pop().unwrap_or_else(ColumnarBatch::new);
        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        if distinct_flag {
            let mut distinct = DistinctOperator::new(projection_plan.items.len());
            let deduped = distinct.execute(final_batch)?;
            let mut limiter = LimitOperator::new(self, offset, limit);
            let limited_batches = limiter.execute_batches(deduped)?;
            return Ok(SelectResult {
                columns: result_columns,
                batches: limited_batches,
            });
        }

        let mut limiter = LimitOperator::new(self, offset, limit);
        let limited_batches = limiter.execute_batches(vec![final_batch])?;
        Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        })
    }
}
