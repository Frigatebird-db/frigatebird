use super::{SqlExecutionError, SqlExecutor, SelectResult};
use super::executor_types::{AggregatedRow, ProjectionPlan};
use super::select_helpers::{
    GroupByStrategy, build_aggregate_alias_map, build_projection_alias_map,
    determine_group_by_strategy, projection_expressions_from_plan, resolve_group_by_exprs,
    resolve_order_by_exprs, rewrite_aliases_in_expr,
};
use crate::metadata_store::ColumnCatalog;
use crate::sql::PlannerError;
use crate::sql::executor::aggregates::{
    AggregateProjectionPlan, plan_aggregate_projection, select_item_contains_aggregate,
};
use crate::sql::executor::grouping_helpers::validate_group_by;
use crate::sql::executor::helpers::{
    collect_expr_column_ordinals, column_name_from_expr, object_name_to_string, parse_limit,
    parse_offset,
};
use crate::sql::executor::ordering::OrderClause;
use crate::sql::executor::physical_evaluator::filter_supported;
use crate::sql::executor::projection_helpers::build_projection;
use crate::sql::executor::merge_batches;
use crate::sql::executor::scan_helpers::{collect_sort_key_filters, collect_sort_key_prefixes};
use crate::sql::executor::scan_stream::collect_stream_batches;
use crate::sql::executor::window_helpers::{
    collect_window_function_plans, collect_window_plans_from_expr, ensure_common_partition,
    plan_order_clauses,
};
use crate::pipeline::operators::{AggregateOperator, FilterOperator, PipelineOperator};
use crate::sql::planner::ExpressionPlanner;
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, GroupByExpr, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, Value};
use std::collections::{BTreeSet, HashMap, HashSet};

impl SqlExecutor {
    pub(crate) fn execute_select(&self, mut query: Query) -> Result<SelectResult, SqlExecutionError> {
        if query.with.is_some()
            || !query.limit_by.is_empty()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
        {
            return Err(SqlExecutionError::Unsupported(
                "SELECT with advanced clauses is not supported".into(),
            ));
        }

        let order_by_clauses = std::mem::take(&mut query.order_by);
        let limit_expr = query.limit.take();
        let offset_expr = query.offset.take();

        let body = *query.body;
        let select = match body {
            SetExpr::Select(select) => *select,
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "only simple SELECT statements are supported".into(),
                ));
            }
        };

        let Select {
            distinct,
            top,
            projection,
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window,
            qualify,
            value_table_mode,
        } = select;

        let distinct_flag = distinct.is_some();

        if top.is_some()
            || into.is_some()
            || !lateral_views.is_empty()
            || !cluster_by.is_empty()
            || !distribute_by.is_empty()
            || !sort_by.is_empty()
            || !named_window.is_empty()
            || value_table_mode.is_some()
        {
            return Err(SqlExecutionError::Unsupported(
                "SELECT with advanced clauses is not supported".into(),
            ));
        }

        if from.len() != 1 {
            return Err(SqlExecutionError::Unsupported(
                "SELECT supports exactly one table".into(),
            ));
        }
        let table_with_joins = &from[0];
        if !table_with_joins.joins.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "SELECT with JOINs is not supported".into(),
            ));
        }

        let (table_name, table_alias) = match &table_with_joins.relation {
            TableFactor::Table { name, alias, .. } => {
                let tname = object_name_to_string(name);
                let talias = alias.as_ref().map(|a| a.name.value.clone());
                (tname, talias)
            }
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "only direct table references supported".into(),
                ));
            }
        };

        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        let mut column_types: HashMap<String, DataType> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
            column_types.insert(column.name.clone(), column.data_type);
        }

        let projection_items = projection;
        let mut window_plans = collect_window_function_plans(&projection_items)?;
        if let Some(expr) = &qualify {
            collect_window_plans_from_expr(expr, &mut window_plans)?;
        }

        let selection_expr_opt = selection;
        let (selection_expr, has_selection) = match selection_expr_opt {
            Some(expr) => (expr, true),
            None => (Expr::Value(Value::Boolean(true)), false),
        };

        let expr_planner = ExpressionPlanner::new(&catalog);
        let physical_selection_expr = if has_selection {
            match expr_planner.plan_expression(&selection_expr) {
                Ok(expr) => Some(expr),
                Err(PlannerError::Unsupported(_)) => None,
                Err(err) => return Err(SqlExecutionError::Plan(err)),
            }
        } else {
            None
        };
        let can_use_physical_filter = physical_selection_expr
            .as_ref()
            .map_or(false, filter_supported);

        let sort_columns_refs = catalog.sort_key();
        if sort_columns_refs.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "SELECT currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_columns_refs.into_iter().cloned().collect();

        let mut key_values = Vec::with_capacity(sort_columns.len());

        let aggregate_query = projection_items
            .iter()
            .any(|item| select_item_contains_aggregate(item));

        let has_grouping = matches!(
            &group_by,
            GroupByExpr::Expressions(exprs) if !exprs.is_empty()
        );

        let needs_aggregation = aggregate_query || has_grouping || having.is_some();

        if has_grouping && !aggregate_query {
            return Err(SqlExecutionError::Unsupported(
                "GROUP BY requires aggregate projections".into(),
            ));
        }

        if needs_aggregation && !window_plans.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "window functions are not supported with aggregates or GROUP BY yet".into(),
            ));
        }

        let mut aggregate_plan_opt: Option<AggregateProjectionPlan> = None;
        let mut projection_plan_opt: Option<ProjectionPlan> = None;
        let mut required_ordinals: BTreeSet<usize>;
        let result_columns: Vec<String>;
        let mut alias_map: HashMap<String, Expr> = HashMap::new();
        let projection_exprs: Vec<Expr>;

        if needs_aggregation {
            let plan = plan_aggregate_projection(&projection_items, &column_ordinals, &table_name)?;
            alias_map = build_aggregate_alias_map(&plan);
            projection_exprs = plan
                .outputs
                .iter()
                .map(|output| output.expr.clone())
                .collect();
            required_ordinals = plan.required_ordinals.clone();
            result_columns = plan.headers.clone();
            aggregate_plan_opt = Some(plan);
        } else {
            let projection_plan = build_projection(
                projection_items.clone(),
                &columns,
                &column_ordinals,
                &table_name,
                table_alias.as_deref(),
            )?;
            alias_map = build_projection_alias_map(&projection_plan, &columns);
            projection_exprs = projection_expressions_from_plan(&projection_plan, &columns);
            required_ordinals = projection_plan.required_ordinals.clone();
            result_columns = projection_plan.headers.clone();
            projection_plan_opt = Some(projection_plan);
        }

        let resolved_group_by = resolve_group_by_exprs(&group_by, &projection_exprs)?;
        let resolved_order_by = resolve_order_by_exprs(&order_by_clauses, &projection_exprs)?;
        let order_clauses = plan_order_clauses(
            &resolved_order_by,
            if alias_map.is_empty() {
                None
            } else {
                Some(&alias_map)
            },
        )?;
        let qualify_expr = qualify.as_ref().map(|expr| {
            if alias_map.is_empty() {
                expr.clone()
            } else {
                rewrite_aliases_in_expr(expr, &alias_map)
            }
        });
        let group_strategy = if has_grouping {
            Some(determine_group_by_strategy(
                &resolved_group_by,
                &sort_columns,
                &order_clauses,
            )?)
        } else {
            None
        };
        let group_by_info = validate_group_by(&resolved_group_by)?;

        for column in &sort_columns {
            required_ordinals.insert(column.ordinal);
        }

        for clause in &order_clauses {
            let ordinals =
                collect_expr_column_ordinals(&clause.expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(ordinals);
        }

        if let Some(expr) = qualify_expr.as_ref() {
            let ordinals = collect_expr_column_ordinals(expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(ordinals);
        }

        if let Some(group_info) = &group_by_info {
            for grouping in &group_info.sets {
                for expr in &grouping.expressions {
                    let ordinals =
                        collect_expr_column_ordinals(expr, &column_ordinals, &table_name)?;
                    required_ordinals.extend(ordinals);
                }
            }
        }

        if let Some(having_expr) = &having {
            let ordinals =
                collect_expr_column_ordinals(having_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(ordinals);
        }

        for plan in &window_plans {
            for expr in &plan.partition_by {
                let ordinals = collect_expr_column_ordinals(expr, &column_ordinals, &table_name)?;
                required_ordinals.extend(ordinals);
            }
            for order in &plan.order_by {
                let ordinals =
                    collect_expr_column_ordinals(&order.expr, &column_ordinals, &table_name)?;
                required_ordinals.extend(ordinals);
            }
            if let Some(arg) = &plan.arg {
                let ordinals = collect_expr_column_ordinals(arg, &column_ordinals, &table_name)?;
                required_ordinals.extend(ordinals);
            }
        }

        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        let apply_selection_late = !window_plans.is_empty();

        let mut sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;
        let mut sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };
        if let Some(expr) = if has_selection { Some(&selection_expr) } else { None } {
            if selection_uses_numeric_literal_on_string_sort(expr, &sort_columns, &column_types) {
                sort_key_filters = None;
                sort_key_prefixes = None;
            }
        }

        let scan_selection_expr = None;
        let mut selection_applied_in_scan = false;
        let selection_expr_full = if has_selection {
            Some(&selection_expr)
        } else {
            None
        };

        if !window_plans.is_empty() {
            if has_selection && !can_use_physical_filter {
                return Err(SqlExecutionError::Unsupported(
                    "complex WHERE predicates are not supported with window queries".into(),
                ));
            }
            let projection_plan = projection_plan_opt.take().ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "window queries require an explicit projection plan".into(),
                )
            })?;
            let partition_exprs = ensure_common_partition(&window_plans)?;
            let vectorized_selection_expr = if has_selection && can_use_physical_filter {
                physical_selection_expr.as_ref()
            } else {
                None
            };
            let window_plans_vec = std::mem::take(&mut window_plans);
            return self.execute_vectorized_window_query(
                &table_name,
                &catalog,
                &columns,
                projection_plan,
                window_plans_vec,
                partition_exprs,
                &required_ordinals,
                selection_expr_full,
                vectorized_selection_expr,
                selection_applied_in_scan,
                &column_ordinals,
                &column_types,
                &order_clauses,
                result_columns.clone(),
                limit_expr.clone(),
                offset_expr.clone(),
                distinct_flag,
                qualify.clone(),
                None,
            );
        }

        let simple_group_exprs = if let Some(info) = &group_by_info {
            if info.sets.len() == 1 && info.sets[0].masked_exprs.is_empty() {
                Some(info.sets[0].expressions.clone())
            } else {
                None
            }
        } else {
            Some(Vec::new())
        };

        let mut used_index = sort_key_filters.is_some() || sort_key_prefixes.is_some();
        let mut row_ids = if let Some(sort_key_filters) = sort_key_filters.as_ref() {
            for column in &sort_columns {
                key_values.push(sort_key_filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "SELECT requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?);
            }
            Some(self.locate_rows_by_sort_tuple(
                &table_name,
                &sort_columns,
                &key_values,
            )?)
        } else if let Some(prefixes) = sort_key_prefixes.as_ref() {
            Some(self.locate_rows_by_sort_prefixes(
                &table_name,
                &sort_columns,
                &prefixes,
            )?)
        } else {
            None
        };

        if used_index {
            selection_applied_in_scan = false;
        }

        if let Some(rows) = row_ids.as_mut() {
            rows.sort_unstable();
            rows.dedup();
            if rows.is_empty() {
                row_ids = None;
                used_index = false;
            }
        }
        if !needs_aggregation {
            let projection_plan = projection_plan_opt.expect("projection plan required");
            return self.execute_vectorized_projection(
                &table_name,
                &catalog,
                &columns,
                &projection_plan,
                &required_ordinals,
                selection_expr_full,
                scan_selection_expr,
                selection_applied_in_scan,
                &column_ordinals,
                &column_types,
                &order_clauses,
                result_columns.clone(),
                limit_expr.clone(),
                offset_expr.clone(),
                distinct_flag,
                qualify_expr.as_ref(),
                row_ids.clone(),
            );
        }

        let stream = self.build_scan_stream(
            &table_name,
            &columns,
            &required_ordinals,
            scan_selection_expr,
            row_ids.clone(),
        )?;
        let mut base_batches = collect_stream_batches(stream)?;
        if has_selection && !selection_applied_in_scan {
            if let Some(expr) = selection_expr_full {
                let mut filtered = Vec::new();
                for batch in base_batches {
                    let mut filter = FilterOperator::new(
                        self,
                        expr,
                        scan_selection_expr,
                        &catalog,
                        &table_name,
                        &columns,
                        &column_ordinals,
                        &column_types,
                    );
                    let results = filter.execute(batch)?;
                    for result in results {
                        if result.num_rows > 0 {
                            filtered.push(result);
                        }
                    }
                }
                base_batches = filtered;
            }
        }
        let base_batch = merge_batches(base_batches);

        if let Some(group_exprs) = simple_group_exprs.clone() {
            let aggregate_plan = aggregate_plan_opt.expect("aggregate plan must exist");
            let prefer_exact_numeric = group_strategy
                .as_ref()
                .map_or(false, GroupByStrategy::prefer_exact_numeric);
            let aggregate_operator = AggregateOperator::new(
                self,
                &table_name,
                &catalog,
                &columns,
                &aggregate_plan,
                &required_ordinals,
                &column_ordinals,
                &column_types,
                prefer_exact_numeric,
                result_columns.clone(),
                limit_expr.clone(),
                offset_expr.clone(),
                having.as_ref(),
                qualify_expr.as_ref(),
                &order_clauses,
                distinct_flag,
            );
            return aggregate_operator.execute_simple_from_batch(&base_batch, &group_exprs);
        }

        if needs_aggregation {
            let aggregate_plan = aggregate_plan_opt.expect("aggregate plan must exist");
            let prefer_exact_numeric = group_strategy
                .as_ref()
                .map_or(false, GroupByStrategy::prefer_exact_numeric);
            let aggregate_operator = AggregateOperator::new(
                self,
                &table_name,
                &catalog,
                &columns,
                &aggregate_plan,
                &required_ordinals,
                &column_ordinals,
                &column_types,
                prefer_exact_numeric,
                result_columns.clone(),
                limit_expr.clone(),
                offset_expr.clone(),
                having.as_ref(),
                qualify_expr.as_ref(),
                &order_clauses,
                distinct_flag,
            );
            let mut aggregated_rows: Vec<AggregatedRow> = Vec::new();

            if let Some(group_info) = &group_by_info {
                for grouping in &group_info.sets {
                    aggregated_rows.extend(
                        aggregate_operator.execute_grouping_set_rows_from_batch(
                            &base_batch,
                            &grouping.expressions,
                            Some(grouping.masked_exprs.as_slice()),
                        )?,
                    );
                }
            } else {
                aggregated_rows.extend(
                    aggregate_operator.execute_grouping_set_rows_from_batch(&base_batch, &[], None)?,
                );
            }

            return self.finalize_aggregation_rows(
                aggregated_rows,
                &order_clauses,
                distinct_flag,
                limit_expr.clone(),
                offset_expr.clone(),
                result_columns,
            );
        }

        Err(SqlExecutionError::OperationFailed(
            "select execution fell through without a pipeline path".into(),
        ))
    }
}

fn selection_uses_numeric_literal_on_string_sort(
    expr: &Expr,
    sort_columns: &[ColumnCatalog],
    column_types: &HashMap<String, DataType>,
) -> bool {
    let sort_names: HashSet<&str> = sort_columns.iter().map(|col| col.name.as_str()).collect();
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                if let Some(name) = column_name_from_expr(left) {
                    if sort_names.contains(name.as_str()) {
                        if is_numeric_literal(right) {
                            return matches!(
                                column_types.get(&name),
                                Some(DataType::String)
                            );
                        }
                    }
                }
                if let Some(name) = column_name_from_expr(right) {
                    if sort_names.contains(name.as_str()) {
                        if is_numeric_literal(left) {
                            return matches!(
                                column_types.get(&name),
                                Some(DataType::String)
                            );
                        }
                    }
                }
            }
            selection_uses_numeric_literal_on_string_sort(left, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(right, sort_columns, column_types)
        }
        Expr::Nested(inner) => {
            selection_uses_numeric_literal_on_string_sort(inner, sort_columns, column_types)
        }
        Expr::UnaryOp { expr, .. } => {
            selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
        }
        Expr::Between { expr, low, high, .. } => {
            selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(low, sort_columns, column_types)
                || selection_uses_numeric_literal_on_string_sort(high, sort_columns, column_types)
        }
        Expr::InList { expr, list, .. } => {
            if selection_uses_numeric_literal_on_string_sort(expr, sort_columns, column_types) {
                return true;
            }
            list.iter().any(|item| {
                selection_uses_numeric_literal_on_string_sort(item, sort_columns, column_types)
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|op| {
                    selection_uses_numeric_literal_on_string_sort(
                        op,
                        sort_columns,
                        column_types,
                    )
                })
                || conditions.iter().any(|cond| {
                    selection_uses_numeric_literal_on_string_sort(
                        cond,
                        sort_columns,
                        column_types,
                    )
                })
                || results.iter().any(|res| {
                    selection_uses_numeric_literal_on_string_sort(
                        res,
                        sort_columns,
                        column_types,
                    )
                })
                || else_result.as_ref().is_some_and(|expr| {
                    selection_uses_numeric_literal_on_string_sort(
                        expr,
                        sort_columns,
                        column_types,
                    )
                })
        }
        _ => false,
    }
}

fn is_numeric_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(Value::Number(_, _)) => true,
        Expr::UnaryOp { op, expr } => {
            matches!(op, sqlparser::ast::UnaryOperator::Plus | sqlparser::ast::UnaryOperator::Minus)
                && matches!(**expr, Expr::Value(Value::Number(_, _)))
        }
        _ => false,
    }
}
