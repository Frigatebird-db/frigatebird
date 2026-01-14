use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::pipeline::operators::{
    DistinctOperator, FilterOperator, LimitOperator, PipelineOperator, ProjectOperator,
    SortOperator, WindowOperator as PipelineWindowOperator,
};
use crate::pipeline::window_helpers::{
    WindowFunctionPlan, assign_window_display_aliases, collect_window_function_plans,
    collect_window_plans_from_expr, plan_order_clauses, rewrite_projection_plan_for_windows,
    rewrite_window_expressions,
};
use crate::pipeline::filtering::apply_qualify_filter;
use crate::sql::executor::batch::ColumnarBatch;
use crate::pipeline::operators::AggregateOperator;
use crate::pipeline::planner::plan_row_ids_for_select;
use crate::pipeline::window_helpers::ensure_common_partition;
use crate::sql::executor::{
    AggregateProjectionPlan, OrderClause, ProjectionPlan, SqlExecutionError,
    SqlExecutor, SelectResult, merge_batches,
};
use crate::sql::executor::select_helpers::GroupByStrategy;
use crate::sql::executor::aggregates::{
    plan_aggregate_projection, select_item_contains_aggregate,
};
use crate::sql::executor::grouping_helpers::validate_group_by;
use crate::sql::executor::helpers::{
    collect_expr_column_ordinals, object_name_to_string, parse_limit, parse_offset,
};
use crate::sql::executor::projection_helpers::build_projection;
use crate::sql::executor::select_helpers::{
    build_aggregate_alias_map, build_projection_alias_map, determine_group_by_strategy,
    projection_expressions_from_plan, resolve_group_by_exprs, resolve_order_by_exprs,
    rewrite_aliases_in_expr,
};
use crate::sql::executor::physical_evaluator::filter_supported;
use crate::sql::executor::scan_stream::collect_stream_batches;
use crate::sql::PlannerError;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::planner::ExpressionPlanner;
use crate::sql::types::DataType;
use sqlparser::ast::{
    Expr, GroupByExpr, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, Value,
};
use std::collections::{BTreeSet, HashMap};

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_projection_pipeline(
    executor: &SqlExecutor,
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
    let stream = executor.build_scan_stream(
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
                    executor,
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
                executor,
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
        let mut sorter = SortOperator::new(executor, order_clauses, catalog);
        processed_batches = sorter.execute_batches(processed_batches)?;
    }

    let mut project = ProjectOperator::new(executor, projection_plan, catalog);
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
        let mut limiter = LimitOperator::new(executor, offset, limit);
        let limited_batches = limiter.execute_batches(deduped)?;
        return Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        });
    }

    let mut limiter = LimitOperator::new(executor, offset, limit);
    let limited_batches = limiter.execute_batches(projected)?;
    Ok(SelectResult {
        columns: result_columns,
        batches: limited_batches,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_window_pipeline(
    executor: &SqlExecutor,
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

    let stream = executor.build_scan_stream(
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
                    executor,
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
        processed_batch = apply_qualify_filter(processed_batch, &expr, catalog)?;
        if processed_batch.num_rows == 0 {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }
    }

    if !final_order_clauses.is_empty() {
        let mut sorter = SortOperator::new(executor, &final_order_clauses, catalog);
        let sorted_batches = sorter.execute_batches(vec![processed_batch])?;
        processed_batch = merge_batches(sorted_batches);
    }

    let mut project = ProjectOperator::new(executor, &projection_plan, catalog);
    let mut results = project.execute(processed_batch)?;
    let final_batch = results.pop().unwrap_or_else(ColumnarBatch::new);
    let offset = parse_offset(offset_expr)?;
    let limit = parse_limit(limit_expr)?;

    if distinct_flag {
        let mut distinct = DistinctOperator::new(projection_plan.items.len());
        let deduped = distinct.execute_batches(vec![final_batch])?;
        let mut limiter = LimitOperator::new(executor, offset, limit);
        let limited_batches = limiter.execute_batches(deduped)?;
        return Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        });
    }

    let mut limiter = LimitOperator::new(executor, offset, limit);
    let limited_batches = limiter.execute_batches(vec![final_batch])?;
    Ok(SelectResult {
        columns: result_columns,
        batches: limited_batches,
    })
}

pub(crate) fn execute_select_plan(
    executor: &SqlExecutor,
    mut query: Query,
) -> Result<SelectResult, SqlExecutionError> {
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

    let catalog = executor
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
    let selection_expr_opt = if has_selection { Some(&selection_expr) } else { None };

    let expr_planner = ExpressionPlanner::new(&catalog);
    let physical_selection_expr = if has_selection {
        match expr_planner.plan_expression(&selection_expr) {
            Ok(expr) => Some(expr),
            Err(crate::sql::PlannerError::Unsupported(_)) => None,
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
        if alias_map.is_empty() { None } else { Some(&alias_map) },
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

    let row_id_selection = selection_expr_opt;
    let mut scan_selection_expr = if has_selection && can_use_physical_filter {
        physical_selection_expr.as_ref()
    } else {
        None
    };
    let mut selection_applied_in_scan = scan_selection_expr.is_some();
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
        return execute_window_pipeline(
            executor,
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

    let simple_group_exprs: Option<Vec<Expr>> = if let Some(info) = &group_by_info {
        if info.sets.len() == 1 && info.sets[0].masked_exprs.is_empty() {
            Some(info.sets[0].expressions.clone())
        } else {
            None
        }
    } else {
        Some(Vec::new())
    };

    let mut row_ids = plan_row_ids_for_select(
        executor,
        &table_name,
        &sort_columns,
        row_id_selection,
        &column_types,
    )?;
    let mut used_index = row_ids.is_some();
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
        return execute_projection_pipeline(
            executor,
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

    let stream = executor.build_scan_stream(
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
                    executor,
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
            executor,
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
            executor,
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
        let mut aggregated_rows = Vec::new();

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

        return executor.finalize_aggregation_rows(
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
