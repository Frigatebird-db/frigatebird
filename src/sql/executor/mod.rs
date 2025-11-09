pub mod batch;
mod aggregates;
mod expressions;
mod grouping_helpers;
mod helpers;
mod ordering;
mod projection_helpers;
mod row_functions;
mod scalar_functions;
mod scan_helpers;
mod values;
mod window_helpers;

use self::batch::ColumnarPage;
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{ColumnCatalog, PageDescriptor, PageDirectory};
use crate::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::sql::{CreateTablePlan, plan_create_table_statement};
use crate::writer::{
    ColumnUpdate, DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator,
    UpdateJob, UpdateOp, Writer,
};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, FromTable, FunctionArg, FunctionArgExpr, GroupByExpr, Ident,
    ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value, WindowFrameBound, WindowFrameUnits, WindowType,
};
use sqlparser::parser::ParserError;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use aggregates::{
    AggregateDataset, AggregateProjectionPlan, MaterializedColumns, WindowResultMap,
    evaluate_aggregate_outputs, plan_aggregate_projection, select_item_contains_aggregate,
};
use expressions::{
    evaluate_row_expr, evaluate_scalar_expression, evaluate_selection_expr,
    evaluate_selection_on_page,
};
use grouping_helpers::{evaluate_group_key, evaluate_having, validate_group_by};
use helpers::{
    collect_expr_column_names, collect_expr_column_ordinals, column_name_from_expr, expr_to_string,
    object_name_matches_table, object_name_to_string, parse_interval_seconds, parse_limit,
    parse_offset, table_with_joins_to_name, wildcard_options_supported,
};
use ordering::{
    NullsPlacement, OrderClause, OrderKey, build_group_order_key, build_row_order_key,
    compare_order_keys, sort_rows_logical,
};
use projection_helpers::{build_projection, materialize_columns};
use scan_helpers::collect_sort_key_filters;
use values::{CachedValue, ScalarValue, compare_strs, scalar_from_f64};
use window_helpers::{
    collect_window_function_plans, collect_window_plans_from_expr, compute_window_results,
    plan_order_clauses,
};

#[derive(Debug)]
pub enum SqlExecutionError {
    Parse(ParserError),
    Plan(crate::sql::models::PlannerError),
    Unsupported(String),
    TableNotFound(String),
    ColumnMismatch { table: String, column: String },
    ValueMismatch(String),
    OperationFailed(String),
}

impl std::fmt::Display for SqlExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlExecutionError::Parse(err) => write!(f, "failed to parse SQL: {err}"),
            SqlExecutionError::Plan(err) => write!(f, "{err}"),
            SqlExecutionError::Unsupported(msg) => write!(f, "unsupported SQL: {msg}"),
            SqlExecutionError::TableNotFound(table) => write!(f, "unknown table: {table}"),
            SqlExecutionError::ColumnMismatch { table, column } => {
                write!(f, "column {column} is not defined on table {table}")
            }
            SqlExecutionError::ValueMismatch(msg) => write!(f, "{msg}"),
            SqlExecutionError::OperationFailed(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SqlExecutionError {}

impl From<ParserError> for SqlExecutionError {
    fn from(value: ParserError) -> Self {
        SqlExecutionError::Parse(value)
    }
}

impl From<crate::sql::models::PlannerError> for SqlExecutionError {
    fn from(value: crate::sql::models::PlannerError) -> Self {
        SqlExecutionError::Plan(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

struct ProjectionPlan {
    headers: Vec<String>,
    items: Vec<ProjectionItem>,
    required_ordinals: BTreeSet<usize>,
}

impl ProjectionPlan {
    fn new() -> Self {
        Self {
            headers: Vec::new(),
            items: Vec::new(),
            required_ordinals: BTreeSet::new(),
        }
    }

    fn needs_dataset(&self) -> bool {
        self.items
            .iter()
            .any(|item| matches!(item, ProjectionItem::Computed { .. }))
    }
}

fn build_projection_alias_map(
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

fn build_aggregate_alias_map(plan: &AggregateProjectionPlan) -> HashMap<String, Expr> {
    let mut map = HashMap::new();
    for (label, output) in plan.headers.iter().zip(plan.outputs.iter()) {
        map.insert(label.clone(), output.expr.clone());
    }
    map
}

fn projection_expressions_from_plan(plan: &ProjectionPlan, columns: &[ColumnCatalog]) -> Vec<Expr> {
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

fn resolve_group_by_exprs(
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

fn determine_group_by_strategy(
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
                let name = column_name_from_expr(expr).ok_or_else(|| {
                    SqlExecutionError::Unsupported(
                        "GROUP BY columns must match table sort key prefix".into(),
                    )
                })?;
                names.push(name);
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

    Err(SqlExecutionError::Unsupported(
        "GROUP BY columns must match table sort key prefix".into(),
    ))
}

enum GroupByStrategy {
    SortPrefix,
    OrderAligned,
}

impl GroupByStrategy {
    fn prefer_exact_numeric(&self) -> bool {
        matches!(self, GroupByStrategy::OrderAligned)
    }
}

fn resolve_order_by_exprs(
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

enum ProjectionItem {
    Direct { ordinal: usize },
    Computed { expr: Expr },
}

struct GroupByInfo {
    sets: Vec<GroupingSetPlan>,
}

#[derive(Clone)]
struct GroupingSetPlan {
    expressions: Vec<Expr>,
    masked_exprs: Vec<Expr>,
}

#[derive(Clone)]
struct AggregatedRow {
    order_key: OrderKey,
    values: Vec<Option<String>>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct GroupKey {
    values: Vec<Option<String>>,
}

#[derive(Clone)]
enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    Sum { frame: SumWindowFrame },
    Lag { offset: usize },
    Lead { offset: usize },
    FirstValue,
    LastValue,
}

#[derive(Clone)]
struct WindowFunctionPlan {
    key: String,
    kind: WindowFunctionKind,
    partition_by: Vec<Expr>,
    order_by: Vec<OrderByExpr>,
    arg: Option<Expr>,
    default_expr: Option<Expr>,
}

#[derive(Clone)]
enum SumWindowFrame {
    Rows { preceding: Option<usize> },
    Range { preceding: RangePreceding },
}

#[derive(Clone, Copy)]
enum RangePreceding {
    Unbounded,
    Value(f64),
}

const FULL_SCAN_BATCH_SIZE: u64 = 4_096;

pub struct SqlExecutor {
    page_handler: Arc<PageHandler>,
    page_directory: Arc<PageDirectory>,
    writer: Arc<Writer>,
    use_writer_inserts: bool,
}

impl SqlExecutor {
    pub fn new(page_handler: Arc<PageHandler>, page_directory: Arc<PageDirectory>) -> Self {
        Self::new_with_writer_mode(page_handler, page_directory, true)
    }

    pub fn new_with_writer_mode(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
    ) -> Self {
        let allocator: Arc<dyn PageAllocator> =
            Arc::new(DirectBlockAllocator::new().expect("allocator init failed"));
        let metadata_client: Arc<dyn MetadataClient> =
            Arc::new(DirectoryMetadataClient::new(Arc::clone(&page_directory)));
        let writer = Arc::new(Writer::new(
            Arc::clone(&page_handler),
            allocator,
            metadata_client,
        ));
        Self {
            page_handler,
            page_directory,
            writer,
            use_writer_inserts,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<(), SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }
        let statement = statements.remove(0);
        match statement {
            Statement::CreateTable { .. } => self.execute_create(statement),
            Statement::Insert { .. } => self.execute_insert(statement),
            Statement::Update {
                table,
                assignments,
                selection,
                returning,
                from,
                ..
            } => self.execute_update(table, assignments, selection, returning, from),
            Statement::Delete {
                tables,
                from,
                using,
                selection,
                returning,
                order_by,
                limit,
                ..
            } => self.execute_delete(tables, from, using, selection, returning, order_by, limit),
            other => Err(SqlExecutionError::Unsupported(format!(
                "{other:?} is not supported yet"
            ))),
        }
    }

    pub fn query(&self, sql: &str) -> Result<SelectResult, SqlExecutionError> {
        let mut statements = crate::sql::parse_sql(sql)?;
        if statements.is_empty() {
            return Err(SqlExecutionError::Unsupported("empty SQL statement".into()));
        }
        if statements.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "only single SQL statements are supported".into(),
            ));
        }

        match statements.remove(0) {
            Statement::Query(query) => self.execute_select(*query),
            other => Err(SqlExecutionError::Unsupported(format!(
                "{other:?} is not supported yet"
            ))),
        }
    }

    fn execute_select(&self, mut query: Query) -> Result<SelectResult, SqlExecutionError> {
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
            TableFactor::Table { name, alias, .. } => (
                object_name_to_string(name),
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "unsupported table reference".into(),
                ));
            }
        };

        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
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

        if needs_aggregation && qualify.is_some() {
            return Err(SqlExecutionError::Unsupported(
                "QUALIFY with aggregates is not supported yet".into(),
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

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;

        let scan_selection_expr = if has_selection && !apply_selection_late {
            Some(&selection_expr)
        } else {
            None
        };

        let mut candidate_rows = if let Some(sort_key_filters) = sort_key_filters {
            for column in &sort_columns {
                key_values.push(sort_key_filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "SELECT requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?);
            }

            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?
        } else {
            self.scan_rows_via_full_table(
                &table_name,
                &columns,
                &required_ordinals,
                scan_selection_expr,
                &column_ordinals,
            )?
        };
        if candidate_rows.is_empty() && !needs_aggregation {
            return Ok(SelectResult {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        let materialized = materialize_columns(
            &self.page_handler,
            &table_name,
            &columns,
            &required_ordinals,
            &candidate_rows,
        )?;

        let mut matching_rows = if apply_selection_late {
            candidate_rows.clone()
        } else {
            Vec::new()
        };
        let mut selection_set: HashSet<u64> = HashSet::new();

        for &row_idx in &candidate_rows {
            let passes = if has_selection {
                evaluate_selection_expr(
                    &selection_expr,
                    row_idx,
                    &column_ordinals,
                    &materialized,
                )?
            } else {
                true
            };

            if passes {
                if apply_selection_late {
                    selection_set.insert(row_idx);
                } else {
                    matching_rows.push(row_idx);
                }
            }
        }

        if apply_selection_late {
            if selection_set.is_empty() && !needs_aggregation {
                return Ok(SelectResult {
                    columns: result_columns,
                    rows: Vec::new(),
                });
            }
        }

        if !needs_aggregation {
            sort_rows_logical(
                &order_clauses,
                &materialized,
                &column_ordinals,
                &mut matching_rows,
            )?;
        }

        if apply_selection_late {
            matching_rows.retain(|row| selection_set.contains(row));
        }

        if needs_aggregation {
            let aggregate_plan = aggregate_plan_opt.expect("aggregate plan must exist");
            let mut aggregated_rows: Vec<AggregatedRow> = Vec::new();
            let prefer_exact_numeric = group_strategy
                .as_ref()
                .map_or(false, GroupByStrategy::prefer_exact_numeric);

            let full_dataset = AggregateDataset {
                rows: matching_rows.as_slice(),
                materialized: &materialized,
                column_ordinals: &column_ordinals,
                row_positions: None,
                window_results: None,
                masked_exprs: None,
                prefer_exact_numeric,
            };

            if let Some(group_info) = &group_by_info {
                for grouping in &group_info.sets {
                    let mut groups: HashMap<GroupKey, Vec<u64>> = HashMap::new();
                    let mut key_order: Vec<GroupKey> = Vec::new();

                    for &row_idx in &matching_rows {
                        let key =
                            evaluate_group_key(&grouping.expressions, row_idx, &full_dataset)?;
                        match groups.entry(key.clone()) {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                entry.get_mut().push(row_idx);
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(vec![row_idx]);
                                key_order.push(key);
                            }
                        }
                    }

                    for key in key_order {
                        let rows = groups.get(&key).expect("group rows must exist");
                        let dataset = AggregateDataset {
                            rows: rows.as_slice(),
                            materialized: &materialized,
                            column_ordinals: &column_ordinals,
                            row_positions: None,
                            window_results: None,
                            masked_exprs: Some(grouping.masked_exprs.as_slice()),
                            prefer_exact_numeric,
                        };

                        if !evaluate_having(&having, &dataset)? {
                            continue;
                        }

                        let order_key = if order_clauses.is_empty() {
                            OrderKey { values: Vec::new() }
                        } else {
                            build_group_order_key(&order_clauses, &dataset)?
                        };

                        let output_row = evaluate_aggregate_outputs(&aggregate_plan, &dataset)?;
                        aggregated_rows.push(AggregatedRow {
                            order_key,
                            values: output_row,
                        });
                    }
                }
            } else {
                let dataset = AggregateDataset {
                    rows: matching_rows.as_slice(),
                    materialized: &materialized,
                    column_ordinals: &column_ordinals,
                    row_positions: None,
                    window_results: None,
                    masked_exprs: None,
                    prefer_exact_numeric,
                };

                if evaluate_having(&having, &dataset)? {
                    let order_key = if order_clauses.is_empty() {
                        OrderKey { values: Vec::new() }
                    } else {
                        build_group_order_key(&order_clauses, &dataset)?
                    };
                    let output_row = evaluate_aggregate_outputs(&aggregate_plan, &dataset)?;
                    aggregated_rows.push(AggregatedRow {
                        order_key,
                        values: output_row,
                    });
                }
            }

            if distinct_flag {
                let mut seen: HashSet<Vec<Option<String>>> = HashSet::new();
                aggregated_rows.retain(|row| seen.insert(row.values.clone()));
            }

            if !order_clauses.is_empty() {
                aggregated_rows.sort_unstable_by(|left, right| {
                    compare_order_keys(&left.order_key, &right.order_key, &order_clauses)
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

            return Ok(SelectResult {
                columns: result_columns,
                rows: final_rows,
            });
        }

        let mut window_results_map: Option<WindowResultMap> = None;
        let mut row_positions_map: Option<HashMap<u64, usize>> = None;
        if !window_plans.is_empty() {
            let results = compute_window_results(
                &window_plans,
                matching_rows.as_slice(),
                &materialized,
                &column_ordinals,
            )?;
            let positions = matching_rows
                .iter()
                .enumerate()
                .map(|(idx, row)| (*row, idx))
                .collect::<HashMap<u64, usize>>();
            row_positions_map = Some(positions);
            window_results_map = Some(results);

            if apply_selection_late {
                if let Some(results_map) = window_results_map.as_mut() {
                    let window_dataset = AggregateDataset {
                        rows: matching_rows.as_slice(),
                        materialized: &materialized,
                        column_ordinals: &column_ordinals,
                        row_positions: None,
                        window_results: None,
                        masked_exprs: None,
                        prefer_exact_numeric: false,
                    };

                    for plan in &window_plans {
                        if let WindowFunctionKind::Sum { frame } = &plan.kind {
                            if !selection_set.is_empty() {
                                if let SumWindowFrame::Rows { preceding } = frame {
                                    if preceding.is_some() {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                                if let Some(values) = results_map.get_mut(&plan.key) {
                                    if let Some(arg_expr) = &plan.arg {
                                        for (idx, &row_idx) in matching_rows.iter().enumerate() {
                                            if !selection_set.contains(&row_idx) {
                                                continue;
                                            }
                                            let partition_key = evaluate_group_key(
                                                &plan.partition_by,
                                                row_idx,
                                                &window_dataset,
                                            )?;
                                            let mut running_sum = 0.0;
                                            let mut found_value = false;
                                            for &candidate_row in matching_rows.iter().take(idx + 1) {
                                                let candidate_key = evaluate_group_key(
                                                    &plan.partition_by,
                                                    candidate_row,
                                                    &window_dataset,
                                                )?;
                                                if candidate_key == partition_key {
                                                    let value = evaluate_row_expr(
                                                        arg_expr,
                                                        candidate_row,
                                                        &window_dataset,
                                                    )?;
                                                    if let Some(num) = value.as_f64() {
                                                        running_sum += num;
                                                        found_value = true;
                                                    }
                                                }
                                            }
                                            if found_value {
                                                values[idx] = scalar_from_f64(running_sum);
                                            } else {
                                                values[idx] = ScalarValue::Null;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let no_selected_rows = apply_selection_late && selection_set.is_empty();
        if (!apply_selection_late && matching_rows.is_empty()) || no_selected_rows {
            return Ok(SelectResult {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        let projection_plan = projection_plan_opt.expect("projection plan required");
        let mut rows = Vec::with_capacity(matching_rows.len());
        let dataset_required =
            projection_plan.needs_dataset() || qualify.is_some() || !window_plans.is_empty();
        let dataset_holder = if dataset_required {
            Some(AggregateDataset {
                rows: matching_rows.as_slice(),
                materialized: &materialized,
                column_ordinals: &column_ordinals,
                row_positions: row_positions_map.as_ref(),
                window_results: window_results_map.as_ref(),
                masked_exprs: None,
                prefer_exact_numeric: false,
            })
        } else {
            None
        };
        let dataset = dataset_holder.as_ref();

        for &row_idx in &matching_rows {
            if apply_selection_late && !selection_set.contains(&row_idx) {
                continue;
            }
            if let Some(qualify_expr) = &qualify {
                let dataset = dataset.expect("dataset required for QUALIFY evaluation");
                let value = evaluate_row_expr(qualify_expr, row_idx, dataset)?;
                if !value.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            let mut projected = Vec::with_capacity(projection_plan.items.len());
            for item in &projection_plan.items {
                match item {
                    ProjectionItem::Direct { ordinal } => {
                        let cached = materialized
                            .get(ordinal)
                            .and_then(|column_map| column_map.get(&row_idx))
                            .cloned()
                            .or_else(|| {
                                self.page_handler
                                    .read_entry_at(&table_name, &columns[*ordinal].name, row_idx)
                                    .map(|entry| CachedValue::from_entry(&entry))
                            })
                            .ok_or_else(|| {
                                SqlExecutionError::OperationFailed(format!(
                                    "missing value for {table_name}.{} at row {row_idx}",
                                    columns[*ordinal].name
                                ))
                            })?;
                        projected.push(cached.into_option_string());
                    }
                    ProjectionItem::Computed { expr } => {
                        let dataset = dataset.expect("dataset required for computed projection");
                        let value = evaluate_row_expr(expr, row_idx, dataset)?;
                        projected.push(value.into_option_string());
                    }
                }
            }
            rows.push(projected);
        }

        if distinct_flag {
            let mut seen: HashSet<Vec<Option<String>>> = HashSet::with_capacity(rows.len());
            let mut deduped = Vec::with_capacity(rows.len());
            for row in rows.into_iter() {
                if seen.insert(row.clone()) {
                    deduped.push(row);
                }
            }
            rows = deduped;
        }

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        let start = offset.min(rows.len());
        let end = if let Some(limit) = limit {
            start.saturating_add(limit).min(rows.len())
        } else {
            rows.len()
        };
        let final_rows = rows[start..end].to_vec();

        Ok(SelectResult {
            columns: result_columns,
            rows: final_rows,
        })
    }

    fn execute_create(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        let plan: CreateTablePlan = plan_create_table_statement(&statement)?;
        create_table_from_plan(&self.page_directory, &plan)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        Ok(())
    }

    fn execute_insert(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        if self.use_writer_inserts {
            self.execute_insert_writer(statement)
        } else {
            self.execute_insert_legacy(statement)
        }
    }

    fn execute_insert_writer(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        let Statement::Insert {
            table_name,
            columns: specified_columns,
            source,
            ..
        } = statement
        else {
            unreachable!("matched Insert above");
        };

        let table = object_name_to_string(&table_name);
        let query = source
            .as_ref()
            .ok_or_else(|| SqlExecutionError::Unsupported("INSERT without VALUES".into()))?;

        let SetExpr::Values(values) = query.body.as_ref() else {
            return Err(SqlExecutionError::Unsupported(
                "only INSERT ... VALUES is supported".into(),
            ));
        };

        let catalog = self
            .page_directory
            .table_catalog(&table)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();

        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        let specified_ordinals: Vec<usize> = if specified_columns.is_empty() {
            (0..columns.len()).collect()
        } else {
            specified_columns
                .iter()
                .map(|ident| {
                    let name = ident.value.clone();
                    column_ordinals.get(&name).copied().ok_or_else(|| {
                        SqlExecutionError::ColumnMismatch {
                            table: table.clone(),
                            column: name,
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "INSERT currently requires ORDER BY tables".into(),
            ));
        }
        for row in &values.rows {
            if row.len() != specified_ordinals.len() {
                return Err(SqlExecutionError::ValueMismatch(format!(
                    "expected {} values, got {}",
                    specified_ordinals.len(),
                    row.len()
                )));
            }

            let mut row_values: Vec<Option<String>> = vec![None; columns.len()];
            for (expr, &ordinal) in row.iter().zip(&specified_ordinals) {
                let literal = expr_to_string(expr)?;
                row_values[ordinal] = Some(literal);
            }

            for &ordinal in &sort_indices {
                if row_values[ordinal].is_none() {
                    return Err(SqlExecutionError::ValueMismatch(format!(
                        "missing value for sort column {}",
                        columns[ordinal].name
                    )));
                }
            }

            let final_row: Vec<String> = row_values
                .into_iter()
                .map(|value| value.unwrap_or_default())
                .collect();

            let column_update = ColumnUpdate::new(
                "*",
                vec![UpdateOp::BufferRow {
                    row: final_row,
                }],
            );
            let job = UpdateJob::new(table.clone(), vec![column_update]);
            self.writer.submit(job).map_err(|err| {
                SqlExecutionError::OperationFailed(format!(
                    "failed to submit insert job: {err:?}"
                ))
            })?;
        }

        if !values.rows.is_empty() {
            self.writer
                .flush_table(&table)
                .map_err(|err| SqlExecutionError::OperationFailed(format!("flush failed: {err:?}")))?;
        }

        Ok(())
    }

    fn execute_insert_legacy(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        let Statement::Insert {
            table_name,
            columns: specified_columns,
            source,
            ..
        } = statement
        else {
            unreachable!("matched Insert above");
        };

        let table = object_name_to_string(&table_name);
        let query = source
            .as_ref()
            .ok_or_else(|| SqlExecutionError::Unsupported("INSERT without VALUES".into()))?;

        let SetExpr::Values(values) = query.body.as_ref() else {
            return Err(SqlExecutionError::Unsupported(
                "only INSERT ... VALUES is supported".into(),
            ));
        };

        let catalog = self
            .page_directory
            .table_catalog(&table)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();

        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        let specified_ordinals: Vec<usize> = if specified_columns.is_empty() {
            (0..columns.len()).collect()
        } else {
            specified_columns
                .iter()
                .map(|ident| {
                    let name = ident.value.clone();
                    column_ordinals.get(&name).copied().ok_or_else(|| {
                        SqlExecutionError::ColumnMismatch {
                            table: table.clone(),
                            column: name,
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "INSERT currently requires ORDER BY tables".into(),
            ));
        }
        for row in &values.rows {
            if row.len() != specified_ordinals.len() {
                return Err(SqlExecutionError::ValueMismatch(format!(
                    "expected {} values, got {}",
                    specified_ordinals.len(),
                    row.len()
                )));
            }

            let mut values_by_ordinal: Vec<String> = vec![String::new(); columns.len()];
            for (expr, &ordinal) in row.iter().zip(&specified_ordinals) {
                let literal = expr_to_string(expr)?;
                values_by_ordinal[ordinal] = literal;
            }

            let leading_column = &columns[sort_indices[0]].name;
            if self.table_is_empty(&table, leading_column)? {
                self.initialise_table_with_row(&table, &columns, &values_by_ordinal)?;
            } else {
                let mut kv_pairs = Vec::with_capacity(columns.len());
                for column in &columns {
                    kv_pairs.push((
                        column.name.clone(),
                        values_by_ordinal[column.ordinal].clone(),
                    ));
                }
                let tuple: Vec<(&str, &str)> = kv_pairs
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                insert_sorted_row(&self.page_handler, &table, &tuple)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            }
        }

        Ok(())
    }

    fn execute_update(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        from: Option<TableWithJoins>,
    ) -> Result<(), SqlExecutionError> {
        if returning.is_some() || from.is_some() {
            return Err(SqlExecutionError::Unsupported(
                "UPDATE with RETURNING or FROM is not supported".into(),
            ));
        }

        let table_name = table_with_joins_to_name(&table)?;
        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "UPDATE currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_indices
            .iter()
            .map(|&idx| columns[idx].clone())
            .collect();

        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        if assignments.is_empty() {
            return Ok(());
        }

        let mut assignments_vec = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            if assignment.id.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "assignment missing column".into(),
                ));
            }
            let column_name = assignment.id.last().unwrap().value.clone();
            let ordinal = column_ordinals.get(&column_name).copied().ok_or_else(|| {
                SqlExecutionError::ColumnMismatch {
                    table: table_name.clone(),
                    column: column_name.clone(),
                }
            })?;
            let value = expr_to_string(&assignment.value)?;
            assignments_vec.push((ordinal, value));
        }

        let selection_expr = selection.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
        let has_selection = !matches!(selection_expr, Expr::Value(Value::Boolean(true)));

        let mut required_ordinals: BTreeSet<usize> = sort_indices.iter().copied().collect();
        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;

        let mut key_values = Vec::with_capacity(sort_columns.len());
        let mut candidate_rows = if let Some(filters) = sort_key_filters {
            for column in &sort_columns {
                let value = filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "UPDATE requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?;
                key_values.push(value);
            }
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?
        } else {
            self.scan_rows_via_full_table(
                &table_name,
                &columns,
                &required_ordinals,
                if has_selection {
                    Some(&selection_expr)
                } else {
                    None
                },
                &column_ordinals,
            )?
        };

        if candidate_rows.is_empty() {
            return Ok(());
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        let materialized = materialize_columns(
            &self.page_handler,
            &table_name,
            &columns,
            &required_ordinals,
            &candidate_rows,
        )?;

        let mut matching_rows = Vec::new();
        for &row_idx in &candidate_rows {
            if !has_selection
                || evaluate_selection_expr(
                    &selection_expr,
                    row_idx,
                    &column_ordinals,
                    &materialized,
                )?
            {
                matching_rows.push(row_idx);
            }
        }

        if matching_rows.is_empty() {
            return Ok(());
        }

        matching_rows.sort_unstable();

        for &row_idx in matching_rows.iter().rev() {
            let current_row = read_row(&self.page_handler, &table_name, row_idx)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            let mut new_row = current_row.clone();
            for (ordinal, value) in &assignments_vec {
                new_row[*ordinal] = value.clone();
            }

            let sort_changed = sort_indices
                .iter()
                .any(|&idx| compare_strs(&current_row[idx], &new_row[idx]) != Ordering::Equal);

            if sort_changed {
                delete_row(&self.page_handler, &table_name, row_idx)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
                let kv_pairs: Vec<(String, String)> = columns
                    .iter()
                    .map(|col| (col.name.clone(), new_row[col.ordinal].clone()))
                    .collect();
                let tuple: Vec<(&str, &str)> = kv_pairs
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                insert_sorted_row(&self.page_handler, &table_name, &tuple)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            } else {
                overwrite_row(&self.page_handler, &table_name, row_idx, &new_row)
                    .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
            }
        }

        Ok(())
    }

    fn execute_delete(
        &self,
        tables: Vec<ObjectName>,
        from: FromTable,
        using: Option<Vec<TableWithJoins>>,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        order_by: Vec<OrderByExpr>,
        limit: Option<Expr>,
    ) -> Result<(), SqlExecutionError> {
        if !tables.is_empty()
            || using.is_some()
            || returning.is_some()
            || !order_by.is_empty()
            || limit.is_some()
        {
            return Err(SqlExecutionError::Unsupported(
                "DELETE with advanced clauses is not supported".into(),
            ));
        }

        let table_with_joins = match &from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                if tables.len() != 1 {
                    return Err(SqlExecutionError::Unsupported(
                        "DELETE supports exactly one table".into(),
                    ));
                }
                &tables[0]
            }
        };
        if !table_with_joins.joins.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "DELETE with JOINs is not supported".into(),
            ));
        }
        let table_name = match &table_with_joins.relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "unsupported DELETE target".into(),
                ));
            }
        };

        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        let sort_indices: Vec<usize> = catalog.sort_key().iter().map(|col| col.ordinal).collect();
        if sort_indices.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "DELETE currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_indices
            .iter()
            .map(|&idx| columns[idx].clone())
            .collect();

        let selection_expr = selection.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
        let has_selection = !matches!(selection_expr, Expr::Value(Value::Boolean(true)));

        let mut required_ordinals: BTreeSet<usize> = sort_indices.iter().copied().collect();
        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;

        let mut key_values = Vec::with_capacity(sort_columns.len());
        let mut candidate_rows = if let Some(filters) = sort_key_filters {
            for column in &sort_columns {
                let value = filters.get(&column.name).cloned().ok_or_else(|| {
                    SqlExecutionError::Unsupported(format!(
                        "DELETE requires equality predicate for ORDER BY column {}",
                        column.name
                    ))
                })?;
                key_values.push(value);
            }
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?
        } else {
            self.scan_rows_via_full_table(
                &table_name,
                &columns,
                &required_ordinals,
                if has_selection {
                    Some(&selection_expr)
                } else {
                    None
                },
                &column_ordinals,
            )?
        };

        if candidate_rows.is_empty() {
            return Ok(());
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        let materialized = materialize_columns(
            &self.page_handler,
            &table_name,
            &columns,
            &required_ordinals,
            &candidate_rows,
        )?;

        let mut matching_rows = Vec::new();
        for &row_idx in &candidate_rows {
            if !has_selection
                || evaluate_selection_expr(
                    &selection_expr,
                    row_idx,
                    &column_ordinals,
                    &materialized,
                )?
            {
                matching_rows.push(row_idx);
            }
        }

        if matching_rows.is_empty() {
            return Ok(());
        }

        matching_rows.sort_unstable();
        matching_rows.dedup();
        for row_idx in matching_rows.into_iter().rev() {
            delete_row(&self.page_handler, &table_name, row_idx)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }

        Ok(())
    }

    fn table_is_empty(&self, table: &str, leading_column: &str) -> Result<bool, SqlExecutionError> {
        match self
            .page_handler
            .locate_latest_in_table(table, leading_column)
        {
            Some(descriptor) => Ok(descriptor.entry_count == 0),
            None => Ok(true),
        }
    }

    fn initialise_table_with_row(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        row: &[String],
    ) -> Result<(), SqlExecutionError> {
        for column in columns {
            let descriptor = self
                .page_handler
                .locate_latest_in_table(table, &column.name)
                .or_else(|| {
                    self.page_directory.register_page_in_table_with_sizes(
                        table,
                        &column.name,
                        format!("mem://{table}_{}_page0", column.name),
                        0,
                        0,
                        0,
                        0,
                    )
                })
                .ok_or_else(|| {
                    SqlExecutionError::OperationFailed(format!(
                        "failed to allocate page for {table}.{}",
                        column.name
                    ))
                })?;

            let mut page = Page::new();
            page.page_metadata = descriptor.id.clone();
            page.entries.push(Entry::new(&row[column.ordinal]));
            self.page_handler.write_back_uncompressed(
                &descriptor.id,
                PageCacheEntryUncompressed::from_disk_page(page),
            );
            self.page_handler
                .update_entry_count_in_table(table, &column.name, 1)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }
        Ok(())
    }

    fn locate_rows_by_sort_tuple(
        &self,
        table: &str,
        sort_columns: &[ColumnCatalog],
        key_values: &[String],
    ) -> Result<Vec<u64>, SqlExecutionError> {
        if sort_columns.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let first_column = &sort_columns[0].name;
        let descriptors = self.page_directory.pages_for_in_table(table, first_column);
        let target = &key_values[0];
        let mut base = 0u64;
        let mut continue_on_equal_tail = false;

        for descriptor in descriptors {
            let page = self
                .page_handler
                .get_page(descriptor.clone())
                .ok_or_else(|| SqlExecutionError::OperationFailed("unable to load page".into()))?;
            let disk_page = page.page.as_disk_page();
            let entries = disk_page.entries;
            if entries.is_empty() {
                continue;
            }

            let first_cmp = compare_strs(entries.first().unwrap().get_data(), target);
            let last_cmp = compare_strs(entries.last().unwrap().get_data(), target);

            if first_cmp == Ordering::Greater && !continue_on_equal_tail {
                break;
            }
            if last_cmp == Ordering::Less {
                base += entries.len() as u64;
                continue_on_equal_tail = false;
                continue;
            }

            if let Ok(pos) =
                entries.binary_search_by(|entry| compare_strs(entry.get_data(), target))
            {
                let mut idx = pos;
                while idx > 0
                    && compare_strs(entries[idx - 1].get_data(), target) == Ordering::Equal
                {
                    idx -= 1;
                }
                while idx < entries.len()
                    && compare_strs(entries[idx].get_data(), target) == Ordering::Equal
                {
                    result.push(base + idx as u64);
                    idx += 1;
                }
            }

            base += entries.len() as u64;
            continue_on_equal_tail = last_cmp == Ordering::Equal;
            if last_cmp == Ordering::Greater {
                break;
            }
        }

        if sort_columns.len() == 1 {
            return Ok(result);
        }

        let mut refined = Vec::new();
        'outer: for row_idx in result {
            for (column, expected) in sort_columns.iter().zip(key_values.iter()) {
                let entry = self
                    .page_handler
                    .read_entry_at(table, &column.name, row_idx)
                    .ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "unable to read row {row_idx} for column {table}.{}",
                            column.name
                        ))
                    })?;
                if compare_strs(entry.get_data(), expected) != Ordering::Equal {
                    continue 'outer;
                }
            }
            refined.push(row_idx);
        }

        Ok(refined)
    }

    fn scan_rows_via_full_table(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        match self.scan_and_filter_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
        ) {
            Ok(rows) => Ok(rows),
            Err(SqlExecutionError::Unsupported(_)) => self.scan_rows_rowwise(
                table,
                columns,
                required_ordinals,
                selection_expr,
                column_ordinals,
            ),
            Err(err) => Err(err),
        }
    }

    fn scan_and_filter_vectorized(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        let leading_column = &columns[0].name;
        let lead_pages = self.page_handler.list_pages_in_table(table, leading_column);
        if lead_pages.is_empty() {
            return Ok(Vec::new());
        }

        let mut predicate_columns = BTreeSet::new();
        if let Some(expr) = selection_expr {
            collect_expr_column_names(expr, &mut predicate_columns);
            if predicate_columns.is_empty() {
                return Err(SqlExecutionError::Unsupported(
                    "vectorized scan requires column references in the predicate".into(),
                ));
            }
        }

        let mut descriptor_map: HashMap<String, Vec<PageDescriptor>> = HashMap::new();
        if selection_expr.is_some() {
            for column in &predicate_columns {
                let descriptors = self.page_handler.list_pages_in_table(table, column);
                if descriptors.len() != lead_pages.len() {
                    return Err(SqlExecutionError::Unsupported(
                        "vectorized scan requires aligned page descriptors across columns".into(),
                    ));
                }
                descriptor_map.insert(column.clone(), descriptors);
            }
        }

        let mut base_row = 0u64;
        let mut matching_rows = Vec::new();

        for (idx, descriptor) in lead_pages.iter().enumerate() {
            let page_row_count = descriptor.entry_count as usize;

            if selection_expr.is_none() {
                for local_idx in 0..page_row_count {
                    matching_rows.push(base_row + local_idx as u64);
                }
            } else {
                let mut column_pages: Vec<(String, Arc<PageCacheEntryUncompressed>)> =
                    Vec::with_capacity(predicate_columns.len());

                for column in &predicate_columns {
                    let descriptors = descriptor_map.get(column).ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing page descriptors for column {column}"
                        ))
                    })?;
                    let column_descriptor = descriptors.get(idx).ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "descriptor misalignment for column {column}"
                        ))
                    })?;
                    let page = self
                        .page_handler
                        .get_page(column_descriptor.clone())
                        .ok_or_else(|| {
                            SqlExecutionError::OperationFailed(format!(
                                "unable to load page {} for column {column}",
                                column_descriptor.id
                            ))
                        })?;
                    column_pages.push((column.clone(), page));
                }

                let mut page_refs: HashMap<String, &ColumnarPage> =
                    HashMap::with_capacity(predicate_columns.len());
                for (name, page_arc) in &column_pages {
                    page_refs.insert(name.clone(), &page_arc.page);
                }

                let num_rows = page_refs
                    .values()
                    .next()
                    .map(|page| page.len())
                    .unwrap_or(page_row_count);
                let bitmap = match evaluate_selection_on_page(
                    selection_expr.expect("selection expression is present"),
                    &page_refs,
                ) {
                    Ok(bitmap) => bitmap,
                    Err(SqlExecutionError::Unsupported(_)) => {
                        return Err(SqlExecutionError::Unsupported(
                            "vectorized predicate evaluation not supported for expression".into(),
                        ));
                    }
                    Err(err) => return Err(err),
                };
                drop(column_pages);

                for local_idx in bitmap.ones_indices() {
                    if local_idx < num_rows {
                        matching_rows.push(base_row + local_idx as u64);
                    }
                }
            }

            base_row += descriptor.entry_count;
        }

        Ok(matching_rows)
    }

    fn scan_rows_rowwise(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        let total_rows = self.estimate_table_row_count(table, columns)?;
        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut matching_rows: Vec<u64> = Vec::new();
        let mut offset: u64 = 0;

        while offset < total_rows {
            let upper = (offset + FULL_SCAN_BATCH_SIZE).min(total_rows);
            let rows: Vec<u64> = (offset..upper).collect();
            let materialized =
                materialize_columns(&self.page_handler, table, columns, required_ordinals, &rows)?;

            for row_idx in rows.iter().copied() {
                if selection_expr
                    .map(|expr| {
                        evaluate_selection_expr(expr, row_idx, column_ordinals, &materialized)
                    })
                    .unwrap_or(Ok(true))?
                {
                    matching_rows.push(row_idx);
                }
            }

            offset = upper;
        }

        Ok(matching_rows)
    }

    fn estimate_table_row_count(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
    ) -> Result<u64, SqlExecutionError> {
        for column in columns {
            if let Some(descriptor) = self
                .page_handler
                .locate_latest_in_table(table, &column.name)
            {
                return Ok(descriptor.entry_count);
            }
        }
        Ok(0)
    }
}
