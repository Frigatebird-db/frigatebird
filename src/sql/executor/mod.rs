mod aggregates;
mod expressions;
mod helpers;
mod row_functions;
mod scalar_functions;
mod values;

use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{ColumnCatalog, PageDirectory};
use crate::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::sql::{CreateTablePlan, plan_create_table_statement};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, FromTable, GroupByExpr, ObjectName, OrderByExpr, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::parser::ParserError;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use aggregates::{
    AggregateDataset, AggregateProjectionPlan, MaterializedColumns, evaluate_aggregate_outputs,
    plan_aggregate_projection, select_item_contains_aggregate,
};
use expressions::{evaluate_row_expr, evaluate_selection_expr};
use helpers::{
    collect_expr_column_names, collect_expr_column_ordinals, column_name_from_expr, expr_to_string,
    object_name_matches_table, object_name_to_string, parse_limit, parse_offset,
    table_with_joins_to_name, wildcard_options_supported,
};
use values::{CachedValue, compare_strs, encode_null};

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

enum ProjectionItem {
    Direct { ordinal: usize },
    Computed { expr: Expr },
}

struct GroupByInfo {
    columns: Vec<GroupByColumn>,
}

struct GroupByColumn {
    ordinal: usize,
}

const FULL_SCAN_BATCH_SIZE: u64 = 4_096;

pub struct SqlExecutor {
    page_handler: Arc<PageHandler>,
    page_directory: Arc<PageDirectory>,
}

impl SqlExecutor {
    pub fn new(page_handler: Arc<PageHandler>, page_directory: Arc<PageDirectory>) -> Self {
        Self {
            page_handler,
            page_directory,
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
            || having.is_some()
            || !named_window.is_empty()
            || qualify.is_some()
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

        let order_specs = validate_order_by(&order_by_clauses, &sort_columns, &table_name)?;

        let mut key_values = Vec::with_capacity(sort_columns.len());

        let aggregate_query = projection_items
            .iter()
            .any(|item| select_item_contains_aggregate(item));

        let group_by_info = validate_group_by(
            &group_by,
            aggregate_query,
            &sort_columns,
            &column_ordinals,
            &table_name,
        )?;

        let mut aggregate_plan_opt: Option<AggregateProjectionPlan> = None;
        let mut projection_plan_opt: Option<ProjectionPlan> = None;
        let mut required_ordinals: BTreeSet<usize>;
        let result_columns: Vec<String>;

        if aggregate_query {
            if distinct_flag {
                return Err(SqlExecutionError::Unsupported(
                    "SELECT DISTINCT with aggregates is not supported yet".into(),
                ));
            }
            let plan = plan_aggregate_projection(&projection_items, &column_ordinals, &table_name)?;
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
            required_ordinals = projection_plan.required_ordinals.clone();
            result_columns = projection_plan.headers.clone();
            projection_plan_opt = Some(projection_plan);
        }

        for column in &sort_columns {
            required_ordinals.insert(column.ordinal);
        }

        if has_selection {
            let predicate_ordinals =
                collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
            required_ordinals.extend(predicate_ordinals);
        }

        if let Some(group_info) = &group_by_info {
            for column in &group_info.columns {
                required_ordinals.insert(column.ordinal);
            }
        }

        let sort_key_filters = collect_sort_key_filters(
            if has_selection {
                Some(&selection_expr)
            } else {
                None
            },
            &sort_columns,
        )?;

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
                if has_selection {
                    Some(&selection_expr)
                } else {
                    None
                },
                &column_ordinals,
            )?
        };
        if candidate_rows.is_empty() && !aggregate_query {
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

        if !order_specs.is_empty() {
            let table_name_order = table_name.clone();
            matching_rows.sort_by(|left, right| {
                self.compare_rows(
                    &table_name_order,
                    &columns,
                    &order_specs,
                    &materialized,
                    *left,
                    *right,
                )
            });
        }

        if aggregate_query {
            let aggregate_plan = aggregate_plan_opt.expect("aggregate plan must exist");
            if let Some(group_info) = &group_by_info {
                if matching_rows.is_empty() {
                    return Ok(SelectResult {
                        columns: result_columns,
                        rows: Vec::new(),
                    });
                }

                let mut grouped_rows = Vec::new();
                let mut start = 0;
                while start < matching_rows.len() {
                    let mut end = start + 1;
                    while end < matching_rows.len()
                        && self.rows_share_group(
                            &table_name,
                            &columns,
                            &materialized,
                            group_info,
                            matching_rows[start],
                            matching_rows[end],
                        )?
                    {
                        end += 1;
                    }

                    let group_slice = &matching_rows[start..end];
                    let dataset = AggregateDataset {
                        rows: group_slice,
                        materialized: &materialized,
                        column_ordinals: &column_ordinals,
                    };
                    let output_row = evaluate_aggregate_outputs(&aggregate_plan, &dataset)?;
                    grouped_rows.push(output_row);
                    start = end;
                }

                let offset = parse_offset(offset_expr)?;
                let limit = parse_limit(limit_expr)?;
                let start_idx = offset.min(grouped_rows.len());
                let end_idx = if let Some(limit) = limit {
                    start_idx.saturating_add(limit).min(grouped_rows.len())
                } else {
                    grouped_rows.len()
                };
                let final_rows = grouped_rows[start_idx..end_idx].to_vec();

                return Ok(SelectResult {
                    columns: result_columns,
                    rows: final_rows,
                });
            } else {
                let dataset = AggregateDataset {
                    rows: matching_rows.as_slice(),
                    materialized: &materialized,
                    column_ordinals: &column_ordinals,
                };
                let output_row = evaluate_aggregate_outputs(&aggregate_plan, &dataset)?;
                return Ok(SelectResult {
                    columns: result_columns,
                    rows: vec![output_row],
                });
            }
        }

        if matching_rows.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        let projection_plan = projection_plan_opt.expect("projection plan required");
        let mut rows = Vec::with_capacity(matching_rows.len());
        let dataset_holder;
        let dataset = if projection_plan.needs_dataset() {
            dataset_holder = Some(AggregateDataset {
                rows: matching_rows.as_slice(),
                materialized: &materialized,
                column_ordinals: &column_ordinals,
            });
            dataset_holder.as_ref()
        } else {
            dataset_holder = None;
            None
        };

        for &row_idx in &matching_rows {
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
            self.page_handler
                .write_back_uncompressed(&descriptor.id, PageCacheEntryUncompressed { page });
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
            let entries = &page.page.entries;
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

    fn compare_rows(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        order_specs: &[OrderingSpec],
        materialized: &MaterializedColumns,
        left: u64,
        right: u64,
    ) -> std::cmp::Ordering {
        for spec in order_specs {
            let ordinal = spec.ordinal;
            let column = &columns[ordinal];
            let left_val = self.lookup_value(table, column, ordinal, left, materialized);
            let right_val = self.lookup_value(table, column, ordinal, right, materialized);
            let left_str = match &left_val {
                CachedValue::Null => encode_null(),
                CachedValue::Text(text) => text.clone(),
            };
            let right_str = match &right_val {
                CachedValue::Null => encode_null(),
                CachedValue::Text(text) => text.clone(),
            };
            let mut ord = compare_strs(&left_str, &right_str);
            if spec.desc {
                ord = ord.reverse();
            }
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    fn lookup_value(
        &self,
        table: &str,
        column: &ColumnCatalog,
        ordinal: usize,
        row_idx: u64,
        materialized: &MaterializedColumns,
    ) -> CachedValue {
        if let Some(map) = materialized.get(&ordinal) {
            if let Some(value) = map.get(&row_idx) {
                return value.clone();
            }
        }
        self.page_handler
            .read_entry_at(table, &column.name, row_idx)
            .map(|entry| CachedValue::from_entry(&entry))
            .unwrap_or(CachedValue::Null)
    }

    fn value_for_row(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        materialized: &MaterializedColumns,
        ordinal: usize,
        row_idx: u64,
    ) -> Result<Option<String>, SqlExecutionError> {
        let cached = materialized
            .get(&ordinal)
            .and_then(|column_map| column_map.get(&row_idx))
            .cloned()
            .or_else(|| {
                self.page_handler
                    .read_entry_at(table, &columns[ordinal].name, row_idx)
                    .map(|entry| CachedValue::from_entry(&entry))
            })
            .ok_or_else(|| {
                SqlExecutionError::OperationFailed(format!(
                    "missing value for {table}.{} at row {row_idx}",
                    columns[ordinal].name
                ))
            })?;
        Ok(cached.into_option_string())
    }

    fn rows_share_group(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        materialized: &MaterializedColumns,
        group_info: &GroupByInfo,
        left: u64,
        right: u64,
    ) -> Result<bool, SqlExecutionError> {
        for column in &group_info.columns {
            let left_value =
                self.value_for_row(table, columns, materialized, column.ordinal, left)?;
            let right_value =
                self.value_for_row(table, columns, materialized, column.ordinal, right)?;
            if left_value != right_value {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn validate_order_by(
    clauses: &[OrderByExpr],
    sort_columns: &[ColumnCatalog],
    table_name: &str,
) -> Result<Vec<OrderingSpec>, SqlExecutionError> {
    if clauses.is_empty() {
        return Ok(Vec::new());
    }

    if clauses.len() > sort_columns.len() {
        return Err(SqlExecutionError::Unsupported(format!(
            "ORDER BY on table {table_name} must match the table ORDER BY prefix",
        )));
    }

    let mut specs = Vec::with_capacity(clauses.len());

    for (idx, clause) in clauses.iter().enumerate() {
        if clause.nulls_first.is_some() {
            return Err(SqlExecutionError::Unsupported(
                "ORDER BY with NULLS FIRST/LAST is not supported yet".into(),
            ));
        }

        let column_name = column_name_from_expr(&clause.expr).ok_or_else(|| {
            SqlExecutionError::Unsupported(
                "ORDER BY expressions must be simple column references".into(),
            )
        })?;

        let expected = &sort_columns[idx].name;
        if column_name != *expected {
            return Err(SqlExecutionError::Unsupported(format!(
                "ORDER BY column {} does not match table ORDER BY column {}",
                column_name, expected
            )));
        }

        specs.push(OrderingSpec {
            ordinal: sort_columns[idx].ordinal,
            desc: clause.asc == Some(false),
        });
    }

    // If only first clause specified and DESC, allow additional implicit ascending for trailing
    // ORDER BY columns requested via table default? We'll append remaining columns as ASC to ensure
    // deterministic ordering when LIMIT present.
    for idx in clauses.len()..sort_columns.len() {
        specs.push(OrderingSpec {
            ordinal: sort_columns[idx].ordinal,
            desc: false,
        });
    }

    Ok(specs)
}

fn collect_sort_key_filters(
    expr: Option<&Expr>,
    sort_columns: &[ColumnCatalog],
) -> Result<Option<HashMap<String, String>>, SqlExecutionError> {
    if sort_columns.is_empty() {
        return Ok(Some(HashMap::new()));
    }

    let expr = match expr {
        Some(expr) => expr,
        None => return Ok(None),
    };

    let sort_names: HashSet<&str> = sort_columns.iter().map(|col| col.name.as_str()).collect();
    let mut filters = HashMap::with_capacity(sort_columns.len());
    let compatible = extract_sort_key_filters(expr, &sort_names, &mut filters)?;

    if !compatible {
        return Ok(None);
    }

    for column in sort_columns {
        if !filters.contains_key(&column.name) {
            return Ok(None);
        }
    }

    Ok(Some(filters))
}

fn extract_sort_key_filters(
    expr: &Expr,
    sort_names: &HashSet<&str>,
    filters: &mut HashMap<String, String>,
) -> Result<bool, SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left_ok = extract_sort_key_filters(left, sort_names, filters)?;
                let right_ok = extract_sort_key_filters(right, sort_names, filters)?;
                Ok(left_ok && right_ok)
            }
            BinaryOperator::Eq => {
                if let Some(column_name) = column_name_from_expr(left) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = match expr_to_string(right) {
                            Ok(value) => value,
                            Err(_) => return Ok(false),
                        };
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Ok(false);
                            }
                        } else {
                            filters.insert(column_name.clone(), value);
                        }
                        return Ok(true);
                    }
                }
                if let Some(column_name) = column_name_from_expr(right) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = match expr_to_string(left) {
                            Ok(value) => value,
                            Err(_) => return Ok(false),
                        };
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Ok(false);
                            }
                        } else {
                            filters.insert(column_name.clone(), value);
                        }
                        return Ok(true);
                    }
                }
                Ok(true)
            }
            BinaryOperator::Or | BinaryOperator::Xor => Ok(false),
            _ => {
                if expression_touches_sort(expr, sort_names) {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        },
        Expr::Nested(inner) => extract_sort_key_filters(inner, sort_names, filters),
        _ => {
            if expression_touches_sort(expr, sort_names) {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }
}

fn expression_touches_sort(expr: &Expr, sort_names: &HashSet<&str>) -> bool {
    let mut columns = BTreeSet::new();
    collect_expr_column_names(expr, &mut columns);
    columns
        .iter()
        .any(|name: &String| sort_names.contains(name.as_str()))
}

fn build_projection(
    projection: Vec<SelectItem>,
    table_columns: &[ColumnCatalog],
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    table_alias: Option<&str>,
) -> Result<ProjectionPlan, SqlExecutionError> {
    if projection.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "SELECT requires at least one projection item".into(),
        ));
    }

    let mut iter = projection.into_iter();
    let first = iter.next().expect("projection is not empty");

    match first {
        SelectItem::Wildcard(options) => {
            if iter.next().is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "mixing * with other projection items is not supported".into(),
                ));
            }
            if !wildcard_options_supported(&options) {
                return Err(SqlExecutionError::Unsupported(
                    "wildcard options are not supported".into(),
                ));
            }
            Ok(collect_all_columns(table_columns))
        }
        SelectItem::QualifiedWildcard(object_name, options) => {
            if iter.next().is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "mixing qualified * with other projection items is not supported".into(),
                ));
            }
            if !wildcard_options_supported(&options) {
                return Err(SqlExecutionError::Unsupported(
                    "wildcard options are not supported".into(),
                ));
            }
            if !object_name_matches_table(&object_name, table_name, table_alias) {
                return Err(SqlExecutionError::Unsupported(
                    "qualified wildcard must reference the target table".into(),
                ));
            }
            Ok(collect_all_columns(table_columns))
        }
        item => {
            let mut plan = ProjectionPlan::new();
            push_projection_item(item, column_ordinals, table_name, &mut plan)?;
            for item in iter {
                push_projection_item(item, column_ordinals, table_name, &mut plan)?;
            }
            Ok(plan)
        }
    }
}

fn push_projection_item(
    item: SelectItem,
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    plan: &mut ProjectionPlan,
) -> Result<(), SqlExecutionError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            push_expression_item(expr, None, column_ordinals, table_name, plan)
        }
        SelectItem::ExprWithAlias { expr, alias } => push_expression_item(
            expr,
            Some(alias.value.clone()),
            column_ordinals,
            table_name,
            plan,
        ),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            Err(SqlExecutionError::Unsupported(
                "wildcard projection must be the only projection item".into(),
            ))
        }
    }
}

fn push_expression_item(
    expr: Expr,
    alias: Option<String>,
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    plan: &mut ProjectionPlan,
) -> Result<(), SqlExecutionError> {
    if let Some(column_name) = column_name_from_expr(&expr) {
        if let Some(&ordinal) = column_ordinals.get(&column_name) {
            let header = alias.unwrap_or_else(|| column_name.clone());
            plan.headers.push(header);
            plan.items.push(ProjectionItem::Direct { ordinal });
            plan.required_ordinals.insert(ordinal);
            return Ok(());
        }
    }

    let ordinals = collect_expr_column_ordinals(&expr, column_ordinals, table_name)?;
    plan.required_ordinals.extend(ordinals.iter().copied());
    let header = alias.unwrap_or_else(|| expr.to_string());
    plan.headers.push(header);
    plan.items.push(ProjectionItem::Computed { expr });
    Ok(())
}

fn collect_all_columns(table_columns: &[ColumnCatalog]) -> ProjectionPlan {
    let mut plan = ProjectionPlan::new();
    for column in table_columns {
        plan.headers.push(column.name.clone());
        plan.items.push(ProjectionItem::Direct {
            ordinal: column.ordinal,
        });
        plan.required_ordinals.insert(column.ordinal);
    }
    plan
}

fn materialize_columns(
    page_handler: &PageHandler,
    table: &str,
    table_columns: &[ColumnCatalog],
    ordinals: &BTreeSet<usize>,
    rows: &[u64],
) -> Result<MaterializedColumns, SqlExecutionError> {
    let mut result: MaterializedColumns = HashMap::with_capacity(ordinals.len());
    if ordinals.is_empty() || rows.is_empty() {
        return Ok(result);
    }

    let start_row = *rows.first().expect("rows not empty");
    let end_row = *rows.last().expect("rows not empty");

    for &ordinal in ordinals {
        let column = table_columns.get(ordinal).ok_or_else(|| {
            SqlExecutionError::OperationFailed(format!(
                "invalid column ordinal {ordinal} on table {table}"
            ))
        })?;

        let slices = page_handler.list_range_in_table(table, &column.name, start_row, end_row);
        if slices.is_empty() {
            result.insert(ordinal, HashMap::new());
            continue;
        }

        let descriptors = slices
            .iter()
            .map(|slice| slice.descriptor.clone())
            .collect::<Vec<_>>();
        let pages = page_handler.get_pages(descriptors);

        let mut page_map: HashMap<String, Arc<PageCacheEntryUncompressed>> =
            HashMap::with_capacity(pages.len());
        for page in pages {
            page_map.insert(page.page.page_metadata.clone(), page);
        }

        let mut values: HashMap<u64, CachedValue> = HashMap::with_capacity(rows.len());
        let mut row_iter = rows.iter().copied().peekable();
        let mut current_row = start_row;

        'outer: for slice in slices {
            if row_iter.peek().is_none() {
                break;
            }

            let page = page_map.get(&slice.descriptor.id).ok_or_else(|| {
                SqlExecutionError::OperationFailed(format!(
                    "missing page {} for column {}",
                    slice.descriptor.id, column.name
                ))
            })?;
            let entries = &page.page.entries;

            let start = slice.start_row_offset as usize;
            let end = slice.end_row_offset.min(entries.len() as u64) as usize;

            for idx in start..end {
                while let Some(&target) = row_iter.peek() {
                    if target < current_row {
                        row_iter.next();
                    } else {
                        break;
                    }
                }

                match row_iter.peek().copied() {
                    Some(target) if target == current_row => {
                        if let Some(entry) = entries.get(idx) {
                            values.insert(target, CachedValue::from_entry(entry));
                        }
                        row_iter.next();
                    }
                    Some(_) => {}
                    None => break 'outer,
                }

                current_row = current_row.saturating_add(1);
            }
        }

        result.insert(ordinal, values);
    }

    Ok(result)
}

fn validate_group_by(
    group_by: &GroupByExpr,
    aggregate_query: bool,
    sort_columns: &[ColumnCatalog],
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
) -> Result<Option<GroupByInfo>, SqlExecutionError> {
    let GroupByExpr::Expressions(exprs) = group_by else {
        return Ok(None);
    };

    if exprs.is_empty() {
        return Ok(None);
    }

    if !aggregate_query {
        return Err(SqlExecutionError::Unsupported(
            "GROUP BY requires aggregate projections".into(),
        ));
    }

    if exprs.len() > sort_columns.len() {
        return Err(SqlExecutionError::Unsupported(format!(
            "GROUP BY columns must be a prefix of the ORDER BY columns on table {table_name}",
        )));
    }

    let mut columns = Vec::with_capacity(exprs.len());
    for (idx, expr) in exprs.iter().enumerate() {
        let column_name = column_name_from_expr(expr).ok_or_else(|| {
            SqlExecutionError::Unsupported("GROUP BY requires simple column references".into())
        })?;

        let expected = &sort_columns[idx].name;
        if column_name != *expected {
            return Err(SqlExecutionError::Unsupported(
                "GROUP BY columns must match the leading ORDER BY columns".into(),
            ));
        }

        let ordinal = column_ordinals.get(&column_name).copied().ok_or_else(|| {
            SqlExecutionError::ColumnMismatch {
                table: table_name.to_string(),
                column: column_name.clone(),
            }
        })?;

        columns.push(GroupByColumn { ordinal });
    }

    Ok(Some(GroupByInfo { columns }))
}
#[derive(Clone)]
struct OrderingSpec {
    ordinal: usize,
    desc: bool,
}
