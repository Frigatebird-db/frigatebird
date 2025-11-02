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
    Assignment, BinaryOperator, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    GroupByExpr, ObjectName, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, UnaryOperator, Value, WildcardAdditionalOptions,
};
use sqlparser::parser::ParserError;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

const NULL_SENTINEL: &str = "\u{0001}";

fn encode_null() -> String {
    NULL_SENTINEL.to_string()
}

fn is_encoded_null(value: &str) -> bool {
    value == NULL_SENTINEL
}

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

#[derive(Debug, Clone)]
enum CachedValue {
    Null,
    Text(String),
}

impl CachedValue {
    fn from_entry(entry: &Entry) -> Self {
        let data = entry.get_data();
        if is_encoded_null(data) {
            CachedValue::Null
        } else {
            CachedValue::Text(data.to_string())
        }
    }

    fn into_option_string(self) -> Option<String> {
        match self {
            CachedValue::Null => None,
            CachedValue::Text(text) => Some(text),
        }
    }
}

fn cached_to_scalar(value: &CachedValue) -> ScalarValue {
    match value {
        CachedValue::Null => ScalarValue::Null,
        CachedValue::Text(text) => ScalarValue::Text(text.clone()),
    }
}

#[derive(Debug, Clone)]
enum ScalarValue {
    Null,
    Int(i128),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl ScalarValue {
    fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(*value as f64),
            ScalarValue::Float(value) => Some(*value),
            ScalarValue::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
            ScalarValue::Text(text) => text.parse::<f64>().ok(),
        }
    }

    fn as_i128(&self) -> Option<i128> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(*value),
            ScalarValue::Float(value) => Some(*value as i128),
            ScalarValue::Bool(value) => Some(if *value { 1 } else { 0 }),
            ScalarValue::Text(text) => text.parse::<i128>().ok(),
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Bool(value) => Some(*value),
            ScalarValue::Int(value) => Some(*value != 0),
            ScalarValue::Float(value) => Some(*value != 0.0),
            ScalarValue::Text(text) => match text.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "y" => Some(true),
                "false" | "f" | "0" | "no" | "n" => Some(false),
                _ => None,
            },
        }
    }

    fn into_option_string(self) -> Option<String> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(value.to_string()),
            ScalarValue::Float(value) => Some(format_float(value)),
            ScalarValue::Text(text) => Some(text),
            ScalarValue::Bool(value) => Some(if value { "true".into() } else { "false".into() }),
        }
    }
}

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
            || !query.order_by.is_empty()
            || !query.limit_by.is_empty()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
        {
            return Err(SqlExecutionError::Unsupported(
                "SELECT with advanced clauses is not supported".into(),
            ));
        }

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

        if distinct.is_some()
            || top.is_some()
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

        match group_by {
            GroupByExpr::All => {}
            GroupByExpr::Expressions(exprs) => {
                if !exprs.is_empty() {
                    return Err(SqlExecutionError::Unsupported(
                        "GROUP BY is not supported".into(),
                    ));
                }
            }
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

        let selection = selection.ok_or_else(|| {
            SqlExecutionError::Unsupported(
                "SELECT requires WHERE with equality predicates on ORDER BY columns".into(),
            )
        })?;
        let selection_expr = selection;

        let sort_columns_refs = catalog.sort_key();
        if sort_columns_refs.is_empty() {
            return Err(SqlExecutionError::Unsupported(
                "SELECT currently requires ORDER BY tables".into(),
            ));
        }
        let sort_columns: Vec<ColumnCatalog> = sort_columns_refs.into_iter().cloned().collect();

        let sort_key_filters = collect_sort_key_filters(&selection_expr, &sort_columns)?;
        let mut key_values = Vec::with_capacity(sort_columns.len());
        for column in &sort_columns {
            key_values.push(sort_key_filters.get(&column.name).cloned().ok_or_else(|| {
                SqlExecutionError::Unsupported(format!(
                    "SELECT requires equality predicate for ORDER BY column {}",
                    column.name
                ))
            })?);
        }

        let aggregate_query = projection_items
            .iter()
            .any(|item| select_item_contains_aggregate(item));

        if aggregate_query && (limit_expr.is_some() || offset_expr.is_some()) {
            return Err(SqlExecutionError::Unsupported(
                "aggregate SELECT does not support LIMIT/OFFSET".into(),
            ));
        }

        let mut aggregate_plan_opt: Option<AggregateProjectionPlan> = None;
        let mut projection_ordinals_opt: Option<Vec<usize>> = None;
        let mut required_ordinals: BTreeSet<usize>;
        let result_columns: Vec<String>;

        if aggregate_query {
            let plan = plan_aggregate_projection(&projection_items, &column_ordinals, &table_name)?;
            required_ordinals = plan.required_ordinals.clone();
            result_columns = plan.headers.clone();
            aggregate_plan_opt = Some(plan);
        } else {
            let (cols, projection_ordinals_vec) = build_projection(
                projection_items.clone(),
                &columns,
                &column_ordinals,
                &table_name,
                table_alias.as_deref(),
            )?;
            required_ordinals = projection_ordinals_vec.iter().copied().collect();
            result_columns = cols;
            projection_ordinals_opt = Some(projection_ordinals_vec);
        }

        for column in &sort_columns {
            required_ordinals.insert(column.ordinal);
        }

        let predicate_ordinals =
            collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
        required_ordinals.extend(predicate_ordinals);

        println!("required ordinals: {:?}", required_ordinals);

        let mut candidate_rows =
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?;
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
            if evaluate_selection_expr(&selection_expr, row_idx, &column_ordinals, &materialized)? {
                matching_rows.push(row_idx);
            }
        }

        if aggregate_query {
            let dataset = AggregateDataset {
                rows: matching_rows.as_slice(),
                materialized: &materialized,
                column_ordinals: &column_ordinals,
            };
            let aggregate_plan = aggregate_plan_opt.expect("aggregate plan must exist");
            let output_row = evaluate_aggregate_outputs(&aggregate_plan, &dataset)?;
            return Ok(SelectResult {
                columns: result_columns,
                rows: vec![output_row],
            });
        }

        if matching_rows.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        let start = offset.min(matching_rows.len());
        let end = if let Some(limit) = limit {
            start.saturating_add(limit).min(matching_rows.len())
        } else {
            matching_rows.len()
        };
        let window = &matching_rows[start..end];

        let projection_ordinals = projection_ordinals_opt.expect("projection ordinals required");

        let mut rows = Vec::with_capacity(window.len());
        for &row_idx in window {
            let mut projected = Vec::with_capacity(projection_ordinals.len());
            for &ordinal in &projection_ordinals {
                let cached = materialized
                    .get(&ordinal)
                    .and_then(|column_map| column_map.get(&row_idx))
                    .cloned()
                    .or_else(|| {
                        self.page_handler
                            .read_entry_at(&table_name, &columns[ordinal].name, row_idx)
                            .map(|entry| CachedValue::from_entry(&entry))
                    })
                    .ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing value for {table_name}.{} at row {row_idx}",
                            columns[ordinal].name
                        ))
                    })?;
                projected.push(cached.into_option_string());
            }
            rows.push(projected);
        }

        Ok(SelectResult {
            columns: result_columns,
            rows,
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

        let selection = selection.ok_or_else(|| {
            SqlExecutionError::Unsupported(
                "UPDATE requires WHERE with equality filters on ORDER BY columns".into(),
            )
        })?;

        let filters = extract_equality_filters(&selection)?;
        for column in filters.keys() {
            if !column_ordinals.contains_key(column) {
                return Err(SqlExecutionError::ColumnMismatch {
                    table: table_name.clone(),
                    column: column.clone(),
                });
            }
        }

        let mut key_values = Vec::with_capacity(sort_columns.len());
        for column in &sort_columns {
            let value = filters.get(&column.name).cloned().ok_or_else(|| {
                SqlExecutionError::Unsupported(format!(
                    "UPDATE requires equality predicate for ORDER BY column {}",
                    column.name
                ))
            })?;
            key_values.push(value);
        }

        let candidate_rows =
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?;
        if candidate_rows.is_empty() {
            return Ok(());
        }

        let mut matching_rows = Vec::new();
        for row_idx in candidate_rows {
            if self.row_matches_filters(&table_name, &columns, row_idx, &filters)? {
                matching_rows.push(row_idx);
            }
        }

        if matching_rows.is_empty() {
            return Ok(());
        }
        if matching_rows.len() > 1 {
            return Err(SqlExecutionError::Unsupported(
                "UPDATE matched multiple rows; refine WHERE clause".into(),
            ));
        }
        let row_idx = matching_rows[0];

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

        let selection = selection.ok_or_else(|| {
            SqlExecutionError::Unsupported(
                "DELETE requires WHERE with equality predicates on ORDER BY columns".into(),
            )
        })?;

        let filters = extract_equality_filters(&selection)?;

        let catalog = self
            .page_directory
            .table_catalog(&table_name)
            .ok_or_else(|| SqlExecutionError::TableNotFound(table_name.clone()))?;
        let columns: Vec<ColumnCatalog> = catalog.columns().to_vec();
        let mut column_ordinals: HashMap<String, usize> = HashMap::new();
        for column in &columns {
            column_ordinals.insert(column.name.clone(), column.ordinal);
        }

        for column in filters.keys() {
            if !column_ordinals.contains_key(column) {
                return Err(SqlExecutionError::ColumnMismatch {
                    table: table_name.clone(),
                    column: column.clone(),
                });
            }
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

        let mut key_values = Vec::with_capacity(sort_columns.len());
        for column in &sort_columns {
            let value = filters.get(&column.name).cloned().ok_or_else(|| {
                SqlExecutionError::Unsupported(format!(
                    "DELETE requires equality predicate for ORDER BY column {}",
                    column.name
                ))
            })?;
            key_values.push(value);
        }

        let candidate_rows =
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?;
        if candidate_rows.is_empty() {
            return Ok(());
        }

        let mut matching_rows = Vec::new();
        for row_idx in candidate_rows {
            if self.row_matches_filters(&table_name, &columns, row_idx, &filters)? {
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

    fn row_matches_filters(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        row_idx: u64,
        filters: &HashMap<String, String>,
    ) -> Result<bool, SqlExecutionError> {
        if filters.is_empty() {
            return Ok(true);
        }
        let row = read_row(&self.page_handler, table, row_idx)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        for column in columns {
            if let Some(expected) = filters.get(&column.name) {
                if compare_strs(&row[column.ordinal], expected) != Ordering::Equal {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

type MaterializedColumns = HashMap<usize, HashMap<u64, CachedValue>>;

fn collect_sort_key_filters(
    expr: &Expr,
    sort_columns: &[ColumnCatalog],
) -> Result<HashMap<String, String>, SqlExecutionError> {
    if sort_columns.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sort_names: HashSet<&str> = HashSet::with_capacity(sort_columns.len());
    for column in sort_columns {
        sort_names.insert(column.name.as_str());
    }

    let mut filters = HashMap::with_capacity(sort_columns.len());
    gather_sort_key_filters(expr, &sort_names, &mut filters)?;

    for column in sort_columns {
        if !filters.contains_key(&column.name) {
            return Err(SqlExecutionError::Unsupported(format!(
                "SELECT requires equality predicate for ORDER BY column {}",
                column.name
            )));
        }
    }

    Ok(filters)
}

fn gather_sort_key_filters(
    expr: &Expr,
    sort_names: &HashSet<&str>,
    filters: &mut HashMap<String, String>,
) -> Result<(), SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                gather_sort_key_filters(left, sort_names, filters)?;
                gather_sort_key_filters(right, sort_names, filters)
            }
            BinaryOperator::Eq => {
                if let Some(column_name) = column_name_from_expr(left) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = expr_to_string(right)?;
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Err(SqlExecutionError::Unsupported(format!(
                                    "conflicting predicates for column {column_name}"
                                )));
                            }
                        } else {
                            filters.insert(column_name, value);
                        }
                        return Ok(());
                    }
                }
                if let Some(column_name) = column_name_from_expr(right) {
                    if sort_names.contains(column_name.as_str()) {
                        let value = expr_to_string(left)?;
                        if let Some(existing) = filters.get(&column_name) {
                            if compare_strs(existing, &value) != Ordering::Equal {
                                return Err(SqlExecutionError::Unsupported(format!(
                                    "conflicting predicates for column {column_name}"
                                )));
                            }
                        } else {
                            filters.insert(column_name, value);
                        }
                        return Ok(());
                    }
                }
                Ok(())
            }
            BinaryOperator::Or | BinaryOperator::Xor => Err(SqlExecutionError::Unsupported(
                "SELECT filters with OR are not supported for ORDER BY lookups".into(),
            )),
            _ => Ok(()),
        },
        Expr::Nested(inner) => gather_sort_key_filters(inner, sort_names, filters),
        _ => Ok(()),
    }
}

fn collect_expr_column_ordinals(
    expr: &Expr,
    column_ordinals: &HashMap<String, usize>,
    table: &str,
) -> Result<BTreeSet<usize>, SqlExecutionError> {
    let mut names = BTreeSet::new();
    collect_expr_column_names(expr, &mut names);

    let mut ordinals = BTreeSet::new();
    for name in names {
        let ordinal = column_ordinals.get(&name).copied().ok_or_else(|| {
            SqlExecutionError::ColumnMismatch {
                table: table.to_string(),
                column: name.clone(),
            }
        })?;
        ordinals.insert(ordinal);
    }

    Ok(ordinals)
}

fn collect_expr_column_names(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.insert(ident.value.clone());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.insert(last.value.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_column_names(left, columns);
            collect_expr_column_names(right, columns);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_expr_column_names(expr, columns);
            collect_expr_column_names(pattern, columns);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_column_names(expr, columns);
            collect_expr_column_names(low, columns);
            collect_expr_column_names(high, columns);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_column_names(expr, columns);
            for item in list {
                collect_expr_column_names(item, columns);
            }
        }
        Expr::UnaryOp { expr, .. } => collect_expr_column_names(expr, columns),
        Expr::Nested(inner) => collect_expr_column_names(inner, columns),
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::SafeCast { expr: inner, .. }
        | Expr::TryCast { expr: inner, .. }
        | Expr::Convert { expr: inner, .. }
        | Expr::Extract { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::Ceil { expr: inner, .. }
        | Expr::Floor { expr: inner, .. }
        | Expr::AtTimeZone {
            timestamp: inner, ..
        } => collect_expr_column_names(inner, columns),
        Expr::JsonAccess { left, right, .. } => {
            collect_expr_column_names(left, columns);
            collect_expr_column_names(right, columns);
        }
        Expr::Function(function) => {
            for function_arg in &function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            collect_expr_column_names(expr, columns);
                        }
                    }
                }
            }
            if let Some(filter) = &function.filter {
                collect_expr_column_names(filter, columns);
            }
            for order in &function.order_by {
                collect_expr_column_names(&order.expr, columns);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_expr_column_names(op, columns);
            }
            for condition in conditions {
                collect_expr_column_names(condition, columns);
            }
            for result in results {
                collect_expr_column_names(result, columns);
            }
            if let Some(else_expr) = else_result {
                collect_expr_column_names(else_expr, columns);
            }
        }
        _ => {}
    }
}

fn select_item_contains_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_contains_aggregate(expr),
        SelectItem::ExprWithAlias { expr, .. } => expr_contains_aggregate(expr),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => {
            if is_aggregate_function(function) {
                return true;
            }
            function.args.iter().any(|arg| match arg {
                FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => match arg {
                    FunctionArgExpr::Expr(expr) => expr_contains_aggregate(expr),
                    FunctionArgExpr::QualifiedWildcard(_) => false,
                    FunctionArgExpr::Wildcard => false,
                },
            }) || function
                .order_by
                .iter()
                .any(|order| expr_contains_aggregate(&order.expr))
                || function
                    .filter
                    .as_ref()
                    .map(|filter| expr_contains_aggregate(filter.as_ref()))
                    .unwrap_or(false)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => expr_contains_aggregate(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|expr| expr_contains_aggregate(expr))
                .unwrap_or(false)
                || conditions.iter().any(expr_contains_aggregate)
                || results.iter().any(expr_contains_aggregate)
                || else_result
                    .as_ref()
                    .map(|expr| expr_contains_aggregate(expr))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::Exists { .. } | Expr::Subquery(_) => false,
        _ => false,
    }
}

fn is_aggregate_function(function: &Function) -> bool {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    matches!(
        name.as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "VARIANCE_POP"
            | "VARIANCE_SAMP"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "PERCENTILE_CONT"
    )
}

struct AggregateProjectionPlan {
    outputs: Vec<AggregateProjection>,
    required_ordinals: BTreeSet<usize>,
    headers: Vec<String>,
}

struct AggregateProjection {
    expr: Expr,
}

struct AggregateDataset<'a> {
    rows: &'a [u64],
    materialized: &'a MaterializedColumns,
    column_ordinals: &'a HashMap<String, usize>,
}

impl<'a> AggregateDataset<'a> {
    fn column_value(&self, column: &str, row_idx: u64) -> Option<&CachedValue> {
        self.column_ordinals
            .get(column)
            .and_then(|ordinal| self.materialized.get(ordinal))
            .and_then(|map| map.get(&row_idx))
    }
}

fn plan_aggregate_projection(
    items: &[SelectItem],
    column_ordinals: &HashMap<String, usize>,
    table: &str,
) -> Result<AggregateProjectionPlan, SqlExecutionError> {
    let mut outputs = Vec::with_capacity(items.len());
    let mut headers = Vec::with_capacity(items.len());
    let mut required_ordinals = BTreeSet::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(SqlExecutionError::Unsupported(
                    "aggregate SELECT does not support wildcard projections".into(),
                ));
            }
            SelectItem::UnnamedExpr(expr) => {
                let label = expr.to_string();
                let expr_clone = expr.clone();
                let mut ordinals =
                    collect_expr_column_ordinals(&expr_clone, column_ordinals, table)?;
                collect_function_order_ordinals(
                    &expr_clone,
                    column_ordinals,
                    table,
                    &mut ordinals,
                )?;
                required_ordinals.extend(ordinals.iter().copied());
                headers.push(label);
                outputs.push(AggregateProjection { expr: expr_clone });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let label = alias.value.clone();
                let expr_clone = expr.clone();
                let mut ordinals =
                    collect_expr_column_ordinals(&expr_clone, column_ordinals, table)?;
                collect_function_order_ordinals(
                    &expr_clone,
                    column_ordinals,
                    table,
                    &mut ordinals,
                )?;
                required_ordinals.extend(ordinals.iter().copied());
                headers.push(label);
                outputs.push(AggregateProjection { expr: expr_clone });
            }
        }
    }

    Ok(AggregateProjectionPlan {
        outputs,
        required_ordinals,
        headers,
    })
}

fn collect_function_order_ordinals(
    expr: &Expr,
    column_ordinals: &HashMap<String, usize>,
    table: &str,
    out: &mut BTreeSet<usize>,
) -> Result<(), SqlExecutionError> {
    match expr {
        Expr::Function(function) => {
            for order in &function.order_by {
                let ordinals = collect_expr_column_ordinals(&order.expr, column_ordinals, table)?;
                out.extend(ordinals);
            }
            if let Some(filter) = &function.filter {
                let ordinals = collect_expr_column_ordinals(filter, column_ordinals, table)?;
                out.extend(ordinals);
                collect_function_order_ordinals(filter, column_ordinals, table, out)?;
            }
            for function_arg in &function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            collect_function_order_ordinals(inner, column_ordinals, table, out)?;
                        }
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_function_order_ordinals(left, column_ordinals, table, out)?;
            collect_function_order_ordinals(right, column_ordinals, table, out)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            collect_function_order_ordinals(low, column_ordinals, table, out)?;
            collect_function_order_ordinals(high, column_ordinals, table, out)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            for item in list {
                collect_function_order_ordinals(item, column_ordinals, table, out)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_function_order_ordinals(operand, column_ordinals, table, out)?;
            }
            for cond in conditions {
                collect_function_order_ordinals(cond, column_ordinals, table, out)?;
            }
            for res in results {
                collect_function_order_ordinals(res, column_ordinals, table, out)?;
            }
            if let Some(else_res) = else_result {
                collect_function_order_ordinals(else_res, column_ordinals, table, out)?;
            }
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_function_order_ordinals(expr, column_ordinals, table, out)?;
            collect_function_order_ordinals(pattern, column_ordinals, table, out)?;
        }
        _ => {}
    }

    Ok(())
}

fn evaluate_aggregate_outputs(
    plan: &AggregateProjectionPlan,
    dataset: &AggregateDataset,
) -> Result<Vec<Option<String>>, SqlExecutionError> {
    let mut row = Vec::with_capacity(plan.outputs.len());
    for output in &plan.outputs {
        let value = evaluate_scalar_expression(&output.expr, dataset)?;
        row.push(value.into_option_string());
    }
    Ok(row)
}

fn evaluate_scalar_expression(
    expr: &Expr,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
        Expr::Value(Value::Number(n, _)) => Ok(match n.parse::<f64>() {
            Ok(num) => ScalarValue::Float(num),
            Err(_) => ScalarValue::Text(n.clone()),
        }),
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::Function(function) => {
            if is_aggregate_function(function) {
                evaluate_aggregate_function(function, dataset)
            } else {
                evaluate_scalar_function(function, dataset)
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = evaluate_scalar_expression(left, dataset)?;
            let rhs = evaluate_scalar_expression(right, dataset)?;
            match op {
                BinaryOperator::Plus => combine_numeric(&lhs, &rhs, |a, b| a + b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric addition".into())),
                BinaryOperator::Minus => {
                    combine_numeric(&lhs, &rhs, |a, b| a - b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric subtraction".into())
                    })
                }
                BinaryOperator::Multiply => {
                    combine_numeric(&lhs, &rhs, |a, b| a * b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric multiplication".into())
                    })
                }
                BinaryOperator::Divide => combine_numeric(&lhs, &rhs, |a, b| a / b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric division".into())),
                BinaryOperator::Modulo => combine_numeric(&lhs, &rhs, |a, b| a % b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric modulo".into())),
                BinaryOperator::And => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a && b))
                }
                BinaryOperator::Or => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a || b))
                }
                BinaryOperator::Xor => {
                    let a = lhs.as_bool().unwrap_or(false);
                    let b = rhs.as_bool().unwrap_or(false);
                    Ok(ScalarValue::Bool(a ^ b))
                }
                BinaryOperator::Eq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = match compare_scalar_values(&lhs, &rhs) {
                        Some(ord) => ord == Ordering::Greater,
                        None => false,
                    };
                    println!(
                        "row compare {:?} > {:?} => {}",
                        lhs,
                        rhs,
                        result
                    );
                    Ok(ScalarValue::Bool(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported operator in aggregate expression: {op:?}"
                ))),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => {
                    let num = value.as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "unsupported unary minus on non-numeric value".into(),
                        )
                    })?;
                    Ok(scalar_from_f64(-num))
                }
                UnaryOperator::Not => Ok(ScalarValue::Bool(!value.as_bool().unwrap_or(false))),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported unary operator {op:?}"
                ))),
            }
        }
        Expr::Nested(inner) => evaluate_scalar_expression(inner, dataset),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case_expression(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            dataset,
        ),
        Expr::Like {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, true),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::ILike {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, false),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = evaluate_scalar_expression(expr, dataset)?;
            let pattern_value = evaluate_scalar_expression(pattern, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => regex_match(&value, &pattern),
                _ => false,
            };
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = evaluate_scalar_expression(expr, dataset)?;
            let low_value = evaluate_scalar_expression(low, dataset)?;
            let high_value = evaluate_scalar_expression(high, dataset)?;
            let cmp_low = compare_scalar_values(&target, &low_value).unwrap_or(Ordering::Less);
            let cmp_high = compare_scalar_values(&target, &high_value).unwrap_or(Ordering::Greater);
            let between = (cmp_low == Ordering::Greater || cmp_low == Ordering::Equal)
                && (cmp_high == Ordering::Less || cmp_high == Ordering::Equal);
            Ok(ScalarValue::Bool(if *negated { !between } else { between }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = evaluate_scalar_expression(expr, dataset)?;
            let mut matches = false;
            for item in list {
                let candidate = evaluate_scalar_expression(item, dataset)?;
                if compare_scalar_values(&target, &candidate)
                    .map(|ord| ord == Ordering::Equal)
                    .unwrap_or(false)
                {
                    matches = true;
                    break;
                }
            }
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(SqlExecutionError::Unsupported(
            "aggregated SELECT cannot project plain columns".into(),
        )),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported expression in aggregate projection: {expr:?}"
        ))),
    }
}

fn evaluate_aggregate_function(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    match name.as_str() {
        "COUNT" => evaluate_count(function, dataset),
        "SUM" | "AVG" | "MIN" | "MAX" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" | "VARIANCE_POP"
        | "VARIANCE_SAMP" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" => {
            evaluate_numeric_aggregate(name.as_str(), function, dataset)
        }
        "PERCENTILE_CONT" => evaluate_percentile_cont(function, dataset),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported aggregate function {name}"
        ))),
    }
}

fn evaluate_count(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.is_empty()
        || matches!(
            function.args.get(0),
            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
        )
    {
        return Ok(ScalarValue::Int(dataset.rows.len() as i128));
    }

    let expr = extract_single_argument(function)?;
    if function.distinct {
        let mut set: HashSet<String> = HashSet::new();
        for &row in dataset.rows {
            let value = evaluate_row_expr(expr, row, dataset)?;
            if let Some(text) = value.into_option_string() {
                set.insert(text);
            }
        }
        Ok(ScalarValue::Int(set.len() as i128))
    } else {
        let mut count: i128 = 0;
        for &row in dataset.rows {
            let value = evaluate_row_expr(expr, row, dataset)?;
            if !value.is_null() {
                count += 1;
            }
        }
        Ok(ScalarValue::Int(count))
    }
}

fn evaluate_numeric_aggregate(
    name: &str,
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let expr = extract_single_argument(function)?;
    let mut count: i128 = 0;
    let mut sum = 0.0;
    let mut min_value: Option<f64> = None;
    let mut max_value: Option<f64> = None;
    let mut mean = 0.0;
    let mut m2 = 0.0;

    println!("dataset rows: {:?}", dataset.rows);
    println!("materialized snapshot: {:?}", dataset.materialized);
    for &row in dataset.rows {
        let value = evaluate_row_expr(expr, row, dataset)?;
        println!("aggregate row value: {:?}", value);
        if let Some(num) = value.as_f64() {
            min_value = Some(min_value.map(|m| m.min(num)).unwrap_or(num));
            max_value = Some(max_value.map(|m| m.max(num)).unwrap_or(num));
            count += 1;
            sum += num;

            let delta = num - mean;
            mean += delta / count as f64;
            let delta2 = num - mean;
            m2 += delta * delta2;
        }
    }

    match name {
        "SUM" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                Ok(scalar_from_f64(sum))
            }
        }
        "AVG" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                Ok(scalar_from_f64(sum / count as f64))
            }
        }
        "MIN" => Ok(min_value.map(scalar_from_f64).unwrap_or(ScalarValue::Null)),
        "MAX" => Ok(max_value.map(scalar_from_f64).unwrap_or(ScalarValue::Null)),
        "VARIANCE" | "VAR_POP" | "VARIANCE_POP" | "STDDEV" | "STDDEV_POP" => {
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                let variance = m2 / count as f64;
                if name.starts_with("STDDEV") {
                    Ok(scalar_from_f64(variance.sqrt()))
                } else {
                    Ok(scalar_from_f64(variance))
                }
            }
        }
        "VAR_SAMP" | "VARIANCE_SAMP" | "STDDEV_SAMP" => {
            if count <= 1 {
                Ok(ScalarValue::Null)
            } else {
                let variance = m2 / (count as f64 - 1.0);
                if name.starts_with("STDDEV") {
                    Ok(scalar_from_f64(variance.sqrt()))
                } else {
                    Ok(scalar_from_f64(variance))
                }
            }
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported numeric aggregate {name}"
        ))),
    }
}

fn evaluate_percentile_cont(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if function.args.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont requires at least one argument".into(),
        ));
    }

    let percent_expr = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => {
            return Err(SqlExecutionError::Unsupported(
                "percentile_cont requires literal percentile argument".into(),
            ))
        }
    };

    let percent = evaluate_scalar_expression(percent_expr, dataset)?
        .as_f64()
        .ok_or_else(|| {
            SqlExecutionError::Unsupported("percentile_cont requires numeric percentile".into())
        })?;
    if !(0.0..=1.0).contains(&percent) {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont percentile must be between 0 and 1".into(),
        ));
    }

    let order_expr = if let Some(order) = function.order_by.first() {
        &order.expr
    } else if function.args.len() >= 2 {
        match &function.args[1] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => expr,
            _ => {
                return Err(SqlExecutionError::Unsupported(
                    "percentile_cont requires an expression to order".into(),
                ))
            }
        }
    } else {
        return Err(SqlExecutionError::Unsupported(
            "percentile_cont requires an ORDER BY expression".into(),
        ));
    };
    let mut values: Vec<f64> = Vec::with_capacity(dataset.rows.len());
    for &row in dataset.rows {
        let value = evaluate_row_expr(order_expr, row, dataset)?;
        if let Some(num) = value.as_f64() {
            values.push(num);
        }
    }

    if values.is_empty() {
        return Ok(ScalarValue::Null);
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let len = values.len() as f64;
    let position = percent * (len - 1.0);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;

    if lower == upper {
        Ok(scalar_from_f64(values[lower]))
    } else {
        let lower_value = values[lower];
        let upper_value = values[upper];
        let fraction = position - lower as f64;
        Ok(scalar_from_f64(
            lower_value + (upper_value - lower_value) * fraction,
        ))
    }
}

fn extract_single_argument<'a>(function: &'a Function) -> Result<&'a Expr, SqlExecutionError> {
    if function.args.len() != 1 {
        return Err(SqlExecutionError::Unsupported(format!(
            "function {} requires exactly one argument",
            function.name
        )));
    }
    match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => Ok(expr),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported argument for function {}",
            function.name
        ))),
    }
}

fn evaluate_case_expression(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if conditions.len() != results.len() {
        return Err(SqlExecutionError::Unsupported(
            "CASE expression requires matching WHEN and THEN clauses".into(),
        ));
    }

    let operand_value = if let Some(expr) = operand {
        Some(evaluate_scalar_expression(expr, dataset)?)
    } else {
        None
    };

    for (condition, result) in conditions.iter().zip(results.iter()) {
        let matches = if let Some(op_value) = &operand_value {
            let cond_value = evaluate_scalar_expression(condition, dataset)?;
            compare_scalar_values(op_value, &cond_value)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or(false)
        } else {
            evaluate_scalar_expression(condition, dataset)?
                .as_bool()
                .unwrap_or(false)
        };

        if matches {
            return evaluate_scalar_expression(result, dataset);
        }
    }

    if let Some(else_expr) = else_result {
        evaluate_scalar_expression(else_expr, dataset)
    } else {
        Ok(ScalarValue::Null)
    }
}

fn evaluate_row_expr(
    expr: &Expr,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    match expr {
        Expr::Identifier(ident) => Ok(dataset
            .column_value(&ident.value, row_idx)
            .map(cached_to_scalar)
            .unwrap_or(ScalarValue::Null)),
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                Ok(dataset
                    .column_value(&last.value, row_idx)
                    .map(cached_to_scalar)
                    .unwrap_or(ScalarValue::Null))
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(ScalarValue::Text(s.clone())),
        Expr::Value(Value::Number(n, _)) => Ok(match n.parse::<f64>() {
            Ok(num) => ScalarValue::Float(num),
            Err(_) => ScalarValue::Text(n.clone()),
        }),
        Expr::Value(Value::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => {
                    let num = value.as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "unary minus requires numeric operand".into(),
                        )
                    })?;
                    Ok(scalar_from_f64(-num))
                }
                UnaryOperator::Not => Ok(ScalarValue::Bool(!value.as_bool().unwrap_or(false))),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported unary operator {op:?}"
                ))),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = evaluate_row_expr(left, row_idx, dataset)?;
            let rhs = evaluate_row_expr(right, row_idx, dataset)?;
            match op {
                BinaryOperator::Plus => combine_numeric(&lhs, &rhs, |a, b| a + b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric addition".into())),
                BinaryOperator::Minus => {
                    combine_numeric(&lhs, &rhs, |a, b| a - b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric subtraction".into())
                    })
                }
                BinaryOperator::Multiply => {
                    combine_numeric(&lhs, &rhs, |a, b| a * b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric multiplication".into())
                    })
                }
                BinaryOperator::Divide => combine_numeric(&lhs, &rhs, |a, b| a / b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric division".into())),
                BinaryOperator::Modulo => combine_numeric(&lhs, &rhs, |a, b| a % b)
                    .ok_or_else(|| SqlExecutionError::Unsupported("non-numeric modulo".into())),
                BinaryOperator::And => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) && rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) || rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ScalarValue::Bool(
                    lhs.as_bool().unwrap_or(false) ^ rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Eq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => {
                    let result = compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater)
                        .unwrap_or(false);
                    println!("row compare {:?} > {:?} => {}", lhs, rhs, result);
                    Ok(ScalarValue::Bool(result))
                }
                BinaryOperator::GtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                _ => Err(SqlExecutionError::Unsupported(format!(
                    "unsupported operator {op:?} in row expression"
                ))),
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, true),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::ILike {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => like_match(&value, &pattern, false),
                _ => false,
            };
            Ok(ScalarValue::Bool(matches))
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = evaluate_row_expr(expr, row_idx, dataset)?;
            let pattern_value = evaluate_row_expr(pattern, row_idx, dataset)?;
            let matches = match (
                value.into_option_string(),
                pattern_value.into_option_string(),
            ) {
                (Some(value), Some(pattern)) => regex_match(&value, &pattern),
                _ => false,
            };
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = evaluate_row_expr(expr, row_idx, dataset)?;
            let low_value = evaluate_row_expr(low, row_idx, dataset)?;
            let high_value = evaluate_row_expr(high, row_idx, dataset)?;
            let cmp_low = compare_scalar_values(&target, &low_value).unwrap_or(Ordering::Less);
            let cmp_high = compare_scalar_values(&target, &high_value).unwrap_or(Ordering::Greater);
            let between = (cmp_low == Ordering::Greater || cmp_low == Ordering::Equal)
                && (cmp_high == Ordering::Less || cmp_high == Ordering::Equal);
            Ok(ScalarValue::Bool(if *negated { !between } else { between }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = evaluate_row_expr(expr, row_idx, dataset)?;
            let mut matches = false;
            for item in list {
                let candidate = evaluate_row_expr(item, row_idx, dataset)?;
                if compare_scalar_values(&target, &candidate)
                    .map(|ord| ord == Ordering::Equal)
                    .unwrap_or(false)
                {
                    matches = true;
                    break;
                }
            }
            Ok(ScalarValue::Bool(if *negated { !matches } else { matches }))
        }
        Expr::IsNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Bool(value.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let value = evaluate_row_expr(inner, row_idx, dataset)?;
            Ok(ScalarValue::Bool(!value.is_null()))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_row_case_expr(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            row_idx,
            dataset,
        ),
        Expr::Function(function) => evaluate_row_function(function, row_idx, dataset),
        Expr::Nested(inner) => evaluate_row_expr(inner, row_idx, dataset),
        Expr::Cast { expr, .. }
        | Expr::SafeCast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Convert { expr, .. } => evaluate_row_expr(expr, row_idx, dataset),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported row-level expression: {expr:?}"
        ))),
    }
}

fn evaluate_row_function(
    function: &Function,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => args.push(evaluate_row_expr(expr, row_idx, dataset)?),
            _ => {
                return Err(SqlExecutionError::Unsupported(format!(
                    "unsupported argument for function {name}"
                )));
            }
        }
    }

    match name.as_str() {
        "ABS" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ABS requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.abs()))
        }
        "ROUND" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ROUND requires numeric argument".into())
            })?;
            let digits = args
                .get(1)
                .and_then(|v| v.as_i128())
                .unwrap_or(0)
                .clamp(-18, 18) as i32;
            let factor = 10_f64.powi(digits);
            Ok(scalar_from_f64((value * factor).round() / factor))
        }
        "CEIL" | "CEILING" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ceil()))
        }
        "FLOOR" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.floor()))
        }
        "EXP" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("EXP requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.exp()))
        }
        "LN" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LN requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ln()))
        }
        "LOG" => {
            let result = match args.len() {
                1 => {
                    let value = args[0].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric argument".into())
                    })?;
                    value.ln()
                }
                2 => {
                    let base = args[0].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric base".into())
                    })?;
                    let value = args[1].as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported("LOG requires numeric argument".into())
                    })?;
                    value.log(base)
                }
                _ => {
                    return Err(SqlExecutionError::Unsupported(
                        "LOG requires one or two arguments".into(),
                    ));
                }
            };
            Ok(scalar_from_f64(result))
        }
        "POWER" => {
            let base = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric base".into())
            })?;
            let exponent = args.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric exponent".into())
            })?;
            Ok(scalar_from_f64(base.powf(exponent)))
        }
        "WIDTH_BUCKET" => {
            if args.len() != 4 {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET requires four arguments".into(),
                ));
            }
            let value = args[0].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric value".into())
            })?;
            let low = args[1].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric low".into())
            })?;
            let high = args[2].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric high".into())
            })?;
            let buckets = args[3].as_i128().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires integer bucket count".into())
            })?;

            if buckets <= 0 || high <= low {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET arguments out of range".into(),
                ));
            }

            let bucket = if value < low {
                0
            } else if value >= high {
                buckets + 1
            } else {
                let step = (high - low) / buckets as f64;
                ((value - low) / step).floor() as i128 + 1
            };
            Ok(ScalarValue::Int(bucket))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported row-level function {name}"
        ))),
    }
}

fn evaluate_row_case_expr(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    row_idx: u64,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    if conditions.len() != results.len() {
        return Err(SqlExecutionError::Unsupported(
            "CASE expression requires matching WHEN and THEN clauses".into(),
        ));
    }

    let operand_value = if let Some(expr) = operand {
        Some(evaluate_row_expr(expr, row_idx, dataset)?)
    } else {
        None
    };

    for (condition, result) in conditions.iter().zip(results.iter()) {
        let matches = if let Some(ref op_value) = operand_value {
            let cond_value = evaluate_row_expr(condition, row_idx, dataset)?;
            compare_scalar_values(op_value, &cond_value)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or(false)
        } else {
            if let Some(raw) = dataset.column_value("value", row_idx) {
                println!("dataset column value at row {}: {:?}", row_idx, raw);
            }
            let cond = evaluate_row_expr(condition, row_idx, dataset)?;
            let cond_bool = cond.as_bool().unwrap_or(false);
            println!(
                "case condition {:?} evaluated to {} (value {:?}) for row {}",
                condition, cond_bool, cond, row_idx
            );
            cond_bool
        };

        if matches {
            return evaluate_row_expr(result, row_idx, dataset);
        }
    }

    if let Some(else_expr) = else_result {
        evaluate_row_expr(else_expr, row_idx, dataset)
    } else {
        Ok(ScalarValue::Null)
    }
}

fn evaluate_scalar_function(
    function: &Function,
    dataset: &AggregateDataset,
) -> Result<ScalarValue, SqlExecutionError> {
    let name = function
        .name
        .0
        .last()
        .map(|ident| ident.value.to_uppercase())
        .unwrap_or_default();

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => args.push(evaluate_scalar_expression(expr, dataset)?),
            _ => {
                return Err(SqlExecutionError::Unsupported(format!(
                    "unsupported argument for function {name}"
                )));
            }
        }
    }

    match name.as_str() {
        "ABS" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ABS requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.abs()))
        }
        "ROUND" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("ROUND requires numeric argument".into())
            })?;
            let digits = args
                .get(1)
                .and_then(|v| v.as_i128())
                .unwrap_or(0)
                .clamp(-18, 18) as i32;
            let factor = 10_f64.powi(digits);
            Ok(scalar_from_f64((value * factor).round() / factor))
        }
        "CEIL" | "CEILING" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("CEIL requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ceil()))
        }
        "FLOOR" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("FLOOR requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.floor()))
        }
        "EXP" => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("EXP requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.exp()))
        }
        "LN" | "LOG" if args.len() == 1 => {
            let value = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LN requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.ln()))
        }
        "LOG" if args.len() == 2 => {
            let base = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LOG requires numeric base".into())
            })?;
            let value = args.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("LOG requires numeric argument".into())
            })?;
            Ok(scalar_from_f64(value.log(base)))
        }
        "POWER" => {
            let base = args.get(0).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric base".into())
            })?;
            let exponent = args.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                SqlExecutionError::Unsupported("POWER requires numeric exponent".into())
            })?;
            Ok(scalar_from_f64(base.powf(exponent)))
        }
        "WIDTH_BUCKET" => {
            if args.len() != 4 {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET requires four arguments".into(),
                ));
            }
            let value = args[0].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric value".into())
            })?;
            let low = args[1].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric low".into())
            })?;
            let high = args[2].as_f64().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires numeric high".into())
            })?;
            let buckets = args[3].as_i128().ok_or_else(|| {
                SqlExecutionError::Unsupported("WIDTH_BUCKET requires integer bucket count".into())
            })?;

            if buckets <= 0 || !high.is_finite() || !low.is_finite() || high <= low {
                return Err(SqlExecutionError::Unsupported(
                    "WIDTH_BUCKET arguments out of range".into(),
                ));
            }

            let bucket = if value < low {
                0
            } else if value >= high {
                buckets + 1
            } else {
                let step = (high - low) / buckets as f64;
                ((value - low) / step).floor() as i128 + 1
            };
            Ok(ScalarValue::Int(bucket))
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported scalar function {name}"
        ))),
    }
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

fn evaluate_selection_expr(
    expr: &Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &MaterializedColumns,
) -> Result<bool, SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                Ok(
                    evaluate_selection_expr(left, row_idx, column_ordinals, materialized)?
                        && evaluate_selection_expr(right, row_idx, column_ordinals, materialized)?,
                )
            }
            BinaryOperator::Or => {
                Ok(
                    evaluate_selection_expr(left, row_idx, column_ordinals, materialized)?
                        || evaluate_selection_expr(right, row_idx, column_ordinals, materialized)?,
                )
            }
            BinaryOperator::Eq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Equal
                })
            }
            BinaryOperator::NotEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Equal
                })
            }
            BinaryOperator::Gt => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Greater
                })
            }
            BinaryOperator::GtEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Less
                })
            }
            BinaryOperator::Lt => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord == Ordering::Less
                })
            }
            BinaryOperator::LtEq => {
                compare_operands(left, right, row_idx, column_ordinals, materialized, |ord| {
                    ord != Ordering::Greater
                })
            }
            _ => Err(SqlExecutionError::Unsupported(format!(
                "unsupported binary operator in WHERE clause: {op:?}"
            ))),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_selection_expr(
                expr,
                row_idx,
                column_ordinals,
                materialized,
            )?),
            _ => Err(SqlExecutionError::Unsupported(format!(
                "unsupported unary operator {op:?}"
            ))),
        },
        Expr::Nested(inner) => {
            evaluate_selection_expr(inner, row_idx, column_ordinals, materialized)
        }
        Expr::IsNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(matches!(operand, OperandValue::Null)
                || matches!(
                operand,
                    OperandValue::Text(ref value) if is_null_value(value.as_ref())
                ))
        }
        Expr::IsNotNull(inner) => {
            let operand = resolve_operand(inner, row_idx, column_ordinals, materialized)?;
            Ok(!matches!(operand, OperandValue::Null)
                && !matches!(
                    operand,
                    OperandValue::Text(ref value) if is_null_value(value.as_ref())
                ))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let low_value = resolve_operand(low, row_idx, column_ordinals, materialized)?;
            let high_value = resolve_operand(high, row_idx, column_ordinals, materialized)?;
            let result = match (&target, &low_value, &high_value) {
                (OperandValue::Text(target), OperandValue::Text(low), OperandValue::Text(high)) => {
                    let lower = compare_strs(target.as_ref(), low.as_ref());
                    let upper = compare_strs(target.as_ref(), high.as_ref());
                    (lower == Ordering::Greater || lower == Ordering::Equal)
                        && (upper == Ordering::Less || upper == Ordering::Equal)
                }
                _ => false,
            };
            Ok(if *negated { !result } else { result })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char: _,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    like_match(value.as_ref(), pattern.as_ref(), true)
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::ILike {
            expr,
            pattern,
            negated,
            escape_char: _,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    like_match(value.as_ref(), pattern.as_ref(), false)
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let pattern_value = resolve_operand(pattern, row_idx, column_ordinals, materialized)?;
            let matches = match (target, pattern_value) {
                (OperandValue::Text(value), OperandValue::Text(pattern)) => {
                    regex_match(value.as_ref(), pattern.as_ref())
                }
                _ => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = resolve_operand(expr, row_idx, column_ordinals, materialized)?;
            let matches = match target {
                OperandValue::Text(value) => list.iter().any(|item| {
                    match resolve_operand(item, row_idx, column_ordinals, materialized) {
                        Ok(OperandValue::Text(candidate)) => {
                            compare_strs(value.as_ref(), candidate.as_ref()) == Ordering::Equal
                        }
                        _ => false,
                    }
                }),
                OperandValue::Null => false,
            };
            Ok(if *negated { !matches } else { matches })
        }
        Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(*b),
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => Ok(n != "0"),
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => Ok(!s.is_empty()),
        Expr::Value(sqlparser::ast::Value::Null) => Ok(false),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported expression in WHERE clause: {expr:?}"
        ))),
    }
}

fn compare_operands(
    left: &Expr,
    right: &Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &MaterializedColumns,
    predicate: impl Fn(Ordering) -> bool,
) -> Result<bool, SqlExecutionError> {
    let left_value = resolve_operand(left, row_idx, column_ordinals, materialized)?;
    let right_value = resolve_operand(right, row_idx, column_ordinals, materialized)?;

    match (left_value, right_value) {
        (OperandValue::Text(lhs), OperandValue::Text(rhs)) => {
            Ok(predicate(compare_strs(lhs.as_ref(), rhs.as_ref())))
        }
        _ => Ok(false),
    }
}

enum OperandValue<'a> {
    Text(Cow<'a, str>),
    Null,
}

fn resolve_operand<'a>(
    expr: &'a Expr,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &'a MaterializedColumns,
) -> Result<OperandValue<'a>, SqlExecutionError> {
    match expr {
        Expr::Identifier(ident) => {
            column_operand(&ident.value, row_idx, column_ordinals, materialized)
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                column_operand(&last.value, row_idx, column_ordinals, materialized)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier".into(),
                ))
            }
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
            Ok(OperandValue::Text(Cow::Borrowed(s.as_str())))
        }
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            Ok(OperandValue::Text(Cow::Borrowed(n.as_str())))
        }
        Expr::Value(sqlparser::ast::Value::Boolean(b)) => {
            Ok(OperandValue::Text(Cow::Owned(if *b {
                "true".into()
            } else {
                "false".into()
            })))
        }
        Expr::Value(sqlparser::ast::Value::Null) => Ok(OperandValue::Null),
        Expr::Nested(inner) => resolve_operand(inner, row_idx, column_ordinals, materialized),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let OperandValue::Text(value) =
                resolve_operand(expr, row_idx, column_ordinals, materialized)?
            {
                Ok(OperandValue::Text(Cow::Owned(format!("-{}", value))))
            } else {
                Ok(OperandValue::Null)
            }
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported operand expression: {expr:?}"
        ))),
    }
}

fn column_operand<'a>(
    column_name: &str,
    row_idx: u64,
    column_ordinals: &HashMap<String, usize>,
    materialized: &'a MaterializedColumns,
) -> Result<OperandValue<'a>, SqlExecutionError> {
    let ordinal = column_ordinals.get(column_name).copied().ok_or_else(|| {
        SqlExecutionError::Unsupported(format!("unknown column referenced: {column_name}"))
    })?;
    println!("column operand lookup {} -> ordinal {} for row {}", column_name, ordinal, row_idx);
    if let Some(map) = materialized.get(&ordinal) {
        println!(
            "column map keys for ordinal {}: {:?}",
            ordinal,
            map.keys().collect::<Vec<_>>()
        );
    }
    let value = materialized
        .get(&ordinal)
        .and_then(|column_map| column_map.get(&row_idx));
    match value {
        Some(CachedValue::Null) => Ok(OperandValue::Null),
        Some(CachedValue::Text(text)) => Ok(OperandValue::Text(Cow::Borrowed(text.as_str()))),
        None => Ok(OperandValue::Null),
    }
}

fn is_null_value(value: &str) -> bool {
    value.is_empty()
        || value.eq_ignore_ascii_case("null")
        || value.eq_ignore_ascii_case("nil")
        || is_encoded_null(value)
}

fn like_match(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    let val = if case_sensitive {
        value.to_string()
    } else {
        value.to_lowercase()
    };
    let pat = if case_sensitive {
        pattern.to_string()
    } else {
        pattern.to_lowercase()
    };

    like_match_recursive(&val, &pat)
}

fn like_match_recursive(value: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return value.is_empty();
    }

    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    like_match_dfs(&value_chars, &pattern_chars, 0, 0)
}

fn like_match_dfs(value: &[char], pattern: &[char], mut v_idx: usize, mut p_idx: usize) -> bool {
    let v_len = value.len();
    let p_len = pattern.len();

    while p_idx < p_len {
        match pattern[p_idx] {
            '%' => {
                while p_idx + 1 < p_len && pattern[p_idx + 1] == '%' {
                    p_idx += 1;
                }
                if p_idx + 1 == p_len {
                    return true;
                }
                p_idx += 1;
                while v_idx <= v_len {
                    if like_match_dfs(&value[v_idx..], &pattern[p_idx..], 0, 0) {
                        return true;
                    }
                    if v_idx == v_len {
                        break;
                    }
                    v_idx += 1;
                }
                return false;
            }
            '_' => {
                if v_idx >= v_len {
                    return false;
                }
                v_idx += 1;
                p_idx += 1;
            }
            ch => {
                if v_idx >= v_len || value[v_idx] != ch {
                    return false;
                }
                v_idx += 1;
                p_idx += 1;
            }
        }
    }

    v_idx == v_len
}

fn regex_match(value: &str, pattern: &str) -> bool {
    let starts_with_anchor = pattern.starts_with('^');
    let ends_with_anchor = pattern.ends_with('$');

    let clean_pattern = pattern.trim_start_matches('^').trim_end_matches('$');

    if starts_with_anchor && ends_with_anchor {
        value == clean_pattern
    } else if starts_with_anchor {
        value.starts_with(clean_pattern)
    } else if ends_with_anchor {
        value.ends_with(clean_pattern)
    } else {
        value.contains(clean_pattern)
    }
}

fn build_projection(
    projection: Vec<SelectItem>,
    table_columns: &[ColumnCatalog],
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    table_alias: Option<&str>,
) -> Result<(Vec<String>, Vec<usize>), SqlExecutionError> {
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
            let mut names = Vec::new();
            let mut ordinals = Vec::new();
            push_projection_item(item, column_ordinals, table_name, &mut names, &mut ordinals)?;
            for item in iter {
                push_projection_item(item, column_ordinals, table_name, &mut names, &mut ordinals)?;
            }
            Ok((names, ordinals))
        }
    }
}

fn push_projection_item(
    item: SelectItem,
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    result_names: &mut Vec<String>,
    result_ordinals: &mut Vec<usize>,
) -> Result<(), SqlExecutionError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            let column_name = column_name_from_expr(&expr).ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "only simple column projections are supported".into(),
                )
            })?;
            let ordinal = column_ordinals.get(&column_name).copied().ok_or_else(|| {
                SqlExecutionError::ColumnMismatch {
                    table: table_name.to_string(),
                    column: column_name.clone(),
                }
            })?;
            result_names.push(column_name);
            result_ordinals.push(ordinal);
            Ok(())
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            let column_name = column_name_from_expr(&expr).ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "only simple column projections are supported".into(),
                )
            })?;
            let ordinal = column_ordinals.get(&column_name).copied().ok_or_else(|| {
                SqlExecutionError::ColumnMismatch {
                    table: table_name.to_string(),
                    column: column_name.clone(),
                }
            })?;
            result_names.push(alias.value.clone());
            result_ordinals.push(ordinal);
            Ok(())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            Err(SqlExecutionError::Unsupported(
                "wildcard projection must be the only projection item".into(),
            ))
        }
    }
}

fn collect_all_columns(table_columns: &[ColumnCatalog]) -> (Vec<String>, Vec<usize>) {
    let mut names = Vec::with_capacity(table_columns.len());
    let mut ordinals = Vec::with_capacity(table_columns.len());
    for column in table_columns {
        names.push(column.name.clone());
        ordinals.push(column.ordinal);
    }
    (names, ordinals)
}

fn wildcard_options_supported(options: &WildcardAdditionalOptions) -> bool {
    options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_rename.is_none()
        && options.opt_replace.is_none()
}

fn object_name_matches_table(
    object_name: &ObjectName,
    table_name: &str,
    table_alias: Option<&str>,
) -> bool {
    object_name
        .0
        .last()
        .map(|ident| {
            ident.value == table_name
                || table_alias
                    .map(|alias| ident.value == alias)
                    .unwrap_or(false)
        })
        .unwrap_or(false)
}

fn parse_limit(limit: Option<Expr>) -> Result<Option<usize>, SqlExecutionError> {
    match limit {
        None => Ok(None),
        Some(Expr::Identifier(ident)) if ident.value.eq_ignore_ascii_case("all") => Ok(None),
        Some(expr) => parse_usize_literal(&expr, "LIMIT").map(Some),
    }
}

fn parse_offset(offset: Option<Offset>) -> Result<usize, SqlExecutionError> {
    match offset {
        None => Ok(0),
        Some(offset) => parse_usize_literal(&offset.value, "OFFSET"),
    }
}

fn parse_usize_literal(expr: &Expr, context: &str) -> Result<usize, SqlExecutionError> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<usize>().map_err(|_| {
            SqlExecutionError::Unsupported(format!(
                "{context} requires a non-negative integer literal"
            ))
        }),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "{context} requires a numeric literal"
        ))),
    }
}

fn extract_equality_filters(expr: &Expr) -> Result<HashMap<String, String>, SqlExecutionError> {
    let mut filters = HashMap::new();
    gather_filters(expr, &mut filters)?;
    Ok(filters)
}

fn gather_filters(
    expr: &Expr,
    filters: &mut HashMap<String, String>,
) -> Result<(), SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            gather_filters(left, filters)?;
            gather_filters(right, filters)
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let column = column_name_from_expr(left).ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "only column = literal predicates are supported".into(),
                )
            })?;
            let value = expr_to_string(right)?;
            match filters.get(&column) {
                Some(existing) if compare_strs(existing, &value) != Ordering::Equal => {
                    return Err(SqlExecutionError::Unsupported(format!(
                        "conflicting predicates for column {column}"
                    )));
                }
                _ => {
                    filters.insert(column, value);
                }
            }
            Ok(())
        }
        Expr::Nested(inner) => gather_filters(inner, filters),
        _ => Err(SqlExecutionError::Unsupported(
            "only ANDed column = literal predicates are supported".into(),
        )),
    }
}

fn column_name_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        Expr::Nested(inner) => column_name_from_expr(inner),
        _ => None,
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn table_with_joins_to_name(table: &TableWithJoins) -> Result<String, SqlExecutionError> {
    if !table.joins.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "UPDATE with JOINs is not supported".into(),
        ));
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(SqlExecutionError::Unsupported(
            "unsupported table reference".into(),
        )),
    }
}

fn expr_to_string(expr: &Expr) -> Result<String, SqlExecutionError> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Value(Value::Number(n, _)) => Ok(n.clone()),
        Expr::Value(Value::Boolean(b)) => Ok(if *b { "true" } else { "false" }.into()),
        Expr::Value(Value::Null) => Ok(encode_null()),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported literal expression: {expr:?}"
        ))),
    }
}

fn compare_strs(left: &str, right: &str) -> Ordering {
    if is_encoded_null(left) && is_encoded_null(right) {
        return Ordering::Equal;
    }
    if is_encoded_null(left) {
        return Ordering::Less;
    }
    if is_encoded_null(right) {
        return Ordering::Greater;
    }

    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() || value.is_infinite() {
        return value.to_string();
    }

    let rounded = (value * 1_000_000.0).round() / 1_000_000.0;
    if (rounded.fract().abs() - 0.0).abs() < f64::EPSILON {
        format!("{:.0}", rounded)
    } else {
        let mut s = format!("{rounded}");
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        s
    }
}

fn scalar_from_f64(value: f64) -> ScalarValue {
    if value.is_nan() || value.is_infinite() {
        ScalarValue::Float(value)
    } else if (value.fract().abs() - 0.0).abs() < 1e-9 {
        ScalarValue::Int(value as i128)
    } else {
        ScalarValue::Float(value)
    }
}

fn combine_numeric<F>(left: &ScalarValue, right: &ScalarValue, op: F) -> Option<ScalarValue>
where
    F: Fn(f64, f64) -> f64,
{
    let lhs = left.as_f64()?;
    let rhs = right.as_f64()?;
    Some(scalar_from_f64(op(lhs, rhs)))
}

fn compare_scalar_values(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    match (left.as_f64(), right.as_f64()) {
        (Some(lhs), Some(rhs)) => lhs.partial_cmp(&rhs),
        _ => match (left, right) {
            (ScalarValue::Text(lhs), ScalarValue::Text(rhs)) => Some(compare_strs(lhs, rhs)),
            (ScalarValue::Bool(lhs), ScalarValue::Bool(rhs)) => Some(lhs.cmp(rhs)),
            (ScalarValue::Int(lhs), ScalarValue::Int(rhs)) => Some(lhs.cmp(rhs)),
            _ => None,
        },
    }
}
