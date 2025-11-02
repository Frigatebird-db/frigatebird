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
    Assignment, BinaryOperator, Expr, FromTable, GroupByExpr, ObjectName, Offset, OrderByExpr,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator,
    Value, WildcardAdditionalOptions,
};
use sqlparser::parser::ParserError;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

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
    pub rows: Vec<Vec<String>>,
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

        let (result_columns, projection_ordinals) = build_projection(
            projection,
            &columns,
            &column_ordinals,
            &table_name,
            table_alias.as_deref(),
        )?;

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

        let mut candidate_rows =
            self.locate_rows_by_sort_tuple(&table_name, &sort_columns, &key_values)?;
        if candidate_rows.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        candidate_rows.sort_unstable();
        candidate_rows.dedup();

        let mut required_ordinals: BTreeSet<usize> = projection_ordinals.iter().copied().collect();
        for column in &sort_columns {
            required_ordinals.insert(column.ordinal);
        }

        let predicate_ordinals =
            collect_expr_column_ordinals(&selection_expr, &column_ordinals, &table_name)?;
        required_ordinals.extend(predicate_ordinals);

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

        let mut rows = Vec::with_capacity(window.len());
        for &row_idx in window {
            let mut projected = Vec::with_capacity(projection_ordinals.len());
            for &ordinal in &projection_ordinals {
                let value = materialized
                    .get(&ordinal)
                    .and_then(|column_map| column_map.get(&row_idx))
                    .cloned()
                    .or_else(|| {
                        self.page_handler
                            .read_entry_at(&table_name, &columns[ordinal].name, row_idx)
                            .map(|entry| entry.get_data().to_string())
                    })
                    .ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing value for {table_name}.{} at row {row_idx}",
                            columns[ordinal].name
                        ))
                    })?;
                projected.push(value);
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

type MaterializedColumns = HashMap<usize, HashMap<u64, String>>;

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
        _ => {}
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

        let mut values: HashMap<u64, String> = HashMap::with_capacity(rows.len());
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
                            values.insert(target, entry.get_data().to_string());
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
    let value = materialized
        .get(&ordinal)
        .and_then(|column_map| column_map.get(&row_idx));
    match value {
        Some(val) => Ok(OperandValue::Text(Cow::Borrowed(val.as_str()))),
        None => Ok(OperandValue::Null),
    }
}

fn is_null_value(value: &str) -> bool {
    value.is_empty() || value.eq_ignore_ascii_case("null") || value.eq_ignore_ascii_case("nil")
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
        Expr::Value(Value::Null) => Ok(String::new()),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported literal expression: {expr:?}"
        ))),
    }
}

fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}
