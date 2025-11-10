mod aggregates;
pub mod batch;
mod expressions;
mod grouping_helpers;
mod helpers;
mod ordering;
mod projection_helpers;
mod row_functions;
mod scalar_functions;
mod scan_helpers;
mod spill;
mod values;
mod window_helpers;

use self::batch::{Bitmap, ColumnData, ColumnarBatch, ColumnarPage};
use self::spill::SpillManager;
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{ColumnCatalog, PageDescriptor, PageDirectory, TableCatalog};
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
    Assignment, BinaryOperator, Expr, FromTable, GroupByExpr, Ident, ObjectName, Offset,
    OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    UnaryOperator, Value,
};
use sqlparser::parser::ParserError;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use aggregates::{
    AggregateDataset, AggregateFunctionKind, AggregateFunctionPlan, AggregateProjection,
    AggregateProjectionPlan, AggregateState, AggregationHashTable, evaluate_aggregate_outputs,
    plan_aggregate_projection, select_item_contains_aggregate, vectorized_average_update,
    vectorized_count_star_update, vectorized_count_value_update, vectorized_sum_update,
};
use expressions::{
    evaluate_expression_on_batch, evaluate_row_expr, evaluate_selection_expr,
    evaluate_selection_on_page,
};
use grouping_helpers::{
    evaluate_group_key, evaluate_group_keys_on_batch, evaluate_having, validate_group_by,
};
use helpers::{
    collect_expr_column_names, collect_expr_column_ordinals, column_name_from_expr, expr_to_string,
    object_name_to_string, parse_limit, parse_offset, table_with_joins_to_name,
};
use ordering::{
    MergeOperator, NullsPlacement, OrderClause, OrderKey, build_group_order_key,
    compare_order_keys, sort_batch_in_memory, sort_rows_logical,
};
use projection_helpers::{build_projection, materialize_columns};
use scan_helpers::{SortKeyPrefix, collect_sort_key_filters, collect_sort_key_prefixes};
use values::{
    CachedValue, ScalarValue, combine_numeric, compare_scalar_values, compare_strs, scalar_from_f64,
};
use window_helpers::{
    WindowOperator, collect_window_function_plans, collect_window_plans_from_expr,
    ensure_common_partition, plan_order_clauses, rewrite_window_expressions,
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

fn find_group_expr_index(expr: &Expr, group_exprs: &[Expr]) -> Option<usize> {
    group_exprs.iter().position(|candidate| candidate == expr)
}

fn literal_value(expr: &Expr) -> Option<Option<String>> {
    match expr {
        Expr::Value(Value::Null) => Some(None),
        Expr::Value(Value::Boolean(flag)) => {
            Some(Some(if *flag { "true".into() } else { "false".into() }))
        }
        Expr::Value(Value::Number(value, _)) => Some(Some(value.clone())),
        Expr::Value(Value::SingleQuotedString(text)) => Some(Some(text.clone())),
        _ => None,
    }
}

fn projection_item_expr(
    item: &ProjectionItem,
    columns: &[ColumnCatalog],
) -> Result<Expr, SqlExecutionError> {
    match item {
        ProjectionItem::Direct { ordinal } => {
            let column = columns.get(*ordinal).ok_or_else(|| {
                SqlExecutionError::OperationFailed(format!(
                    "invalid column ordinal {ordinal} in projection"
                ))
            })?;
            Ok(Expr::Identifier(Ident::new(column.name.clone())))
        }
        ProjectionItem::Computed { expr } => Ok(expr.clone()),
    }
}

fn build_group_exprs_for_distinct(
    plan: &ProjectionPlan,
    columns: &[ColumnCatalog],
) -> Result<Vec<Expr>, SqlExecutionError> {
    plan.items
        .iter()
        .map(|item| projection_item_expr(item, columns))
        .collect()
}

fn ensure_aggregate_plan_for_expr(
    expr: &Expr,
    aggregate_plans: &mut Vec<AggregateFunctionPlan>,
    aggregate_lookup: &mut HashMap<String, usize>,
) -> Result<Option<usize>, SqlExecutionError> {
    let key = expr.to_string();
    if let Some(&idx) = aggregate_lookup.get(&key) {
        return Ok(Some(idx));
    }
    if let Some(plan) = AggregateFunctionPlan::from_expr(expr)? {
        let idx = aggregate_plans.len();
        aggregate_plans.push(plan);
        aggregate_lookup.insert(key, idx);
        Ok(Some(idx))
    } else {
        Ok(None)
    }
}

fn ensure_having_aggregate_plans(
    expr: &Expr,
    aggregate_plans: &mut Vec<AggregateFunctionPlan>,
    aggregate_lookup: &mut HashMap<String, usize>,
) -> Result<(), SqlExecutionError> {
    if ensure_aggregate_plan_for_expr(expr, aggregate_plans, aggregate_lookup)?.is_some() {
        return Ok(());
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            ensure_having_aggregate_plans(left, aggregate_plans, aggregate_lookup)?;
            ensure_having_aggregate_plans(right, aggregate_plans, aggregate_lookup)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            ensure_having_aggregate_plans(expr, aggregate_plans, aggregate_lookup)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            ensure_having_aggregate_plans(expr, aggregate_plans, aggregate_lookup)?;
            ensure_having_aggregate_plans(low, aggregate_plans, aggregate_lookup)?;
            ensure_having_aggregate_plans(high, aggregate_plans, aggregate_lookup)?;
        }
        Expr::InList { expr, list, .. } => {
            ensure_having_aggregate_plans(expr, aggregate_plans, aggregate_lookup)?;
            for item in list {
                ensure_having_aggregate_plans(item, aggregate_plans, aggregate_lookup)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                ensure_having_aggregate_plans(op, aggregate_plans, aggregate_lookup)?;
            }
            for condition in conditions {
                ensure_having_aggregate_plans(condition, aggregate_plans, aggregate_lookup)?;
            }
            for result in results {
                ensure_having_aggregate_plans(result, aggregate_plans, aggregate_lookup)?;
            }
            if let Some(else_expr) = else_result {
                ensure_having_aggregate_plans(else_expr, aggregate_plans, aggregate_lookup)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct HavingEvalContext<'a> {
    aggregate_plans: &'a [AggregateFunctionPlan],
    aggregate_lookup: &'a HashMap<String, usize>,
    group_expr_lookup: &'a HashMap<String, usize>,
    group_key: &'a GroupKey,
    states: &'a [AggregateState],
}

fn evaluate_having_expression(
    expr: &Expr,
    ctx: &HavingEvalContext,
) -> Result<ScalarValue, SqlExecutionError> {
    match expr {
        Expr::Identifier(ident) => group_value_as_scalar(&ident.value, ctx),
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                group_value_as_scalar(&last.value, ctx)
            } else {
                Err(SqlExecutionError::Unsupported(
                    "empty compound identifier in HAVING clause".into(),
                ))
            }
        }
        Expr::Value(Value::Number(value, _)) => value
            .parse::<f64>()
            .map(scalar_from_f64)
            .map_err(|_| SqlExecutionError::Unsupported("invalid numeric literal".into())),
        Expr::Value(Value::SingleQuotedString(text)) => Ok(ScalarValue::Text(text.clone())),
        Expr::Value(Value::Boolean(flag)) => Ok(ScalarValue::Bool(*flag)),
        Expr::Value(Value::Null) => Ok(ScalarValue::Null),
        Expr::BinaryOp { left, op, right } => {
            let lhs = evaluate_having_expression(left, ctx)?;
            let rhs = evaluate_having_expression(right, ctx)?;
            match op {
                BinaryOperator::Plus => {
                    combine_numeric(&lhs, &rhs, |a, b| a + b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric addition in HAVING".into())
                    })
                }
                BinaryOperator::Minus => {
                    combine_numeric(&lhs, &rhs, |a, b| a - b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric subtraction in HAVING".into())
                    })
                }
                BinaryOperator::Multiply => {
                    combine_numeric(&lhs, &rhs, |a, b| a * b).ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "non-numeric multiplication in HAVING".into(),
                        )
                    })
                }
                BinaryOperator::Divide => {
                    combine_numeric(&lhs, &rhs, |a, b| a / b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric division in HAVING".into())
                    })
                }
                BinaryOperator::Modulo => {
                    combine_numeric(&lhs, &rhs, |a, b| a % b).ok_or_else(|| {
                        SqlExecutionError::Unsupported("non-numeric modulo in HAVING".into())
                    })
                }
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
                BinaryOperator::Gt => Ok(ScalarValue::Bool(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater)
                        .unwrap_or(false),
                )),
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
                _ => Err(SqlExecutionError::Unsupported(
                    "operator not supported in HAVING".into(),
                )),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_having_expression(expr, ctx)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => {
                    let num = value.as_f64().ok_or_else(|| {
                        SqlExecutionError::Unsupported(
                            "unary minus requires numeric operand in HAVING".into(),
                        )
                    })?;
                    Ok(scalar_from_f64(-num))
                }
                UnaryOperator::Not => Ok(ScalarValue::Bool(!value.as_bool().unwrap_or(false))),
                _ => Err(SqlExecutionError::Unsupported(
                    "unsupported unary operator in HAVING".into(),
                )),
            }
        }
        Expr::Nested(inner) => evaluate_having_expression(inner, ctx),
        Expr::Function(_) => aggregate_value_as_scalar(expr, ctx),
        _ => Err(SqlExecutionError::Unsupported(
            "HAVING expression is not supported in vectorized mode".into(),
        )),
    }
}

fn group_value_as_scalar(
    ident: &str,
    ctx: &HavingEvalContext<'_>,
) -> Result<ScalarValue, SqlExecutionError> {
    let idx = ctx.group_expr_lookup.get(ident).ok_or_else(|| {
        SqlExecutionError::Unsupported(format!("HAVING references non-grouped column {ident}"))
    })?;
    let value = ctx.group_key.value_at(*idx).unwrap_or(None);
    Ok(match value {
        Some(text) => ScalarValue::Text(text),
        None => ScalarValue::Null,
    })
}

fn aggregate_value_as_scalar(
    expr: &Expr,
    ctx: &HavingEvalContext<'_>,
) -> Result<ScalarValue, SqlExecutionError> {
    let key = expr.to_string();
    let slot = ctx.aggregate_lookup.get(&key).ok_or_else(|| {
        SqlExecutionError::Unsupported(format!(
            "aggregate {key} missing from vectorized aggregation"
        ))
    })?;
    let state = ctx.states.get(*slot).ok_or_else(|| {
        SqlExecutionError::OperationFailed("missing aggregate state for HAVING".into())
    })?;
    ctx.aggregate_plans[*slot]
        .scalar_value(state)
        .ok_or_else(|| {
            SqlExecutionError::Unsupported(format!("unable to evaluate aggregate {key} in HAVING"))
        })
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

use std::fmt;

#[derive(Clone)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub batches: Vec<ColumnarBatch>,
}

impl SelectResult {
    pub fn row_iter(&self) -> RowIter<'_> {
        RowIter {
            result: self,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|batch| batch.num_rows == 0)
    }
}

impl fmt::Debug for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectResult")
            .field("columns", &self.columns)
            .field("row_count", &self.row_count())
            .finish()
    }
}

impl fmt::Display for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            writeln!(f, "(0 columns)")?;
        } else {
            for (idx, column) in self.columns.iter().enumerate() {
                if idx > 0 {
                    write!(f, " | ")?;
                }
                write!(f, "{column}")?;
            }
            writeln!(f)?;
        }

        let mut row_count = 0usize;
        for batch in &self.batches {
            for row_idx in 0..batch.num_rows {
                row_count += 1;
                for col_idx in 0..self.columns.len() {
                    if col_idx > 0 {
                        write!(f, " | ")?;
                    }
                    let value = batch
                        .columns
                        .get(&col_idx)
                        .ok_or(fmt::Error)?
                        .value_as_string(row_idx);
                    match value {
                        Some(text) => write!(f, "{text}")?,
                        None => write!(f, "NULL")?,
                    }
                }
                writeln!(f)?;
            }
        }

        writeln!(f, "({row_count} rows)")
    }
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

fn rewrite_projection_plan_for_windows(
    plan: &mut ProjectionPlan,
    alias_map: &HashMap<String, String>,
) -> Result<(), SqlExecutionError> {
    for item in &mut plan.items {
        if let ProjectionItem::Computed { expr } = item {
            rewrite_window_expressions(expr, alias_map)?;
        }
    }
    Ok(())
}

fn assign_window_display_aliases(
    plans: &mut [WindowFunctionPlan],
    projection_plan: &ProjectionPlan,
) {
    use sqlparser::ast::Expr;
    use std::collections::VecDeque;

    let mut key_to_indices: HashMap<String, VecDeque<usize>> = HashMap::new();
    for (idx, plan) in plans.iter().enumerate() {
        key_to_indices
            .entry(plan.key.clone())
            .or_default()
            .push_back(idx);
    }

    for (idx, item) in projection_plan.items.iter().enumerate() {
        let expr = match item {
            ProjectionItem::Computed { expr } => expr,
            ProjectionItem::Direct { .. } => continue,
        };
        if let Expr::Function(function) = expr {
            if function.over.is_none() {
                continue;
            }
            let key = function.to_string();
            if let Some(indices) = key_to_indices.get_mut(&key) {
                if let Some(plan_idx) = indices.pop_front() {
                    plans[plan_idx].display_alias = Some(projection_plan.headers[idx].clone());
                }
            }
        }
    }
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

impl GroupKey {
    fn empty() -> Self {
        Self { values: Vec::new() }
    }

    fn from_values(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    fn value_at(&self, idx: usize) -> Option<Option<String>> {
        self.values.get(idx).cloned()
    }
}

enum VectorAggregationOutput {
    Aggregate { slot_index: usize },
    GroupExpr { group_index: usize },
    Literal { value: Option<String> },
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
    result_ordinal: usize,
    result_alias: String,
    display_alias: Option<String>,
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
const WINDOW_BATCH_CHUNK_SIZE: usize = 1_024;
const SORT_OUTPUT_BATCH_SIZE: usize = 1_024;

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
        let sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };

        let scan_selection_expr = if has_selection && !apply_selection_late {
            Some(&selection_expr)
        } else {
            None
        };

        if !window_plans.is_empty() {
            let projection_plan = projection_plan_opt.take().ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "window queries require an explicit projection plan".into(),
                )
            })?;
            let partition_exprs = ensure_common_partition(&window_plans)?;
            let vectorized_selection_expr = if has_selection {
                Some(&selection_expr)
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
                vectorized_selection_expr,
                &column_ordinals,
                &order_clauses,
                result_columns.clone(),
                limit_expr.clone(),
                offset_expr.clone(),
                distinct_flag,
                qualify.clone(),
            );
        }

        let can_run_vectorized_projection = !needs_aggregation
            && qualify.is_none()
            && window_plans.is_empty()
            && sort_key_filters.is_none()
            && sort_key_prefixes.is_none()
            && projection_plan_opt.is_some()
            && (order_clauses.is_empty() || !distinct_flag);

        if can_run_vectorized_projection {
            if let Some(projection_plan) = &projection_plan_opt {
                match self.execute_vectorized_projection(
                    &table_name,
                    &catalog,
                    &columns,
                    projection_plan,
                    &required_ordinals,
                    scan_selection_expr,
                    &column_ordinals,
                    &order_clauses,
                    result_columns.clone(),
                    limit_expr.clone(),
                    offset_expr.clone(),
                    distinct_flag,
                ) {
                    Ok(result) => return Ok(result),
                    Err(SqlExecutionError::Unsupported(_)) => {}
                    Err(err) => return Err(err),
                }
            }
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

        let can_run_vectorized_aggregation = needs_aggregation
            && !distinct_flag
            && order_clauses.is_empty()
            && sort_key_filters.is_none()
            && sort_key_prefixes.is_none()
            && !apply_selection_late
            && window_plans.is_empty()
            && aggregate_plan_opt.is_some()
            && simple_group_exprs.is_some();

        if can_run_vectorized_aggregation {
            if let Some(aggregate_plan) = &aggregate_plan_opt {
                let group_exprs = simple_group_exprs
                    .clone()
                    .expect("group expressions checked above");
                match self.execute_vectorized_aggregation(
                    &table_name,
                    &catalog,
                    &columns,
                    aggregate_plan,
                    &group_exprs,
                    &required_ordinals,
                    scan_selection_expr,
                    &column_ordinals,
                    result_columns.clone(),
                    limit_expr.clone(),
                    offset_expr.clone(),
                    having.as_ref(),
                ) {
                    Ok(result) => return Ok(result),
                    Err(SqlExecutionError::Unsupported(_)) => {}
                    Err(err) => return Err(err),
                }
            }
        }

        if distinct_flag
            && !needs_aggregation
            && projection_plan_opt.is_some()
            && order_clauses.is_empty()
            && window_plans.is_empty()
            && qualify.is_none()
            && sort_key_prefixes.is_none()
            && sort_key_filters.is_none()
        {
            if let Some(projection_plan) = &projection_plan_opt {
                let group_exprs = build_group_exprs_for_distinct(projection_plan, &columns)?;
                let outputs: Vec<AggregateProjection> = projection_plan
                    .items
                    .iter()
                    .map(|item| {
                        Ok(AggregateProjection {
                            expr: projection_item_expr(item, &columns)?,
                        })
                    })
                    .collect::<Result<_, SqlExecutionError>>()?;
                let synthetic_plan = AggregateProjectionPlan {
                    outputs,
                    required_ordinals: projection_plan.required_ordinals.clone(),
                    headers: projection_plan.headers.clone(),
                };

                return self.execute_vectorized_aggregation(
                    &table_name,
                    &catalog,
                    &columns,
                    &synthetic_plan,
                    &group_exprs,
                    &projection_plan.required_ordinals,
                    scan_selection_expr,
                    &column_ordinals,
                    result_columns,
                    limit_expr,
                    offset_expr,
                    None,
                );
            }
        }

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
        } else if let Some(prefixes) = sort_key_prefixes {
            self.locate_rows_by_sort_prefixes(&table_name, &sort_columns, &prefixes)?
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
                batches: Vec::new(),
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
                evaluate_selection_expr(&selection_expr, row_idx, &column_ordinals, &materialized)?
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
                    batches: Vec::new(),
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
            let batch = rows_to_batch(final_rows);
            let batches = if batch.num_rows == 0 {
                Vec::new()
            } else {
                vec![batch]
            };

            return Ok(SelectResult {
                columns: result_columns,
                batches,
            });
        }

        let no_selected_rows = apply_selection_late && selection_set.is_empty();
        if (!apply_selection_late && matching_rows.is_empty()) || no_selected_rows {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        let projection_plan = projection_plan_opt.expect("projection plan required");
        let mut rows = Vec::with_capacity(matching_rows.len());
        let dataset_required = projection_plan.needs_dataset() || qualify.is_some();
        let dataset_holder = if dataset_required {
            Some(AggregateDataset {
                rows: matching_rows.as_slice(),
                materialized: &materialized,
                column_ordinals: &column_ordinals,
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

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;

        let mut batches = if rows.is_empty() {
            Vec::new()
        } else {
            vec![rows_to_batch(rows)]
        };
        if distinct_flag {
            batches = deduplicate_batches(batches, projection_plan.items.len());
        }
        let limited_batches = self.apply_limit_offset(batches.into_iter(), offset, limit)?;

        Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
        })
    }

    fn execute_vectorized_projection(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        projection_plan: &ProjectionPlan,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
    ) -> Result<SelectResult, SqlExecutionError> {
        let mut batch = self.scan_and_filter_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
        )?;

        if batch.num_rows == 0 || batch.columns.is_empty() {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
        }

        if !order_clauses.is_empty() {
            let sorted_batches =
                self.execute_sort(std::iter::once(batch), order_clauses, catalog)?;
            batch = merge_batches(sorted_batches);
        }

        let final_batch = self.build_projection_batch(&batch, projection_plan, catalog)?;
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

    fn execute_vectorized_window_query(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        mut projection_plan: ProjectionPlan,
        mut window_plans: Vec<WindowFunctionPlan>,
        partition_exprs: Vec<Expr>,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
        mut qualify_expr: Option<Expr>,
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

        let mut batch = self.scan_and_filter_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
        )?;
        if batch.num_rows == 0 {
            return Ok(SelectResult {
                columns: result_columns,
                batches: Vec::new(),
            });
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
            batch = sort_batch_in_memory(&batch, &partition_clauses, catalog)?;
        }

        let chunks = chunk_batch(&batch, WINDOW_BATCH_CHUNK_SIZE);
        let mut operator =
            WindowOperator::new(chunks.into_iter(), window_plans, partition_exprs, catalog);
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

    fn build_projection_batch(
        &self,
        batch: &ColumnarBatch,
        projection_plan: &ProjectionPlan,
        catalog: &TableCatalog,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        let mut final_batch = ColumnarBatch::with_capacity(projection_plan.items.len());
        final_batch.num_rows = batch.num_rows;
        for (idx, item) in projection_plan.items.iter().enumerate() {
            let column_page = match item {
                ProjectionItem::Direct { ordinal } => {
                    batch.columns.get(ordinal).cloned().ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing column ordinal {ordinal} in vectorized batch"
                        ))
                    })?
                }
                ProjectionItem::Computed { expr } => {
                    evaluate_expression_on_batch(expr, batch, catalog)?
                }
            };
            final_batch.columns.insert(idx, column_page);
        }
        Ok(final_batch)
    }

    fn apply_qualify_filter(
        &self,
        batch: ColumnarBatch,
        expr: &Expr,
        catalog: &TableCatalog,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        let filter_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
        let bitmap = boolean_bitmap_from_page(&filter_page)?;
        if bitmap.count_ones() == batch.num_rows {
            return Ok(batch);
        }
        if bitmap.count_ones() == 0 {
            return Ok(ColumnarBatch::new());
        }
        Ok(batch.filter_by_bitmap(&bitmap))
    }

    fn execute_sort<I>(
        &self,
        batches: I,
        clauses: &[OrderClause],
        catalog: &TableCatalog,
    ) -> Result<Vec<ColumnarBatch>, SqlExecutionError>
    where
        I: IntoIterator<Item = ColumnarBatch>,
    {
        if clauses.is_empty() {
            return Ok(batches
                .into_iter()
                .filter(|batch| batch.num_rows > 0)
                .collect());
        }

        let mut spill_manager = SpillManager::new()
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;

        for batch in batches.into_iter() {
            if batch.num_rows == 0 {
                continue;
            }
            let sorted = sort_batch_in_memory(&batch, clauses, catalog)?;
            spill_manager
                .spill_batch(sorted)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }

        let runs = spill_manager
            .finish()
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        if runs.len() <= 1 {
            return Ok(runs);
        }

        let mut merge_operator =
            MergeOperator::new(runs, clauses, catalog, SORT_OUTPUT_BATCH_SIZE)?;
        let mut merged_batches = Vec::new();
        while let Some(batch) = merge_operator.next_batch()? {
            if batch.num_rows > 0 {
                merged_batches.push(batch);
            }
        }
        Ok(merged_batches)
    }

    fn apply_limit_offset<I>(
        &self,
        batches: I,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<ColumnarBatch>, SqlExecutionError>
    where
        I: IntoIterator<Item = ColumnarBatch>,
    {
        let mut rows_seen = 0usize;
        let mut rows_emitted = 0usize;
        let mut limited_batches = Vec::new();

        for batch in batches.into_iter() {
            if batch.num_rows == 0 {
                continue;
            }

            if let Some(limit_value) = limit {
                if rows_emitted >= limit_value {
                    break;
                }
            }

            let batch_end = rows_seen + batch.num_rows;
            if batch_end <= offset {
                rows_seen = batch_end;
                continue;
            }

            let mut start_in_batch = 0usize;
            if offset > rows_seen {
                start_in_batch = offset - rows_seen;
            }

            let mut end_in_batch = batch.num_rows;
            if let Some(limit_value) = limit {
                let remaining = limit_value.saturating_sub(rows_emitted);
                if remaining == 0 {
                    break;
                }
                end_in_batch = (start_in_batch + remaining).min(batch.num_rows);
            }

            if start_in_batch >= end_in_batch {
                rows_seen = batch_end;
                continue;
            }

            let slice = batch.slice(start_in_batch, end_in_batch);
            if slice.num_rows > 0 {
                rows_emitted += slice.num_rows;
                limited_batches.push(slice);
            }
            rows_seen = batch_end;

            if let Some(limit_value) = limit {
                if rows_emitted >= limit_value {
                    break;
                }
            }
        }

        Ok(limited_batches)
    }

    fn execute_vectorized_aggregation(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        aggregate_plan: &AggregateProjectionPlan,
        group_exprs: &[Expr],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        column_ordinals: &HashMap<String, usize>,
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        having_expr: Option<&Expr>,
    ) -> Result<SelectResult, SqlExecutionError> {
        let batch = self.scan_and_filter_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
        )?;

        let group_keys = evaluate_group_keys_on_batch(group_exprs, &batch, catalog)?;

        let mut seen_group_order: HashSet<GroupKey> = HashSet::new();
        let mut group_order: Vec<GroupKey> = Vec::new();
        for key in &group_keys {
            if seen_group_order.insert(key.clone()) {
                group_order.push(key.clone());
            }
        }

        let mut group_expr_lookup: HashMap<String, usize> = HashMap::new();
        for (idx, expr) in group_exprs.iter().enumerate() {
            group_expr_lookup.insert(expr.to_string(), idx);
        }

        let mut aggregate_plans: Vec<AggregateFunctionPlan> = Vec::new();
        let mut aggregate_expr_lookup: HashMap<String, usize> = HashMap::new();
        let mut output_kinds: Vec<VectorAggregationOutput> =
            Vec::with_capacity(aggregate_plan.outputs.len());

        for projection in &aggregate_plan.outputs {
            if let Some(slot_index) = ensure_aggregate_plan_for_expr(
                &projection.expr,
                &mut aggregate_plans,
                &mut aggregate_expr_lookup,
            )? {
                output_kinds.push(VectorAggregationOutput::Aggregate { slot_index });
                continue;
            }

            if let Some(idx) = find_group_expr_index(&projection.expr, group_exprs) {
                output_kinds.push(VectorAggregationOutput::GroupExpr { group_index: idx });
                continue;
            }

            if let Some(value) = literal_value(&projection.expr) {
                output_kinds.push(VectorAggregationOutput::Literal { value });
                continue;
            }

            return Err(SqlExecutionError::Unsupported(
                    "vectorized aggregation only supports direct group expressions, literals, and simple aggregates"
                        .into(),
            ));
        }

        if let Some(expr) = having_expr {
            ensure_having_aggregate_plans(expr, &mut aggregate_plans, &mut aggregate_expr_lookup)?;
        }

        let state_template: Vec<AggregateState> = aggregate_plans
            .iter()
            .map(|plan| plan.initial_state())
            .collect();
        let mut hash_table: AggregationHashTable = HashMap::new();

        if batch.num_rows > 0 {
            if group_keys.len() != batch.num_rows {
                return Err(SqlExecutionError::OperationFailed(
                    "group key evaluation mismatch".into(),
                ));
            }

            for (slot_index, plan) in aggregate_plans.iter().enumerate() {
                match plan.kind {
                    AggregateFunctionKind::CountStar => {
                        vectorized_count_star_update(
                            &mut hash_table,
                            slot_index,
                            &group_keys,
                            &state_template,
                        );
                    }
                    AggregateFunctionKind::CountExpr => {
                        let expr = plan.arg.as_ref().ok_or_else(|| {
                            SqlExecutionError::OperationFailed(
                                "COUNT expression missing argument".into(),
                            )
                        })?;
                        let values_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
                        vectorized_count_value_update(
                            &mut hash_table,
                            slot_index,
                            &group_keys,
                            &values_page,
                            &state_template,
                        );
                    }
                    AggregateFunctionKind::Sum => {
                        let expr = plan.arg.as_ref().ok_or_else(|| {
                            SqlExecutionError::OperationFailed(
                                "SUM expression missing argument".into(),
                            )
                        })?;
                        let values_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
                        vectorized_sum_update(
                            &mut hash_table,
                            slot_index,
                            &group_keys,
                            &values_page,
                            &state_template,
                        );
                    }
                    AggregateFunctionKind::Average => {
                        let expr = plan.arg.as_ref().ok_or_else(|| {
                            SqlExecutionError::OperationFailed(
                                "AVG expression missing argument".into(),
                            )
                        })?;
                        let values_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
                        vectorized_average_update(
                            &mut hash_table,
                            slot_index,
                            &group_keys,
                            &values_page,
                            &state_template,
                        );
                    }
                }
            }
        }

        if hash_table.is_empty() && group_exprs.is_empty() {
            let key = GroupKey::empty();
            hash_table.insert(key.clone(), state_template.clone());
            if seen_group_order.insert(key.clone()) {
                group_order.push(key);
            }
        }

        if group_order.is_empty() {
            group_order.extend(hash_table.keys().cloned());
        }

        if let Some(expr) = having_expr {
            let mut filtered_order = Vec::with_capacity(group_order.len());
            for key in group_order {
                let states = hash_table.get(&key).ok_or_else(|| {
                    SqlExecutionError::OperationFailed("missing aggregate state for group".into())
                })?;
                let ctx = HavingEvalContext {
                    aggregate_plans: &aggregate_plans,
                    aggregate_lookup: &aggregate_expr_lookup,
                    group_expr_lookup: &group_expr_lookup,
                    group_key: &key,
                    states,
                };
                let predicate = evaluate_having_expression(expr, &ctx)?;
                if predicate.as_bool().unwrap_or(false) {
                    filtered_order.push(key);
                }
            }
            group_order = filtered_order;
        }

        let mut column_values: Vec<Vec<Option<String>>> =
            vec![Vec::with_capacity(group_order.len()); output_kinds.len()];
        for key in group_order {
            let states = hash_table.get(&key).ok_or_else(|| {
                SqlExecutionError::OperationFailed("missing aggregate state for group".into())
            })?;
            for (idx, kind) in output_kinds.iter().enumerate() {
                let value = match kind {
                    VectorAggregationOutput::Aggregate { slot_index } => {
                        let state = states.get(*slot_index).ok_or_else(|| {
                            SqlExecutionError::OperationFailed(
                                "missing aggregate state for group".into(),
                            )
                        })?;
                        aggregate_plans[*slot_index].finalize_value(state)
                    }
                    VectorAggregationOutput::GroupExpr { group_index } => {
                        key.value_at(*group_index).unwrap_or(None)
                    }
                    VectorAggregationOutput::Literal { value } => value.clone(),
                };
                column_values[idx].push(value);
            }
        }

        let mut result_batch = ColumnarBatch::with_capacity(column_values.len());
        let num_rows = column_values
            .first()
            .map(|values| values.len())
            .unwrap_or(0);
        result_batch.num_rows = num_rows;
        for (idx, values) in column_values.into_iter().enumerate() {
            result_batch
                .columns
                .insert(idx, strings_to_text_column(values));
        }

        let batches = if num_rows == 0 {
            Vec::new()
        } else {
            vec![result_batch]
        };

        let offset = parse_offset(offset_expr)?;
        let limit = parse_limit(limit_expr)?;
        let limited_batches = self.apply_limit_offset(batches.into_iter(), offset, limit)?;

        Ok(SelectResult {
            columns: result_columns,
            batches: limited_batches,
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

            let column_update =
                ColumnUpdate::new("*", vec![UpdateOp::BufferRow { row: final_row }]);
            let job = UpdateJob::new(table.clone(), vec![column_update]);
            self.writer.submit(job).map_err(|err| {
                SqlExecutionError::OperationFailed(format!("failed to submit insert job: {err:?}"))
            })?;
        }

        if !values.rows.is_empty() {
            self.writer.flush_table(&table).map_err(|err| {
                SqlExecutionError::OperationFailed(format!("flush failed: {err:?}"))
            })?;
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
        let sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };

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
        } else if let Some(prefixes) = sort_key_prefixes {
            self.locate_rows_by_sort_prefixes(&table_name, &sort_columns, &prefixes)?
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
        let sort_key_prefixes = if has_selection {
            collect_sort_key_prefixes(Some(&selection_expr), &sort_columns)?
        } else {
            None
        };

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
        } else if let Some(prefixes) = sort_key_prefixes {
            self.locate_rows_by_sort_prefixes(&table_name, &sort_columns, &prefixes)?
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

    fn locate_rows_by_sort_prefixes(
        &self,
        table: &str,
        sort_columns: &[ColumnCatalog],
        prefixes: &[SortKeyPrefix],
    ) -> Result<Vec<u64>, SqlExecutionError> {
        let mut result = Vec::new();
        for prefix in prefixes {
            if prefix.values.is_empty() || prefix.values.len() > sort_columns.len() {
                continue;
            }
            let columns_slice = &sort_columns[..prefix.len()];
            let rows = self.locate_rows_by_sort_tuple(table, columns_slice, &prefix.values)?;
            result.extend(rows);
        }
        Ok(result)
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
        match self.scan_matching_rows_vectorized(
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
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        if columns.is_empty() || required_ordinals.is_empty() {
            return Ok(ColumnarBatch::new());
        }

        let leading_column = &columns[0].name;
        let lead_pages = self.page_handler.list_pages_in_table(table, leading_column);
        if lead_pages.is_empty() {
            return Ok(ColumnarBatch::new());
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

        let mut predicate_ordinals = BTreeSet::new();
        for column in &predicate_columns {
            let ordinal = column_ordinals.get(column).copied().ok_or_else(|| {
                SqlExecutionError::Unsupported(format!(
                    "vectorized predicate references unknown column {column}"
                ))
            })?;
            predicate_ordinals.insert(ordinal);
        }

        let mut scan_ordinals = required_ordinals.clone();
        scan_ordinals.extend(predicate_ordinals.iter());
        if scan_ordinals.is_empty() {
            return Ok(ColumnarBatch::new());
        }

        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        for &ordinal in &scan_ordinals {
            let column = columns.get(ordinal).ok_or_else(|| {
                SqlExecutionError::OperationFailed(format!(
                    "invalid column ordinal {ordinal} on table {table}"
                ))
            })?;
            let descriptors = self.page_handler.list_pages_in_table(table, &column.name);
            if descriptors.len() != lead_pages.len() {
                return Err(SqlExecutionError::Unsupported(
                    "vectorized scan requires aligned page descriptors across columns".into(),
                ));
            }
            descriptor_map.insert(ordinal, descriptors);
        }

        let mut result_batch = ColumnarBatch::with_capacity(required_ordinals.len());

        for (page_idx, _) in lead_pages.iter().enumerate() {
            let mut page_entries: HashMap<usize, Arc<PageCacheEntryUncompressed>> =
                HashMap::with_capacity(scan_ordinals.len());
            for &ordinal in &scan_ordinals {
                let descriptors = descriptor_map.get(&ordinal).ok_or_else(|| {
                    SqlExecutionError::OperationFailed(format!(
                        "missing page descriptors for ordinal {ordinal}"
                    ))
                })?;
                let descriptor = descriptors.get(page_idx).ok_or_else(|| {
                    SqlExecutionError::OperationFailed(format!(
                        "descriptor misalignment for ordinal {ordinal}"
                    ))
                })?;
                let page = self
                    .page_handler
                    .get_page(descriptor.clone())
                    .ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "unable to load page {} for ordinal {ordinal}",
                            descriptor.id
                        ))
                    })?;
                page_entries.insert(ordinal, page);
            }

            let bitmap = if let Some(expr) = selection_expr {
                let mut predicate_refs: HashMap<String, &ColumnarPage> =
                    HashMap::with_capacity(predicate_columns.len());
                for column in &predicate_columns {
                    let ordinal = column_ordinals.get(column).copied().ok_or_else(|| {
                        SqlExecutionError::Unsupported(format!(
                            "vectorized predicate references unknown column {column}"
                        ))
                    })?;
                    let page = page_entries.get(&ordinal).ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "missing predicate column {column} for ordinal {ordinal}"
                        ))
                    })?;
                    predicate_refs.insert(column.clone(), &page.page);
                }
                let mut bitmap = evaluate_selection_on_page(expr, &predicate_refs)?;
                if bitmap.count_ones() == 0 {
                    continue;
                }
                Some(bitmap)
            } else {
                None
            };

            for &ordinal in required_ordinals {
                let page_entry = page_entries.get(&ordinal).ok_or_else(|| {
                    SqlExecutionError::OperationFailed(format!(
                        "missing page entry for ordinal {ordinal}"
                    ))
                })?;
                let filtered_page = if let Some(bitmap) = &bitmap {
                    page_entry.page.filter_by_bitmap(bitmap)
                } else {
                    page_entry.page.clone()
                };
                result_batch
                    .columns
                    .entry(ordinal)
                    .and_modify(|existing| existing.append(&filtered_page))
                    .or_insert(filtered_page);
            }
        }

        if let Some(first_col) = result_batch.columns.values().next() {
            result_batch.num_rows = first_col.num_rows;
        } else {
            result_batch.num_rows = 0;
        }

        Ok(result_batch)
    }

    fn scan_matching_rows_vectorized(
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

fn boolean_bitmap_from_page(page: &ColumnarPage) -> Result<Bitmap, SqlExecutionError> {
    match &page.data {
        ColumnData::Text(values) => {
            let mut bitmap = Bitmap::new(page.len());
            for (idx, value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                if value.eq_ignore_ascii_case("true") {
                    bitmap.set(idx);
                }
            }
            Ok(bitmap)
        }
        _ => Err(SqlExecutionError::Unsupported(
            "QUALIFY expressions must produce boolean results".into(),
        )),
    }
}

fn strings_to_text_column(values: Vec<Option<String>>) -> ColumnarPage {
    let len = values.len();
    let mut null_bitmap = Bitmap::new(len);
    let mut data: Vec<String> = Vec::with_capacity(len);
    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Some(text) => data.push(text),
            None => {
                null_bitmap.set(idx);
                data.push(String::new());
            }
        }
    }
    ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Text(data),
        null_bitmap,
        num_rows: len,
    }
}

pub struct RowIter<'a> {
    result: &'a SelectResult,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = RowView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.batch_idx < self.result.batches.len() {
            let batch = &self.result.batches[self.batch_idx];
            if self.row_idx >= batch.num_rows {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }
            let view = RowView {
                batch,
                row_idx: self.row_idx,
                column_count: self.result.columns.len(),
            };
            self.row_idx += 1;
            return Some(view);
        }
        None
    }
}

pub struct RowView<'a> {
    batch: &'a ColumnarBatch,
    row_idx: usize,
    column_count: usize,
}

impl<'a> RowView<'a> {
    pub fn value_as_string(&self, column_idx: usize) -> Option<String> {
        self.batch
            .columns
            .get(&column_idx)
            .and_then(|page| page.value_as_string(self.row_idx))
    }

    pub fn to_vec(&self) -> Vec<Option<String>> {
        let mut row = Vec::with_capacity(self.column_count);
        for column_idx in 0..self.column_count {
            row.push(
                self.batch
                    .columns
                    .get(&column_idx)
                    .and_then(|page| page.value_as_string(self.row_idx)),
            );
        }
        row
    }
}

fn rows_to_batch(rows: Vec<Vec<Option<String>>>) -> ColumnarBatch {
    if rows.is_empty() {
        return ColumnarBatch::new();
    }

    let column_count = rows[0].len();
    let mut batch = ColumnarBatch::with_capacity(column_count);
    batch.num_rows = rows.len();
    for column_idx in 0..column_count {
        let mut column_values = Vec::with_capacity(rows.len());
        for row in &rows {
            column_values.push(row.get(column_idx).cloned().unwrap_or(None));
        }
        batch
            .columns
            .insert(column_idx, strings_to_text_column(column_values));
    }
    batch
}

fn batch_to_rows(batch: &ColumnarBatch) -> Vec<Vec<Option<String>>> {
    if batch.num_rows == 0 {
        return Vec::new();
    }
    let column_count = batch.columns.len();
    let mut rows = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut row = Vec::with_capacity(column_count);
        for column_idx in 0..column_count {
            let column = batch
                .columns
                .get(&column_idx)
                .expect("missing projection column in batch");
            row.push(column.value_as_string(row_idx));
        }
        rows.push(row);
    }
    rows
}

fn batches_to_rows(batches: &[ColumnarBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch_to_rows(batch));
    }
    rows
}

#[derive(Hash, PartialEq, Eq)]
enum DistinctValue {
    Null,
    Int(i64),
    Float(u64),
    Text(String),
}

#[derive(Hash, PartialEq, Eq)]
struct DistinctKey {
    values: Vec<DistinctValue>,
}

fn build_distinct_key(batch: &ColumnarBatch, row_idx: usize, column_count: usize) -> DistinctKey {
    let mut values = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        let value = match batch.columns.get(&column_idx) {
            Some(page) => match &page.data {
                ColumnData::Int64(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Int(data[row_idx])
                    }
                }
                ColumnData::Float64(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Float(data[row_idx].to_bits())
                    }
                }
                ColumnData::Text(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Text(data[row_idx].clone())
                    }
                }
            },
            None => DistinctValue::Null,
        };
        values.push(value);
    }
    DistinctKey { values }
}

fn deduplicate_batches(batches: Vec<ColumnarBatch>, column_count: usize) -> Vec<ColumnarBatch> {
    if batches.is_empty() || column_count == 0 {
        return batches;
    }

    let mut seen: HashSet<DistinctKey> = HashSet::new();
    let mut deduped: Vec<ColumnarBatch> = Vec::new();

    for batch in batches.into_iter() {
        if batch.num_rows == 0 {
            continue;
        }
        let mut indices: Vec<usize> = Vec::new();
        for row_idx in 0..batch.num_rows {
            let key = build_distinct_key(&batch, row_idx, column_count);
            if seen.insert(key) {
                indices.push(row_idx);
            }
        }
        if !indices.is_empty() {
            deduped.push(batch.gather(&indices));
        }
    }
    deduped
}

fn chunk_batch(batch: &ColumnarBatch, chunk_size: usize) -> Vec<ColumnarBatch> {
    if batch.num_rows == 0 {
        return Vec::new();
    }
    if batch.num_rows <= chunk_size {
        return vec![batch.clone()];
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < batch.num_rows {
        let end = (start + chunk_size).min(batch.num_rows);
        chunks.push(batch.slice(start, end));
        start = end;
    }
    chunks
}

fn merge_batches(mut batches: Vec<ColumnarBatch>) -> ColumnarBatch {
    if batches.is_empty() {
        return ColumnarBatch::new();
    }
    let mut merged = batches.remove(0);
    for batch in batches {
        merged.append(&batch);
    }
    merged
}
