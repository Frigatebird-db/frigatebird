mod aggregates;
mod aggregation_exec;
pub mod batch;
mod expressions;
mod grouping_helpers;
pub(crate) mod helpers;
mod ordering;
pub(crate) mod physical_evaluator;
mod projection_helpers;
mod scan_stream;
mod dml;
mod select;
mod scan_helpers_exec;
mod row_functions;
mod scalar_functions;
mod scan_helpers;
mod spill;
pub mod values;
mod window_helpers;

use self::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use self::spill::SpillManager;
use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{
    ColumnCatalog, ColumnStats, ColumnStatsKind, JournalColumnDef, MetaJournal, MetaRecord,
    PageDescriptor, PageDirectory, ROWS_PER_PAGE_GROUP, TableCatalog,
};
use crate::ops_handler::{
    create_table_from_plan, delete_row, insert_sorted_row, overwrite_row, read_row,
};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::pipeline::{Job, PipelineBatch, PipelineStep};
use crate::sql::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::planner::ExpressionPlanner;
use crate::sql::{CreateTablePlan, PlannerError, plan_create_table_statement};
use crate::wal::{FsyncSchedule, ReadConsistency, Walrus};
use crate::writer::{
    ColumnUpdate, DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator,
    UpdateJob, UpdateOp, Writer,
};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, FromTable, FunctionArg, FunctionArgExpr, GroupByExpr, Ident,
    ObjectName, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value, WindowType,
};
use sqlparser::parser::ParserError;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::sql::types::DataType;
use aggregates::{
    AggregateDataset, AggregateFunctionKind, AggregateFunctionPlan, AggregateProjection,
    AggregateProjectionPlan, AggregateState, AggregationHashTable, MaterializedColumns,
    ensure_state_vec, evaluate_aggregate_outputs, plan_aggregate_projection,
    select_item_contains_aggregate, vectorized_average_update,
    vectorized_count_distinct_update, vectorized_count_star_update,
    vectorized_count_value_update, vectorized_max_update, vectorized_min_update,
    vectorized_sum_update, vectorized_variance_update,
};
use expressions::{evaluate_expression_on_batch, evaluate_row_expr, evaluate_scalar_expression};
use grouping_helpers::{
    evaluate_group_key, evaluate_group_keys_on_batch, evaluate_having, validate_group_by,
};
use helpers::{
    collect_expr_column_ordinals, column_name_from_expr, expr_to_string, object_name_to_string,
    parse_limit, parse_offset, table_with_joins_to_name,
};
use ordering::{
    MergeOperator, NullsPlacement, OrderClause, OrderKey, build_group_order_key,
    compare_order_keys, sort_batch_in_memory, sort_rows_logical,
};
use physical_evaluator::{PhysicalEvaluator, filter_supported};
use projection_helpers::{build_projection, materialize_columns};
use scan_stream::{
    BatchStream, PipelineBatchStream, PipelineScanBuilder, RowIdBatchStream, SingleBatchStream,
    merge_stream_to_batch,
};
use scan_helpers::{SortKeyPrefix, collect_sort_key_filters, collect_sort_key_prefixes};
use values::{
    CachedValue, ScalarValue, cached_to_scalar_with_type, combine_numeric, compare_scalar_values,
    compare_strs, scalar_from_f64,
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
        Expr::Value(Value::SingleQuotedString(text)) => Ok(ScalarValue::String(text.clone())),
        Expr::Value(Value::Boolean(flag)) => Ok(ScalarValue::Boolean(*flag)),
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
                BinaryOperator::And => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) && rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) || rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ScalarValue::Boolean(
                    lhs.as_bool().unwrap_or(false) ^ rhs.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Eq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::NotEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord != Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Gt => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater)
                        .unwrap_or(false),
                )),
                BinaryOperator::GtEq => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Greater || ord == Ordering::Equal)
                        .unwrap_or(false),
                )),
                BinaryOperator::Lt => Ok(ScalarValue::Boolean(
                    compare_scalar_values(&lhs, &rhs)
                        .map(|ord| ord == Ordering::Less)
                        .unwrap_or(false),
                )),
                BinaryOperator::LtEq => Ok(ScalarValue::Boolean(
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
                UnaryOperator::Not => Ok(ScalarValue::Boolean(!value.as_bool().unwrap_or(false))),
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
        Some(text) => ScalarValue::String(text),
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
                if let Some(name) = column_name_from_expr(expr) {
                    names.push(name);
                } else {
                    return Ok(GroupByStrategy::Hash);
                }
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

    Ok(GroupByStrategy::Hash)
}

enum GroupByStrategy {
    SortPrefix,
    OrderAligned,
    Hash,
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

fn rewrite_aliases_in_expr(expr: &Expr, alias_map: &HashMap<String, Expr>) -> Expr {
    use sqlparser::ast::Expr::*;

    match expr {
        Identifier(ident) => alias_map
            .get(&ident.value)
            .cloned()
            .unwrap_or_else(|| Identifier(ident.clone())),
        CompoundIdentifier(idents) => {
            if idents.len() == 1 {
                if let Some(replacement) = alias_map.get(&idents[0].value) {
                    return replacement.clone();
                }
            }
            CompoundIdentifier(idents.clone())
        }
        BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_aliases_in_expr(left, alias_map)),
            op: op.clone(),
            right: Box::new(rewrite_aliases_in_expr(right, alias_map)),
        },
        UnaryOp { op, expr } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_aliases_in_expr(expr, alias_map)),
        },
        Nested(inner) => Expr::Nested(Box::new(rewrite_aliases_in_expr(inner, alias_map))),
        Function(function) => {
            let mut function = function.clone();
            for arg in &mut function.args {
                match arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(inner) = arg {
                            *inner = rewrite_aliases_in_expr(inner, alias_map);
                        }
                    }
                }
            }
            if let Some(filter) = &mut function.filter {
                *filter = Box::new(rewrite_aliases_in_expr(filter, alias_map));
            }
            for order in &mut function.order_by {
                order.expr = rewrite_aliases_in_expr(&order.expr, alias_map);
            }
            if let Some(WindowType::WindowSpec(spec)) = &mut function.over {
                for expr in &mut spec.partition_by {
                    *expr = rewrite_aliases_in_expr(expr, alias_map);
                }
                for order in &mut spec.order_by {
                    order.expr = rewrite_aliases_in_expr(&order.expr, alias_map);
                }
            }
            Expr::Function(function)
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| Box::new(rewrite_aliases_in_expr(expr, alias_map))),
            conditions: conditions
                .iter()
                .map(|expr| rewrite_aliases_in_expr(expr, alias_map))
                .collect(),
            results: results
                .iter()
                .map(|expr| rewrite_aliases_in_expr(expr, alias_map))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|expr| Box::new(rewrite_aliases_in_expr(expr, alias_map))),
        },
        _ => expr.clone(),
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
    ScalarExpr { expr: Expr },
}

#[derive(Clone)]
enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    Sum { frame: SumWindowFrame },
    Lag { offset: usize },
    Lead { offset: usize },
    FirstValue { preceding: Option<usize> },
    LastValue { preceding: Option<usize> },
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

static SQL_EXECUTOR_WAL_COUNTER: AtomicUsize = AtomicUsize::new(0);
const SQL_EXECUTOR_WAL_PREFIX: &str = "sql-executor-";

pub struct SqlExecutor {
    page_handler: Arc<PageHandler>,
    page_directory: Arc<PageDirectory>,
    writer: Arc<Writer>,
    meta_journal: Option<Arc<MetaJournal>>,
    use_writer_inserts: bool,
    wal_namespace: String,
    meta_namespace: String,
    cleanup_wal_on_drop: bool,
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
        let wal_id = SQL_EXECUTOR_WAL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let wal_namespace = format!("{SQL_EXECUTOR_WAL_PREFIX}{wal_id}");
        let options = SqlExecutorWalOptions::new(wal_namespace);
        SqlExecutor::with_wal_options(page_handler, page_directory, use_writer_inserts, options)
    }

    pub fn with_wal_options(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
        options: SqlExecutorWalOptions,
    ) -> Self {
        let SqlExecutorWalOptions {
            namespace,
            cleanup_on_drop,
            reset_namespace,
            storage_dir,
            fsync_schedule,
            wal_enabled,
        } = options;
        let allocator: Arc<dyn PageAllocator> = if let Some(dir) = storage_dir {
            Arc::new(DirectBlockAllocator::with_data_dir(dir).expect("allocator init failed"))
        } else {
            Arc::new(DirectBlockAllocator::new().expect("allocator init failed"))
        };
        ensure_sql_executor_wal_root();
        let meta_namespace = format!("{namespace}-meta");
        if reset_namespace {
            remove_sql_executor_wal_dir(&namespace);
            remove_sql_executor_wal_dir(&meta_namespace);
        }
        let wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &namespace,
                ReadConsistency::StrictlyAtOnce,
                fsync_schedule,
            )
            .expect("wal init failed"),
        );
        let meta_wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &meta_namespace,
                ReadConsistency::AtLeastOnce {
                    persist_every: u32::MAX,
                },
                FsyncSchedule::SyncEach,
            )
            .expect("metadata wal init failed"),
        );
        let meta_journal = Arc::new(MetaJournal::new(Arc::clone(&meta_wal), 16));
        meta_journal
            .replay_into(&page_directory)
            .expect("metadata journal replay failed");
        let metadata_client: Arc<dyn MetadataClient> = Arc::new(DirectoryMetadataClient::new(
            Arc::clone(&page_directory),
            Arc::clone(&meta_journal),
        ));

        let shard_count = crate::writer::GLOBAL_WRITER_SHARD_COUNT
            .load(AtomicOrdering::Acquire)
            .max(1);

        let writer = Arc::new(Writer::with_shard_count(
            Arc::clone(&page_handler),
            allocator,
            metadata_client,
            wal,
            shard_count,
            wal_enabled,
        ));

        SqlExecutor {
            page_handler,
            page_directory,
            writer,
            meta_journal: Some(meta_journal),
            use_writer_inserts,
            wal_namespace: namespace,
            meta_namespace,
            cleanup_wal_on_drop: cleanup_on_drop,
        }
    }

    pub fn flush_table(&self, table: &str) -> Result<(), SqlExecutionError> {
        self.writer.flush_table(table).map_err(|err| {
            SqlExecutionError::OperationFailed(format!("writer flush failed for {table}: {err:?}"))
        })
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

    // select.rs provides impl SqlExecutor for SELECT flow.

    fn execute_vectorized_projection(
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
            column_ordinals,
            catalog.rows_per_page_group,
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
        final_batch.row_ids = batch.row_ids.clone();
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

    fn apply_filter_expr(
        &self,
        batch: ColumnarBatch,
        expr: &Expr,
        physical_expr: Option<&PhysicalExpr>,
        catalog: &TableCatalog,
        table: &str,
        columns: &[ColumnCatalog],
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
    ) -> Result<ColumnarBatch, SqlExecutionError> {
        if batch.num_rows == 0 {
            return Ok(batch);
        }

        if let Some(physical_expr) = physical_expr {
            let bitmap = PhysicalEvaluator::evaluate_filter(physical_expr, &batch);
            return Ok(batch.filter_by_bitmap(&bitmap));
        }

        match evaluate_expression_on_batch(expr, &batch, catalog) {
            Ok(filter_page) => {
                let bitmap = boolean_bitmap_from_page(&filter_page)?;
                Ok(batch.filter_by_bitmap(&bitmap))
            }
            Err(SqlExecutionError::Unsupported(_)) => {
                let ordinals = collect_expr_column_ordinals(expr, column_ordinals, table)?;
                let materialized = materialize_columns(
                    &self.page_handler,
                    table,
                    columns,
                    &ordinals,
                    &batch.row_ids,
                )?;
                let matching_rows = filter_rows_with_expr(
                    expr,
                    &batch.row_ids,
                    &materialized,
                    column_ordinals,
                    column_types,
                    false,
                )?;
                if matching_rows.is_empty() {
                    return Ok(ColumnarBatch::new());
                }
                let matching: HashSet<u64> = matching_rows.into_iter().collect();
                let mut bitmap = Bitmap::new(batch.num_rows);
                for (idx, row_id) in batch.row_ids.iter().enumerate() {
                    if matching.contains(row_id) {
                        bitmap.set(idx);
                    }
                }
                Ok(batch.filter_by_bitmap(&bitmap))
            }
            Err(err) => Err(err),
        }
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

        return Ok(SelectResult {
            columns: result_columns,
            batches,
        });
    }

    fn execute_grouping_set_aggregation_rows(
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

        let mut group_expr_lookup: HashMap<String, usize> = HashMap::new();
        for (idx, expr) in group_exprs.iter().enumerate() {
            group_expr_lookup.insert(expr.to_string(), idx);
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

    fn execute_create(&self, statement: Statement) -> Result<(), SqlExecutionError> {
        let plan: CreateTablePlan = plan_create_table_statement(&statement)?;
        create_table_from_plan(&self.page_directory, &plan)
            .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;

        // Write table schema to metadata journal for crash recovery
        if let Some(ref journal) = self.meta_journal {
            let columns: Vec<JournalColumnDef> = plan
                .columns
                .iter()
                .map(|spec| JournalColumnDef {
                    name: spec.name.clone(),
                    data_type: crate::sql::types::DataType::from_sql(&spec.data_type)
                        .unwrap_or(crate::sql::types::DataType::String),
                })
                .collect();
            let record = MetaRecord::CreateTable {
                name: plan.table_name.clone(),
                columns,
                sort_key: plan.order_by.clone(),
                rows_per_page_group: ROWS_PER_PAGE_GROUP,
            };
            journal
                .append_commit(&plan.table_name, &record)
                .map_err(|err| SqlExecutionError::OperationFailed(err.to_string()))?;
        }

        Ok(())
    }

    // dml.rs provides impl SqlExecutor for DML flow.
    // scan_helpers_exec.rs provides impl SqlExecutor for scan/index helpers.

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

fn collect_physical_ordinals(expr: &PhysicalExpr, ordinals: &mut BTreeSet<usize>) {
    match expr {
        PhysicalExpr::Column { index, .. } => {
            ordinals.insert(*index);
        }
        PhysicalExpr::BinaryOp { left, right, .. } => {
            collect_physical_ordinals(left, ordinals);
            collect_physical_ordinals(right, ordinals);
        }
        PhysicalExpr::UnaryOp { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Like {
            expr,
            pattern,
            case_insensitive: _,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::RLike {
            expr,
            pattern,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::InList { expr, list, .. } => {
            collect_physical_ordinals(expr, ordinals);
            for item in list {
                collect_physical_ordinals(item, ordinals);
            }
        }
        PhysicalExpr::Cast { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::IsNull(expr) | PhysicalExpr::IsNotNull(expr) => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Literal(_) => {}
    }
}

fn filter_rows_with_expr(
    expr: &Expr,
    rows: &[u64],
    materialized: &MaterializedColumns,
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
    prefer_exact_numeric: bool,
) -> Result<Vec<u64>, SqlExecutionError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let dataset = AggregateDataset {
        rows,
        materialized,
        column_ordinals,
        column_types,
        masked_exprs: None,
        prefer_exact_numeric,
    };

    let mut filtered = Vec::with_capacity(rows.len());
    for &row_idx in rows {
        let value = evaluate_row_expr(expr, row_idx, &dataset)?;
        if value.as_bool().unwrap_or(false) {
            filtered.push(row_idx);
        }
    }

    Ok(filtered)
}

fn refine_rows_with_vectorized_filter(
    page_handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    expr: &PhysicalExpr,
    column_ordinals: &HashMap<String, usize>,
    candidate_rows: &[u64],
    rows_per_page_group: u64,
) -> Result<Vec<u64>, SqlExecutionError> {
    if candidate_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ordinals = BTreeSet::new();
    collect_physical_ordinals(expr, &mut ordinals);
    if ordinals.is_empty() {
        return Ok(candidate_rows.to_vec());
    }

    let mut rows = Vec::new();
    let mut idx = 0;
    while idx < candidate_rows.len() {
        let row = candidate_rows[idx];
        let page_idx = (row / rows_per_page_group) as usize;
        let page_base = (page_idx as u64) * rows_per_page_group;

        let mut page_rows = Vec::new();
        while idx < candidate_rows.len()
            && (candidate_rows[idx] / rows_per_page_group) as usize == page_idx
        {
            page_rows.push((candidate_rows[idx] - page_base) as usize);
            idx += 1;
        }

        let mut batch_slice = ColumnarBatch::with_capacity(ordinals.len());
        let mut pages_keepalive = Vec::with_capacity(ordinals.len());
        let mut num_rows = 0;
        let mut segment_valid = true;

        for &ordinal in &ordinals {
            let column = &columns[ordinal];
            let col_descriptors = page_handler.list_pages_in_table(table, &column.name);
            if let Some(desc) = col_descriptors.get(page_idx) {
                let page_arc = page_handler
                    .get_page(desc.clone())
                    .ok_or_else(|| SqlExecutionError::OperationFailed("page load failed".into()))?;
                num_rows = page_arc.page.num_rows;
                pages_keepalive.push((ordinal, page_arc));
            } else {
                segment_valid = false;
                break;
            }
        }

        if !segment_valid || num_rows == 0 {
            continue;
        }

        for (ordinal, page_arc) in &pages_keepalive {
            batch_slice.columns.insert(*ordinal, page_arc.page.clone());
        }
        batch_slice.num_rows = num_rows;
        batch_slice.aliases = column_ordinals.clone();

        let mut bitmap = PhysicalEvaluator::evaluate_filter(expr, &batch_slice);
        let mut candidate_bitmap = Bitmap::new(num_rows);
        for offset in page_rows {
            candidate_bitmap.set(offset);
        }
        bitmap.and(&candidate_bitmap);

        for offset in bitmap.iter_ones() {
            rows.push(page_base + offset as u64);
        }
    }

    Ok(rows)
}

impl Drop for SqlExecutor {
    fn drop(&mut self) {
        if self.cleanup_wal_on_drop {
            remove_sql_executor_wal_dir(&self.wal_namespace);
            remove_sql_executor_wal_dir(&self.meta_namespace);
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqlExecutorWalOptions {
    namespace: String,
    cleanup_on_drop: bool,
    reset_namespace: bool,
    storage_dir: Option<String>,
    fsync_schedule: FsyncSchedule,
    wal_enabled: bool,
}

impl SqlExecutorWalOptions {
    pub fn new(namespace: impl Into<String>) -> Self {
        SqlExecutorWalOptions {
            namespace: namespace.into(),
            cleanup_on_drop: true,
            reset_namespace: true,
            storage_dir: None,
            fsync_schedule: FsyncSchedule::SyncEach,
            wal_enabled: true,
        }
    }

    pub fn cleanup_on_drop(mut self, value: bool) -> Self {
        self.cleanup_on_drop = value;
        self
    }

    pub fn reset_namespace(mut self, value: bool) -> Self {
        self.reset_namespace = value;
        self
    }

    pub fn storage_dir(mut self, dir: impl Into<String>) -> Self {
        self.storage_dir = Some(dir.into());
        self
    }

    pub fn fsync_schedule(mut self, schedule: FsyncSchedule) -> Self {
        self.fsync_schedule = schedule;
        self
    }

    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

fn sql_executor_wal_base_dir() -> PathBuf {
    std::env::var("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("wal_files"))
}

fn ensure_sql_executor_wal_root() {
    let _ = fs::create_dir_all(sql_executor_wal_base_dir());
}

fn remove_sql_executor_wal_dir(namespace: &str) {
    let dir = sql_executor_wal_base_dir().join(namespace);
    if dir.exists() {
        let _ = fs::remove_dir_all(dir);
    }
}

fn boolean_bitmap_from_page(page: &ColumnarPage) -> Result<Bitmap, SqlExecutionError> {
    match &page.data {
        ColumnData::Text(col) => {
            let mut bitmap = Bitmap::new(page.len());
            for idx in 0..col.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let value = col.get_bytes(idx);
                if value.eq_ignore_ascii_case(b"true") {
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
    let mut col = BytesColumn::with_capacity(len, len * 16);
    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Some(text) => col.push(&text),
            None => {
                null_bitmap.set(idx);
                col.push("");
            }
        }
    }
    ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Text(col),
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
    batch.row_ids = (0..rows.len() as u64).collect();
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
    Boolean(bool),
    Timestamp(i64),
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
                ColumnData::Text(col) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Text(col.get_string(row_idx))
                    }
                }
                ColumnData::Boolean(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Boolean(data[row_idx])
                    }
                }
                ColumnData::Timestamp(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Timestamp(data[row_idx])
                    }
                }
                ColumnData::Dictionary(dict) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Text(dict.get_string(row_idx))
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

#[derive(Debug, Clone)]
struct PagePrunePredicate {
    column: String,
    comparison: PagePruneComparison,
    value: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
enum PagePruneComparison {
    GreaterThan { inclusive: bool },
    LessThan { inclusive: bool },
    Equal,
    IsNull,
    IsNotNull,
}

fn extract_page_prunable_predicates(expr: &Expr) -> Option<Vec<PagePrunePredicate>> {
    let mut predicates = Vec::new();
    if gather_numeric_prunable_predicates(expr, &mut predicates) && !predicates.is_empty() {
        Some(predicates)
    } else {
        None
    }
}

fn gather_numeric_prunable_predicates(expr: &Expr, acc: &mut Vec<PagePrunePredicate>) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                gather_numeric_prunable_predicates(left, acc)
                    && gather_numeric_prunable_predicates(right, acc)
            }
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Eq => build_prunable_comparison(left, op, right, acc),
            _ => false,
        },
        Expr::IsNull(inner) => build_null_prunable_predicate(inner, true, acc),
        Expr::IsNotNull(inner) => build_null_prunable_predicate(inner, false, acc),
        Expr::Nested(inner) => gather_numeric_prunable_predicates(inner, acc),
        _ => false,
    }
}

fn build_prunable_comparison(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(left) {
        if let Some(value) = parse_numeric_literal(right) {
            if let Some(comparison) = comparison_for_operator(op, true) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    if let Some(column) = column_name_from_expr(right) {
        if let Some(value) = parse_numeric_literal(left) {
            if let Some(comparison) = comparison_for_operator(op, false) {
                acc.push(PagePrunePredicate {
                    column,
                    comparison,
                    value: Some(value),
                });
                return true;
            }
        }
    }

    false
}

fn build_null_prunable_predicate(
    expr: &Expr,
    expect_null: bool,
    acc: &mut Vec<PagePrunePredicate>,
) -> bool {
    if let Some(column) = column_name_from_expr(expr) {
        acc.push(PagePrunePredicate {
            column,
            comparison: if expect_null {
                PagePruneComparison::IsNull
            } else {
                PagePruneComparison::IsNotNull
            },
            value: None,
        });
        return true;
    }
    false
}

fn comparison_for_operator(
    op: &BinaryOperator,
    column_on_left: bool,
) -> Option<PagePruneComparison> {
    match op {
        BinaryOperator::Gt => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: false }
        } else {
            PagePruneComparison::LessThan { inclusive: false }
        }),
        BinaryOperator::GtEq => Some(if column_on_left {
            PagePruneComparison::GreaterThan { inclusive: true }
        } else {
            PagePruneComparison::LessThan { inclusive: true }
        }),
        BinaryOperator::Lt => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: false }
        } else {
            PagePruneComparison::GreaterThan { inclusive: false }
        }),
        BinaryOperator::LtEq => Some(if column_on_left {
            PagePruneComparison::LessThan { inclusive: true }
        } else {
            PagePruneComparison::GreaterThan { inclusive: true }
        }),
        BinaryOperator::Eq => Some(PagePruneComparison::Equal),
        _ => None,
    }
}

fn parse_numeric_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<f64>().ok(),
        Expr::Value(Value::SingleQuotedString(text)) => text.parse::<f64>().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => parse_numeric_literal(expr).map(|value| -value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => parse_numeric_literal(expr),
        Expr::Nested(inner) => parse_numeric_literal(inner),
        _ => None,
    }
}

fn should_prune_page(
    page_idx: usize,
    predicates: &[PagePrunePredicate],
    descriptor_map: &HashMap<usize, Vec<PageDescriptor>>,
    column_ordinals: &HashMap<String, usize>,
) -> bool {
    for predicate in predicates {
        let ordinal = match column_ordinals.get(&predicate.column) {
            Some(ord) => *ord,
            None => continue,
        };
        let descriptors = match descriptor_map.get(&ordinal) {
            Some(list) => list,
            None => continue,
        };
        let descriptor = match descriptors.get(page_idx) {
            Some(desc) => desc,
            None => continue,
        };
        if let Some(stats) = &descriptor.stats {
            match predicate.comparison {
                PagePruneComparison::IsNull => {
                    if stats.null_count == 0 {
                        return true;
                    }
                }
                PagePruneComparison::IsNotNull => {
                    if stats.null_count == descriptor.entry_count {
                        return true;
                    }
                }
                _ => {
                    if matches!(
                        stats.kind,
                        ColumnStatsKind::Int64 | ColumnStatsKind::Float64
                    ) && predicate_disqualifies(stats, predicate)
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn predicate_disqualifies(stats: &ColumnStats, predicate: &PagePrunePredicate) -> bool {
    let value = match predicate.value {
        Some(v) => v,
        None => return false,
    };
    let min = stats
        .min_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());
    let max = stats
        .max_value
        .as_ref()
        .and_then(|value| value.parse::<f64>().ok());

    match predicate.comparison {
        PagePruneComparison::GreaterThan { inclusive } => match max {
            Some(max_val) => {
                if inclusive {
                    max_val < value
                } else {
                    max_val <= value
                }
            }
            None => false,
        },
        PagePruneComparison::LessThan { inclusive } => match min {
            Some(min_val) => {
                if inclusive {
                    min_val > value
                } else {
                    min_val >= value
                }
            }
            None => false,
        },
        PagePruneComparison::Equal => match (min, max) {
            (Some(min_val), Some(max_val)) => value < min_val || value > max_val,
            _ => false,
        },
        PagePruneComparison::IsNull | PagePruneComparison::IsNotNull => false,
    }
}

#[cfg(test)]
mod pruning_tests {
    use super::*;
    use std::collections::HashMap;

    fn column_expr(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn number_expr(value: &str) -> Expr {
        Expr::Value(Value::Number(value.into(), false))
    }

    #[test]
    fn extract_prunable_predicates_from_conjunction() {
        let greater_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Gt,
            right: Box::new(number_expr("100")),
        };
        let less_expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::LtEq,
            right: Box::new(number_expr("500")),
        };
        let expr = Expr::BinaryOp {
            left: Box::new(greater_expr),
            op: BinaryOperator::And,
            right: Box::new(less_expr),
        };

        let predicates = extract_page_prunable_predicates(&expr).expect("predicates");
        assert_eq!(predicates.len(), 2);
        assert!(matches!(
            predicates[0].comparison,
            PagePruneComparison::GreaterThan { inclusive: false }
        ));
        assert!(matches!(
            predicates[1].comparison,
            PagePruneComparison::LessThan { inclusive: true }
        ));
    }

    #[test]
    fn unsupported_expression_returns_none() {
        let expr = Expr::BinaryOp {
            left: Box::new(column_expr("price")),
            op: BinaryOperator::Or,
            right: Box::new(number_expr("10")),
        };
        assert!(extract_page_prunable_predicates(&expr).is_none());
    }

    fn descriptor_with_stats(stats: ColumnStats, entry_count: u64) -> PageDescriptor {
        PageDescriptor {
            id: "test".into(),
            disk_path: "/tmp/pg".into(),
            offset: 0,
            alloc_len: 0,
            actual_len: 0,
            entry_count,
            data_type: crate::sql::types::DataType::Int64,
            stats: Some(stats),
        }
    }

    #[test]
    fn should_prune_when_range_disjoint() {
        let stats = ColumnStats {
            min_value: Some("0".into()),
            max_value: Some("10".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::GreaterThan { inclusive: false },
            value: Some(25.0),
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn does_not_prune_when_overlap_exists() {
        let stats = ColumnStats {
            min_value: Some("5".into()),
            max_value: Some("50".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 10);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::LessThan { inclusive: true },
            value: Some(30.0),
        };
        assert!(!should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn prunes_is_null_when_page_has_no_nulls() {
        let stats = ColumnStats {
            min_value: Some("1".into()),
            max_value: Some("2".into()),
            null_count: 0,
            kind: ColumnStatsKind::Int64,
        };
        let descriptor = descriptor_with_stats(stats, 8);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNull,
            value: None,
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }

    #[test]
    fn prunes_is_not_null_when_page_all_nulls() {
        let stats = ColumnStats {
            min_value: None,
            max_value: None,
            null_count: 4,
            kind: ColumnStatsKind::Text,
        };
        let descriptor = descriptor_with_stats(stats, 4);
        let mut descriptor_map: HashMap<usize, Vec<PageDescriptor>> = HashMap::new();
        descriptor_map.insert(0, vec![descriptor]);
        let mut pred_map = HashMap::new();
        pred_map.insert("price".into(), 0usize);

        let predicate = PagePrunePredicate {
            column: "price".into(),
            comparison: PagePruneComparison::IsNotNull,
            value: None,
        };
        assert!(should_prune_page(
            0,
            &[predicate],
            &descriptor_map,
            &pred_map
        ));
    }
}
