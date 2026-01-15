use super::SqlExecutionError;
use super::aggregates::AggregateDataset;
use super::batch::{ColumnData, ColumnarBatch, ColumnarPage};
use super::expressions::{evaluate_expression_on_batch, evaluate_scalar_expression};
use super::helpers::column_name_from_expr;
use super::values::{ScalarValue, compare_scalar_values, compare_strs, format_float};
use crate::metadata_store::TableCatalog;
use sqlparser::ast::Expr;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct OrderClause {
    pub(crate) expr: Expr,
    pub(crate) descending: bool,
    pub(crate) nulls: NullsPlacement,
}

#[derive(Clone, Copy)]
pub(crate) enum NullsPlacement {
    Default,
    First,
    Last,
}

#[derive(Clone)]
pub(crate) struct OrderKey {
    pub(crate) values: Vec<ScalarValue>,
}

#[derive(Clone)]
struct OrderColumn {
    ordinal: usize,
    descending: bool,
    nulls: NullsPlacement,
}

// Legacy rowwise ordering helpers removed after pipeline refactor.

pub(crate) fn compare_order_keys(
    left: &OrderKey,
    right: &OrderKey,
    clauses: &[OrderClause],
) -> Ordering {
    for (idx, clause) in clauses.iter().enumerate() {
        let lhs = &left.values[idx];
        let rhs = &right.values[idx];
        let ord = compare_scalar_with_clause(lhs, rhs, clause);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

pub(crate) fn build_group_order_key(
    clauses: &[OrderClause],
    dataset: &AggregateDataset,
) -> Result<OrderKey, SqlExecutionError> {
    if clauses.is_empty() {
        return Ok(OrderKey { values: Vec::new() });
    }

    let mut values = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let value = evaluate_scalar_expression(&clause.expr, dataset)?;
        values.push(value);
    }
    Ok(OrderKey { values })
}

fn compare_scalar_with_clause(
    left: &ScalarValue,
    right: &ScalarValue,
    clause: &OrderClause,
) -> Ordering {
    let left_null = left.is_null();
    let right_null = right.is_null();

    if left_null || right_null {
        if left_null && right_null {
            return Ordering::Equal;
        }

        return match clause.nulls {
            NullsPlacement::First => {
                if left_null {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            NullsPlacement::Last => {
                if left_null {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            NullsPlacement::Default => {
                if clause.descending {
                    if left_null {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else if left_null {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        };
    }

    let mut ord = compare_scalar_values(left, right).unwrap_or_else(|| {
        let left_str = scalar_to_string(left);
        let right_str = scalar_to_string(right);
        compare_strs(&left_str, &right_str)
    });
    if clause.descending {
        ord = ord.reverse();
    }
    ord
}

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::Float64(value) => format_float(*value),
        ScalarValue::String(text) => text.clone(),
        ScalarValue::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        ScalarValue::Timestamp(ts) => ts.to_string(),
        ScalarValue::Null => "NULL".to_string(),
    }
}

pub(crate) fn build_order_keys_on_batch(
    clauses: &[OrderClause],
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Result<Vec<OrderKey>, SqlExecutionError> {
    if clauses.is_empty() || batch.num_rows == 0 {
        return Ok(vec![OrderKey { values: Vec::new() }; batch.num_rows]);
    }

    let mut evaluated_columns: Vec<ColumnarPage> = Vec::with_capacity(clauses.len());
    for clause in clauses {
        evaluated_columns.push(evaluate_expression_on_batch(&clause.expr, batch, catalog)?);
    }

    let mut keys = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let mut values = Vec::with_capacity(clauses.len());
        for column in &evaluated_columns {
            values.push(column_scalar_value(column, row_idx));
        }
        keys.push(OrderKey { values });
    }
    Ok(keys)
}

pub(crate) fn sort_batch_in_memory(
    batch: &ColumnarBatch,
    clauses: &[OrderClause],
    catalog: &TableCatalog,
) -> Result<ColumnarBatch, SqlExecutionError> {
    sort_batch_in_memory_with_limit(batch, clauses, catalog, None)
}

pub(crate) fn sort_batch_in_memory_with_limit(
    batch: &ColumnarBatch,
    clauses: &[OrderClause],
    catalog: &TableCatalog,
    limit: Option<usize>,
) -> Result<ColumnarBatch, SqlExecutionError> {
    if clauses.is_empty() || batch.num_rows <= 1 {
        return Ok(batch.clone());
    }

    let mut indices: Vec<usize> = (0..batch.num_rows).collect();
    if let Some(order_columns) = extract_order_columns(clauses, batch, catalog) {
        let compare = |left: &usize, right: &usize| {
            let ordering = compare_rows_by_columns(batch, &order_columns, *left, *right);
            if ordering == Ordering::Equal {
                let left_id = batch.row_ids.get(*left).copied().unwrap_or(*left as u64);
                let right_id = batch.row_ids.get(*right).copied().unwrap_or(*right as u64);
                left_id.cmp(&right_id)
            } else {
                ordering
            }
        };

        if let Some(limit) = limit
            && limit < indices.len()
        {
            indices.select_nth_unstable_by(limit, compare);
            indices[..limit].sort_unstable_by(compare);
            return Ok(batch.gather(&indices[..limit]));
        }

        indices.sort_unstable_by(compare);
        return Ok(batch.gather(&indices));
    }

    let order_keys = build_order_keys_on_batch(clauses, batch, catalog)?;
    let compare = |left: &usize, right: &usize| {
        let ordering = compare_order_keys(&order_keys[*left], &order_keys[*right], clauses);
        if ordering == Ordering::Equal {
            let left_id = batch.row_ids.get(*left).copied().unwrap_or(*left as u64);
            let right_id = batch.row_ids.get(*right).copied().unwrap_or(*right as u64);
            left_id.cmp(&right_id)
        } else {
            ordering
        }
    };

    if let Some(limit) = limit
        && limit < indices.len()
    {
        indices.select_nth_unstable_by(limit, compare);
        indices[..limit].sort_unstable_by(compare);
        return Ok(batch.gather(&indices[..limit]));
    }

    indices.sort_unstable_by(compare);
    Ok(batch.gather(&indices))
}

fn extract_order_columns(
    clauses: &[OrderClause],
    batch: &ColumnarBatch,
    catalog: &TableCatalog,
) -> Option<Vec<OrderColumn>> {
    let mut columns = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let name = column_name_from_expr(&clause.expr)?;
        let column = catalog.column(&name)?;
        if !batch.columns.contains_key(&column.ordinal) {
            return None;
        }
        columns.push(OrderColumn {
            ordinal: column.ordinal,
            descending: clause.descending,
            nulls: clause.nulls,
        });
    }
    Some(columns)
}

fn compare_rows_by_columns(
    batch: &ColumnarBatch,
    order_columns: &[OrderColumn],
    left_idx: usize,
    right_idx: usize,
) -> Ordering {
    for column in order_columns {
        let Some(page) = batch.columns.get(&column.ordinal) else {
            continue;
        };
        let left_null = page.null_bitmap.is_set(left_idx);
        let right_null = page.null_bitmap.is_set(right_idx);
        if let Some(null_order) = compare_nulls(left_null, right_null, column) {
            if null_order != Ordering::Equal {
                return null_order;
            }
            continue;
        }

        let mut ord = match &page.data {
            ColumnData::Int64(values) => values[left_idx].cmp(&values[right_idx]),
            ColumnData::Timestamp(values) => values[left_idx].cmp(&values[right_idx]),
            ColumnData::Float64(values) => values[left_idx]
                .partial_cmp(&values[right_idx])
                .unwrap_or(Ordering::Equal),
            ColumnData::Boolean(values) => values[left_idx].cmp(&values[right_idx]),
            ColumnData::Text(col) => col.get_bytes(left_idx).cmp(col.get_bytes(right_idx)),
            ColumnData::Dictionary(dict) => {
                dict.get_bytes(left_idx).cmp(dict.get_bytes(right_idx))
            }
        };
        if column.descending {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn compare_nulls(left_null: bool, right_null: bool, clause: &OrderColumn) -> Option<Ordering> {
    if !left_null && !right_null {
        return None;
    }
    if left_null && right_null {
        return Some(Ordering::Equal);
    }

    let ordering = match clause.nulls {
        NullsPlacement::First => {
            if left_null {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        NullsPlacement::Last => {
            if left_null {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        NullsPlacement::Default => {
            if clause.descending {
                if left_null {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            } else if left_null {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    };
    Some(ordering)
}

fn column_scalar_value(page: &ColumnarPage, idx: usize) -> ScalarValue {
    if page.null_bitmap.is_set(idx) {
        return ScalarValue::Null;
    }
    match &page.data {
        ColumnData::Int64(values) => ScalarValue::Int64(values[idx]),
        ColumnData::Float64(values) => ScalarValue::Float64(values[idx]),
        // Legacy bridge: allocates String for ordering compatibility
        ColumnData::Text(col) => ScalarValue::String(col.get_string(idx)),
        ColumnData::Boolean(values) => ScalarValue::Boolean(values[idx]),
        ColumnData::Timestamp(values) => ScalarValue::Timestamp(values[idx]),
        // Legacy bridge: allocates String for ordering compatibility
        ColumnData::Dictionary(dict) => ScalarValue::String(dict.get_string(idx)),
    }
}

pub(crate) struct MergeOperator {
    runs: Vec<MergeRun>,
    heap: BinaryHeap<HeapItem>,
    batch_capacity: usize,
    clauses: Arc<Vec<OrderClause>>,
    limit: Option<usize>,
    produced: usize,
}

impl MergeOperator {
    pub fn new(
        runs: Vec<ColumnarBatch>,
        clauses: &[OrderClause],
        catalog: &TableCatalog,
        batch_capacity: usize,
        limit: Option<usize>,
    ) -> Result<Self, SqlExecutionError> {
        if runs.is_empty() {
            return Ok(Self {
                runs: Vec::new(),
                heap: BinaryHeap::new(),
                batch_capacity,
                clauses: Arc::new(Vec::new()),
                limit,
                produced: 0,
            });
        }

        let clauses_arc = Arc::new(clauses.to_vec());
        let mut merge_runs = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();

        for batch in runs.into_iter() {
            if batch.num_rows == 0 {
                continue;
            }
            let keys = build_order_keys_on_batch(clauses_arc.as_slice(), &batch, catalog)?;
            let run_idx = merge_runs.len();
            let row_ids = if batch.row_ids.is_empty() {
                (0..batch.num_rows as u64).collect()
            } else {
                batch.row_ids.clone()
            };
            merge_runs.push(MergeRun {
                batch,
                keys,
                row_ids,
            });
            let key = merge_runs[run_idx].keys.first().cloned().ok_or_else(|| {
                SqlExecutionError::OperationFailed("missing order key for merge run".into())
            })?;
            let row_id = merge_runs[run_idx].row_ids.first().copied().unwrap_or(0);
            heap.push(HeapItem::new(
                run_idx,
                0,
                row_id,
                key,
                Arc::clone(&clauses_arc),
            ));
        }

        Ok(Self {
            runs: merge_runs,
            heap,
            batch_capacity,
            clauses: clauses_arc,
            limit,
            produced: 0,
        })
    }

    pub fn next_batch(&mut self) -> Result<Option<ColumnarBatch>, SqlExecutionError> {
        if self.heap.is_empty() {
            return Ok(None);
        }
        if let Some(limit) = self.limit
            && self.produced >= limit
        {
            return Ok(None);
        }

        let mut emitted = 0;
        let remaining = self.limit.map(|limit| limit - self.produced);
        let batch_target = remaining.unwrap_or(self.batch_capacity).min(self.batch_capacity);
        let mut chunks: Vec<RowChunk> = Vec::new();

        while emitted < batch_target {
            let Some(item) = self.heap.pop() else {
                break;
            };

            emitted += 1;
            if let Some(chunk) = chunks.last_mut() {
                if chunk.run_idx == item.run_idx && chunk.end_row == item.row_idx {
                    chunk.end_row += 1;
                } else {
                    chunks.push(RowChunk::new(item.run_idx, item.row_idx));
                }
            } else {
                chunks.push(RowChunk::new(item.run_idx, item.row_idx));
            }

            let next_row = item.row_idx + 1;
            if let Some(next_key) = self.runs[item.run_idx].keys.get(next_row).cloned() {
                let row_id = self.runs[item.run_idx]
                    .row_ids
                    .get(next_row)
                    .copied()
                    .unwrap_or(next_row as u64);
                self.heap.push(HeapItem::new(
                    item.run_idx,
                    next_row,
                    row_id,
                    next_key,
                    Arc::clone(&self.clauses),
                ));
            }
        }

        if chunks.is_empty() {
            return Ok(None);
        }

        let mut output = ColumnarBatch::new();
        for chunk in chunks {
            let slice = self.runs[chunk.run_idx]
                .batch
                .slice(chunk.start_row, chunk.end_row);
            output.append(&slice);
        }
        self.produced += emitted;
        Ok(Some(output))
    }
}

struct MergeRun {
    batch: ColumnarBatch,
    keys: Vec<OrderKey>,
    row_ids: Vec<u64>,
}

#[derive(Clone)]
struct HeapItem {
    run_idx: usize,
    row_idx: usize,
    row_id: u64,
    key: OrderKey,
    clauses: Arc<Vec<OrderClause>>,
}

impl HeapItem {
    fn new(
        run_idx: usize,
        row_idx: usize,
        row_id: u64,
        key: OrderKey,
        clauses: Arc<Vec<OrderClause>>,
    ) -> Self {
        Self {
            run_idx,
            row_idx,
            row_id,
            key,
            clauses,
        }
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx && self.row_idx == other.row_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match compare_order_keys(&self.key, &other.key, self.clauses.as_slice()) {
            Ordering::Equal => other
                .row_id
                .cmp(&self.row_id)
                .then_with(|| other.run_idx.cmp(&self.run_idx))
                .then_with(|| other.row_idx.cmp(&self.row_idx)),
            ord => ord.reverse(),
        }
    }
}

struct RowChunk {
    run_idx: usize,
    start_row: usize,
    end_row: usize,
}

impl RowChunk {
    fn new(run_idx: usize, start_row: usize) -> Self {
        Self {
            run_idx,
            start_row,
            end_row: start_row + 1,
        }
    }
}
