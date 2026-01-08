use crate::page_handler::PageHandler;
use crate::sql::executor::batch::{Bitmap, ColumnData, ColumnarPage};
use crate::sql::executor::helpers::like_match;
use crate::sql::executor::physical_evaluator::{evaluate_col_lit, reverse_operator};
use crate::sql::executor::values::ScalarValue;
use crate::sql::models::FilterExpr;
use crate::sql::physical_plan::PhysicalExpr;
use crossbeam::channel::{Receiver, Sender};
use sqlparser::ast::BinaryOperator;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn evaluate_filters(filters: &[FilterExpr], column_index: usize, page: &ColumnarPage) -> Bitmap {
    if filters.is_empty() {
        let mut bitmap = Bitmap::new(page.num_rows);
        bitmap.fill(true);
        return bitmap;
    }
    let mut bitmap = evaluate_filter_expr(&filters[0], column_index, page);
    for filter in filters.iter().skip(1) {
        let rhs = evaluate_filter_expr(filter, column_index, page);
        bitmap.and(&rhs);
    }
    bitmap
}

fn evaluate_filter_expr(filter: &FilterExpr, column_index: usize, page: &ColumnarPage) -> Bitmap {
    match filter {
        FilterExpr::Leaf(expr) => evaluate_physical_expr(expr, column_index, page),
        FilterExpr::And(filters) => {
            if filters.is_empty() {
                let mut bitmap = Bitmap::new(page.num_rows);
                bitmap.fill(true);
                return bitmap;
            }
            let mut bitmap = evaluate_filter_expr(&filters[0], column_index, page);
            for expr in filters.iter().skip(1) {
                let rhs = evaluate_filter_expr(expr, column_index, page);
                bitmap.and(&rhs);
            }
            bitmap
        }
        FilterExpr::Or(filters) => {
            let mut bitmap = Bitmap::new(page.num_rows);
            for expr in filters {
                let rhs = evaluate_filter_expr(expr, column_index, page);
                bitmap.or(&rhs);
            }
            bitmap
        }
    }
}

fn evaluate_physical_expr(expr: &PhysicalExpr, column_index: usize, page: &ColumnarPage) -> Bitmap {
    match expr {
        PhysicalExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let mut lhs = evaluate_physical_expr(left, column_index, page);
                let rhs = evaluate_physical_expr(right, column_index, page);
                lhs.and(&rhs);
                lhs
            }
            BinaryOperator::Or => {
                let mut lhs = evaluate_physical_expr(left, column_index, page);
                let rhs = evaluate_physical_expr(right, column_index, page);
                lhs.or(&rhs);
                lhs
            }
            _ => evaluate_binary_op(left, op, right, column_index, page),
        },
        PhysicalExpr::UnaryOp { op, expr } => {
            use sqlparser::ast::UnaryOperator;
            match op {
                UnaryOperator::Not => {
                    let mut bitmap = evaluate_physical_expr(expr, column_index, page);
                    bitmap.invert();
                    bitmap
                }
                _ => Bitmap::new(page.num_rows),
            }
        }
        PhysicalExpr::Column { index, .. } => {
            if *index == column_index {
                if let ColumnData::Boolean(values) = &page.data {
                    let mut bitmap = Bitmap::new(page.num_rows);
                    for (i, &v) in values.iter().enumerate() {
                        if v {
                            bitmap.set(i);
                        }
                    }
                    return bitmap;
                }
            }
            Bitmap::new(page.num_rows)
        }
        PhysicalExpr::Literal(ScalarValue::Boolean(b)) => {
            let mut bitmap = Bitmap::new(page.num_rows);
            if *b {
                bitmap.fill(true);
            }
            bitmap
        }
        PhysicalExpr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => evaluate_like(
            expr,
            pattern,
            *case_insensitive,
            *negated,
            column_index,
            page,
        ),
        PhysicalExpr::InList {
            expr,
            list,
            negated,
        } => evaluate_in_list(expr, list, *negated, column_index, page),
        PhysicalExpr::Cast { expr, .. } => evaluate_physical_expr(expr, column_index, page),
        _ => Bitmap::new(page.num_rows),
    }
}

fn evaluate_binary_op(
    left: &PhysicalExpr,
    op: &BinaryOperator,
    right: &PhysicalExpr,
    column_index: usize,
    page: &ColumnarPage,
) -> Bitmap {
    if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(val)) = (left, right) {
        if *index == column_index {
            return evaluate_col_lit(&page.data, &page.null_bitmap, op, val, page.num_rows);
        }
    }
    if let (PhysicalExpr::Literal(val), PhysicalExpr::Column { index, .. }) = (left, right) {
        if *index == column_index {
            if let Some(rev_op) = reverse_operator(op) {
                return evaluate_col_lit(
                    &page.data,
                    &page.null_bitmap,
                    &rev_op,
                    val,
                    page.num_rows,
                );
            }
        }
    }
    Bitmap::new(page.num_rows)
}

fn evaluate_like(
    expr: &PhysicalExpr,
    pattern: &PhysicalExpr,
    case_insensitive: bool,
    negated: bool,
    column_index: usize,
    page: &ColumnarPage,
) -> Bitmap {
    let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(ScalarValue::String(pat))) =
        (expr, pattern)
    else {
        return Bitmap::new(page.num_rows);
    };
    if *index != column_index {
        return Bitmap::new(page.num_rows);
    }
    let ColumnData::Text(col) = &page.data else {
        return Bitmap::new(page.num_rows);
    };
    let mut bitmap = Bitmap::new(page.num_rows);
    for idx in 0..col.len() {
        if page.null_bitmap.is_set(idx) {
            continue;
        }
        // LIKE requires string semantics
        let value = col.get_string(idx);
        let matches = like_match(&value, pat, !case_insensitive);
        if if negated { !matches } else { matches } {
            bitmap.set(idx);
        }
    }
    bitmap
}

fn evaluate_in_list(
    expr: &PhysicalExpr,
    list: &[PhysicalExpr],
    negated: bool,
    column_index: usize,
    page: &ColumnarPage,
) -> Bitmap {
    let PhysicalExpr::Column { index, .. } = expr else {
        return Bitmap::new(page.num_rows);
    };
    if *index != column_index {
        return Bitmap::new(page.num_rows);
    }
    let mut bitmap = Bitmap::new(page.num_rows);
    if list.is_empty() {
        return bitmap;
    }
    match &page.data {
        ColumnData::Int64(values) => {
            let mut set = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let Some(i) = scalar_to_i64(val) {
                        set.insert(i);
                    }
                }
            }
            for (idx, &value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let matches = set.contains(&value);
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
        ColumnData::Float64(values) => {
            let mut set = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let Some(f) = scalar_to_f64(val) {
                        set.insert(f.to_bits());
                    }
                }
            }
            for (idx, &value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let matches = set.contains(&value.to_bits());
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
        ColumnData::Boolean(values) => {
            let mut set = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(ScalarValue::Boolean(val)) = item {
                    set.insert(*val);
                }
            }
            for (idx, &value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let matches = set.contains(&value);
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
        ColumnData::Timestamp(values) => {
            let mut set = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(ScalarValue::Timestamp(val)) = item {
                    set.insert(*val);
                }
            }
            for (idx, &value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let matches = set.contains(&value);
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
        ColumnData::Text(col) => {
            // Build HashSet of literal values as bytes for fast comparison
            let mut set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let Some(text) = scalar_to_string(val) {
                        set.insert(text.into_bytes());
                    }
                }
            }
            for idx in 0..col.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let row_bytes = col.get_bytes(idx);
                let matches = set.contains(row_bytes);
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
        ColumnData::Dictionary(dict) => {
            // Build set of matching keys for dictionary optimization
            let mut set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let Some(text) = scalar_to_string(val) {
                        set.insert(text.into_bytes());
                    }
                }
            }
            // Pre-compute which dictionary keys match
            let mut matching_keys = std::collections::HashSet::new();
            for key in 0..dict.values.len() {
                if set.contains(dict.values.get_bytes(key)) {
                    matching_keys.insert(key as u16);
                }
            }
            for idx in 0..dict.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let key = dict.keys[idx];
                let matches = matching_keys.contains(&key);
                if (matches && !negated) || (!matches && negated) {
                    bitmap.set(idx);
                }
            }
        }
    }
    bitmap
}

fn scalar_to_i64(val: &ScalarValue) -> Option<i64> {
    match val {
        ScalarValue::Int64(i) => Some(*i),
        ScalarValue::Float64(f) if f.is_finite() && f.fract().abs() < f64::EPSILON => {
            Some(*f as i64)
        }
        ScalarValue::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn scalar_to_f64(val: &ScalarValue) -> Option<f64> {
    match val {
        ScalarValue::Float64(f) => Some(*f),
        ScalarValue::Int64(i) => Some(*i as f64),
        ScalarValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn scalar_to_string(val: &ScalarValue) -> Option<String> {
    match val {
        ScalarValue::String(s) => Some(s.clone()),
        ScalarValue::Int64(i) => Some(i.to_string()),
        ScalarValue::Float64(f) => Some(f.to_string()),
        ScalarValue::Boolean(b) => Some(b.to_string()),
        ScalarValue::Timestamp(ts) => Some(ts.to_string()),
        ScalarValue::Null => None,
    }
}

pub type PipelineBatch = Vec<usize>;

#[derive(Clone)]
pub struct PipelineStep {
    pub current_producer: Sender<PipelineBatch>,
    pub previous_receiver: Receiver<PipelineBatch>,
    pub column: String,
    pub column_ordinal: usize,
    pub filters: Vec<FilterExpr>,
    pub is_root: bool,
    pub table: String,
    pub page_handler: Arc<PageHandler>,
}

impl std::fmt::Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("column", &self.column)
            .field("column_ordinal", &self.column_ordinal)
            .field("filters", &self.filters)
            .field("is_root", &self.is_root)
            .field("table", &self.table)
            .finish()
    }
}

impl PipelineStep {
    pub fn new(
        table: String,
        column: String,
        column_ordinal: usize,
        filters: Vec<FilterExpr>,
        is_root: bool,
        page_handler: Arc<PageHandler>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
    ) -> Self {
        Self {
            current_producer,
            previous_receiver,
            column,
            column_ordinal,
            filters,
            is_root,
            table,
            page_handler,
        }
    }

    pub fn execute(&self) {
        if self.is_root {
            self.execute_root();
        } else {
            self.execute_non_root();
        }
    }

    fn execute_root(&self) {
        let descriptors = self
            .page_handler
            .list_pages_in_table(&self.table, &self.column);
        let page_ids: Vec<String> = descriptors.iter().map(|d| d.id.clone()).collect();
        self.page_handler.ensure_pages_cached(&page_ids);
        let pages = self.page_handler.get_pages(descriptors);

        let mut base_row = 0usize;
        for page in pages {
            let page_len = page.page.num_rows;
            if page_len == 0 {
                continue;
            }
            let bitmap = evaluate_filters(&self.filters, self.column_ordinal, &page.page);
            let mut passing_rows = Vec::new();
            for idx in bitmap.iter_ones().take_while(|&idx| idx < page_len) {
                passing_rows.push(base_row + idx);
            }

            if !passing_rows.is_empty() {
                if self.current_producer.send(passing_rows).is_err() {
                    return;
                }
            }

            base_row += page_len;
        }
        let _ = self.current_producer.send(Vec::new());
    }

    fn execute_non_root(&self) {
        let mut sent_termination = false;
        while let Ok(mut batch) = self.previous_receiver.recv() {
            if batch.is_empty() {
                if self.current_producer.send(Vec::new()).is_err() {
                    return;
                }
                sent_termination = true;
                break;
            }

            let mut grouped: HashMap<
                String,
                (crate::metadata_store::PageDescriptor, Vec<(usize, usize)>),
            > = HashMap::new();
            for row_id in &batch {
                let row_id_u64 = *row_id as u64;
                if let Some(location) =
                    self.page_handler
                        .locate_row_in_table(&self.table, &self.column, row_id_u64)
                {
                    grouped
                        .entry(location.descriptor.id.clone())
                        .or_insert_with(|| (location.descriptor.clone(), Vec::new()))
                        .1
                        .push((*row_id, location.page_row_index as usize));
                }
            }

            let mut filtered = Vec::with_capacity(batch.len());
            for (_, (descriptor, rows)) in grouped {
                let page = match self.page_handler.get_page(descriptor.clone()) {
                    Some(page) => page,
                    None => continue,
                };
                let page_len = page.page.num_rows;
                if page_len == 0 {
                    continue;
                }
                let bitmap = evaluate_filters(&self.filters, self.column_ordinal, &page.page);
                for (row_id, page_idx) in rows {
                    if page_idx < page_len && bitmap.is_set(page_idx) {
                        filtered.push(row_id);
                    }
                }
            }
            batch = filtered;

            if batch.is_empty() {
                continue;
            }

            if self.current_producer.send(batch).is_err() {
                sent_termination = true;
                break;
            }
        }
        if !sent_termination {
            let _ = self.current_producer.send(Vec::new());
        }
    }
}

pub struct Job {
    pub table_name: String,
    pub steps: Vec<PipelineStep>,
    pub cost: usize,
    pub next_free_slot: AtomicUsize,
    pub id: String,
    pub entry_producer: Sender<PipelineBatch>,
}

impl Job {
    pub fn new(
        table_name: String,
        steps: Vec<PipelineStep>,
        entry_producer: Sender<PipelineBatch>,
    ) -> Self {
        let cost = steps.len();
        Job {
            table_name,
            steps,
            cost,
            next_free_slot: AtomicUsize::new(0),
            id: super::builder::generate_pipeline_id(),
            entry_producer,
        }
    }

    pub fn get_next(&self) {
        let total = self.steps.len();
        if total == 0 {
            return;
        }

        let mut slot = self.next_free_slot.load(AtomicOrdering::Relaxed);
        loop {
            if slot >= total {
                return;
            }

            match self.next_free_slot.compare_exchange_weak(
                slot,
                slot + 1,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    self.steps[slot].execute();
                    return;
                }
                Err(current) => {
                    slot = current;
                }
            }
        }
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by cost first (fewer steps = higher priority)
        // Then by table name for deterministic ordering
        self.cost
            .cmp(&other.cost)
            .then_with(|| self.table_name.cmp(&other.table_name))
    }
}
