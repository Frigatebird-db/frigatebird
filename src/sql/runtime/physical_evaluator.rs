use super::batch::{Bitmap, ColumnData, ColumnarBatch, ColumnarPage};
use super::helpers::{like_match, regex_match};
use super::values::ScalarValue;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::types::DataType;
use sqlparser::ast::BinaryOperator;
use std::collections::HashSet;

pub struct PhysicalEvaluator;

pub(crate) fn filter_supported(expr: &PhysicalExpr) -> bool {
    match expr {
        PhysicalExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                filter_supported(left) && filter_supported(right)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                (is_column_or_cast(left) && is_literal_or_cast(right))
                    || (is_literal_or_cast(left) && is_column_or_cast(right))
            }
            _ => false,
        },
        PhysicalExpr::UnaryOp { op, expr } => {
            matches!(op, sqlparser::ast::UnaryOperator::Not) && filter_supported(expr)
        }
        PhysicalExpr::Like { expr, pattern, .. } => {
            is_column_or_cast(expr) && is_literal_or_cast(pattern)
        }
        PhysicalExpr::RLike { expr, pattern, .. } => {
            is_column_or_cast(expr) && is_literal_or_cast(pattern)
        }
        PhysicalExpr::InList { expr, list, .. } => {
            is_column_or_cast(expr) && list.iter().all(is_literal_or_cast)
        }
        PhysicalExpr::IsNull(expr) | PhysicalExpr::IsNotNull(expr) => is_column_or_cast(expr),
        PhysicalExpr::Column { .. } => false,
        PhysicalExpr::Literal(ScalarValue::Boolean(_)) => true,
        PhysicalExpr::Literal(_) => false,
        PhysicalExpr::Cast { .. } => false,
    }
}

fn is_column_or_cast(expr: &PhysicalExpr) -> bool {
    match expr {
        PhysicalExpr::Column { .. } => true,
        PhysicalExpr::Cast { expr, .. } => matches!(**expr, PhysicalExpr::Column { .. }),
        _ => false,
    }
}

fn is_literal_or_cast(expr: &PhysicalExpr) -> bool {
    match expr {
        PhysicalExpr::Literal(_) => true,
        PhysicalExpr::Cast { expr, .. } => matches!(**expr, PhysicalExpr::Literal(_)),
        _ => false,
    }
}

impl PhysicalEvaluator {
    /// Evaluates a boolean expression against a batch, returning a selection Bitmap.
    pub fn evaluate_filter(expr: &PhysicalExpr, batch: &ColumnarBatch) -> Bitmap {
        match expr {
            PhysicalExpr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let mut lhs = Self::evaluate_filter(left, batch);
                    let rhs = Self::evaluate_filter(right, batch);
                    lhs.and(&rhs);
                    lhs
                }
                BinaryOperator::Or => {
                    let mut lhs = Self::evaluate_filter(left, batch);
                    let rhs = Self::evaluate_filter(right, batch);
                    lhs.or(&rhs);
                    lhs
                }
                _ => Self::evaluate_binary_op(left, op, right, batch),
            },
            PhysicalExpr::UnaryOp { op, expr } => {
                use sqlparser::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        let mut bitmap = Self::evaluate_filter(expr, batch);
                        bitmap.invert();
                        bitmap
                    }
                    _ => Bitmap::new(batch.num_rows),
                }
            }
            PhysicalExpr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
            } => evaluate_like(expr, pattern, *case_insensitive, *negated, batch),
            PhysicalExpr::RLike {
                expr,
                pattern,
                negated,
            } => evaluate_rlike(expr, pattern, *negated, batch),
            PhysicalExpr::InList {
                expr,
                list,
                negated,
            } => evaluate_in_list(expr, list, *negated, batch),
            PhysicalExpr::Column { index, .. } => {
                // Boolean column used directly as predicate
                if let Some(page) = batch.columns.get(index)
                    && let ColumnData::Boolean(values) = &page.data
                {
                    return build_word_bitmap(
                        values,
                        &page.null_bitmap,
                        batch.num_rows,
                        |v| *v,
                    );
                }
                Bitmap::new(batch.num_rows)
            }
            PhysicalExpr::Literal(ScalarValue::Boolean(b)) => {
                let mut bm = Bitmap::new(batch.num_rows);
                if *b {
                    bm.fill(true);
                }
                bm
            }
            PhysicalExpr::Cast { expr, target_type } => {
                // For now, casts in WHERE are limited or not fully supported in filter pushdown
                // Simplest fallback: evaluate inner, ignore cast if types are compatible-ish?
                // Actually, if we just ignore it, we might be wrong.
                // Let's return empty for safety or unsupported.
                Bitmap::new(batch.num_rows)
            }
            PhysicalExpr::IsNull(expr) => evaluate_is_null(expr, batch),
            PhysicalExpr::IsNotNull(expr) => {
                let mut bitmap = evaluate_is_null(expr, batch);
                bitmap.invert();
                bitmap
            }
            _ => {
                // Unsupported expression - return empty bitmap (safe fallback)
                Bitmap::new(batch.num_rows)
            }
        }
    }

    fn evaluate_binary_op(
        left: &PhysicalExpr,
        op: &BinaryOperator,
        right: &PhysicalExpr,
        batch: &ColumnarBatch,
    ) -> Bitmap {
        let left_unwrapped = strip_cast(left);
        let right_unwrapped = strip_cast(right);

        // Most common case: Column <Op> Literal
        if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(val)) =
            (left_unwrapped, right_unwrapped)
            && let Some(col_page) = batch.columns.get(index)
        {
            return evaluate_col_lit(
                &col_page.data,
                &col_page.null_bitmap,
                op,
                val,
                batch.num_rows,
            );
        }

        // Handle Literal <Op> Column (Flip it)
        if let (PhysicalExpr::Literal(val), PhysicalExpr::Column { index, .. }) =
            (left_unwrapped, right_unwrapped)
            && let Some(col_page) = batch.columns.get(index)
            && let Some(rev_op) = reverse_operator(op)
        {
            return evaluate_col_lit(
                &col_page.data,
                &col_page.null_bitmap,
                &rev_op,
                val,
                batch.num_rows,
            );
        }

        Bitmap::new(batch.num_rows) // Fallback empty
    }
}

fn strip_cast(expr: &PhysicalExpr) -> &PhysicalExpr {
    if let PhysicalExpr::Cast { expr, .. } = expr {
        strip_cast(expr)
    } else {
        expr
    }
}

pub fn reverse_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator::*;
    match op {
        Gt => Some(Lt),
        GtEq => Some(LtEq),
        Lt => Some(Gt),
        LtEq => Some(GtEq),
        Eq => Some(Eq),
        NotEq => Some(NotEq),
        _ => None,
    }
}

pub(crate) fn evaluate_col_lit(
    col: &ColumnData,
    null_bitmap: &Bitmap,
    op: &BinaryOperator,
    lit: &ScalarValue,
    num_rows: usize,
) -> Bitmap {
    let mut bitmap = Bitmap::new(num_rows);

    match (col, lit) {
        // ---------------------------------------------------------
        // FAST PATH: INT64 / TIMESTAMP
        // ---------------------------------------------------------
        (ColumnData::Int64(vec), ScalarValue::Int64(val))
        | (ColumnData::Timestamp(vec), ScalarValue::Timestamp(val)) => {
            let target = *val;
            match op {
                BinaryOperator::Eq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v == target);
                }
                BinaryOperator::NotEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v != target);
                }
                BinaryOperator::Gt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v > target);
                }
                BinaryOperator::GtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v >= target);
                }
                BinaryOperator::Lt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v < target);
                }
                BinaryOperator::LtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v <= target);
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // FAST PATH: FLOAT64
        // ---------------------------------------------------------
        (ColumnData::Float64(vec), ScalarValue::Float64(val)) => {
            let target = *val;
            match op {
                BinaryOperator::Eq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v - target).abs() < f64::EPSILON
                    });
                }
                BinaryOperator::NotEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v - target).abs() >= f64::EPSILON
                    });
                }
                BinaryOperator::Gt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v > target);
                }
                BinaryOperator::GtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v >= target);
                }
                BinaryOperator::Lt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v < target);
                }
                BinaryOperator::LtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v <= target);
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // MIXED NUMERIC PATHS
        // ---------------------------------------------------------
        (ColumnData::Int64(vec), ScalarValue::Float64(val)) => {
            let target = *val;
            match op {
                BinaryOperator::Eq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64 - target).abs() < f64::EPSILON
                    });
                }
                BinaryOperator::NotEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64 - target).abs() >= f64::EPSILON
                    });
                }
                BinaryOperator::Gt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64) > target
                    });
                }
                BinaryOperator::GtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64) >= target
                    });
                }
                BinaryOperator::Lt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64) < target
                    });
                }
                BinaryOperator::LtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v as f64) <= target
                    });
                }
                _ => {}
            }
        }
        (ColumnData::Float64(vec), ScalarValue::Int64(val)) => {
            let target = *val as f64;
            match op {
                BinaryOperator::Eq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v - target).abs() < f64::EPSILON
                    });
                }
                BinaryOperator::NotEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| {
                        (*v - target).abs() >= f64::EPSILON
                    });
                }
                BinaryOperator::Gt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v > target);
                }
                BinaryOperator::GtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v >= target);
                }
                BinaryOperator::Lt => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v < target);
                }
                BinaryOperator::LtEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v <= target);
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // FAST PATH: BOOLEAN
        // ---------------------------------------------------------
        (ColumnData::Boolean(vec), ScalarValue::Boolean(val)) => {
            let target = *val;
            match op {
                BinaryOperator::Eq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v == target);
                }
                BinaryOperator::NotEq => {
                    bitmap = build_word_bitmap(vec, null_bitmap, num_rows, |v| *v != target);
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // FAST PATH: STRING with length-first optimization
        // ---------------------------------------------------------
        (ColumnData::Text(col), ScalarValue::String(val)) => {
            let val_bytes = val.as_bytes();
            let val_len = val_bytes.len();

            match op {
                BinaryOperator::Eq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_len = col.get_len(i);
                        if row_len != val_len {
                            return false;
                        }
                        let row_bytes = col.get_bytes(i);
                        row_bytes == val_bytes
                    });
                }
                BinaryOperator::NotEq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_len = col.get_len(i);
                        if row_len != val_len {
                            return true;
                        }
                        let row_bytes = col.get_bytes(i);
                        row_bytes != val_bytes
                    });
                }
                BinaryOperator::Gt => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = col.get_bytes(i);
                        row_bytes > val_bytes
                    });
                }
                BinaryOperator::GtEq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = col.get_bytes(i);
                        row_bytes >= val_bytes
                    });
                }
                BinaryOperator::Lt => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = col.get_bytes(i);
                        row_bytes < val_bytes
                    });
                }
                BinaryOperator::LtEq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = col.get_bytes(i);
                        row_bytes <= val_bytes
                    });
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // FAST PATH: DICTIONARY with key-based optimization
        // ---------------------------------------------------------
        (ColumnData::Dictionary(dict), ScalarValue::String(val)) => {
            let val_bytes = val.as_bytes();

            match op {
                BinaryOperator::Eq => {
                    // Key optimization: look up literal in dictionary ONCE
                    // If found, compare integer keys instead of bytes
                    if let Some(target_key) = dict.find_key(val_bytes) {
                        bitmap = build_word_bitmap(
                            &dict.keys,
                            null_bitmap,
                            num_rows,
                            |key| *key == target_key,
                        );
                    }
                    // If value not in dictionary, no rows can match - bitmap stays empty
                }
                BinaryOperator::NotEq => {
                    if let Some(target_key) = dict.find_key(val_bytes) {
                        bitmap = build_word_bitmap(
                            &dict.keys,
                            null_bitmap,
                            num_rows,
                            |key| *key != target_key,
                        );
                    } else {
                        // Value not in dictionary - all non-null rows match
                        bitmap = build_non_null_bitmap(num_rows, null_bitmap);
                    }
                }
                // For ordering comparisons, fall back to byte comparison
                // (dictionary order doesn't preserve lexicographic order)
                BinaryOperator::Gt => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = dict.get_bytes(i);
                        row_bytes > val_bytes
                    });
                }
                BinaryOperator::GtEq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = dict.get_bytes(i);
                        row_bytes >= val_bytes
                    });
                }
                BinaryOperator::Lt => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = dict.get_bytes(i);
                        row_bytes < val_bytes
                    });
                }
                BinaryOperator::LtEq => {
                    bitmap = build_indexed_word_bitmap(num_rows, null_bitmap, |i| {
                        let row_bytes = dict.get_bytes(i);
                        row_bytes <= val_bytes
                    });
                }
                _ => {}
            }
        }

        // Dictionary with numeric literal - parse and compare
        (ColumnData::Dictionary(dict), ScalarValue::Int64(val)) => {
            let target = *val as f64;
            for i in 0..num_rows {
                if null_bitmap.is_set(i) {
                    continue;
                }
                let s = dict.get_string(i);
                let Ok(num) = s.parse::<f64>() else {
                    continue;
                };
                match op {
                    BinaryOperator::Eq => {
                        if (num - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::NotEq => {
                        if (num - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Gt => {
                        if num > target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::GtEq => {
                        if num >= target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Lt => {
                        if num < target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::LtEq => {
                        if num <= target {
                            bitmap.set(i);
                        }
                    }
                    _ => {}
                }
            }
        }
        (ColumnData::Dictionary(dict), ScalarValue::Float64(val)) => {
            let target = *val;
            for i in 0..num_rows {
                if null_bitmap.is_set(i) {
                    continue;
                }
                let s = dict.get_string(i);
                let Ok(num) = s.parse::<f64>() else {
                    continue;
                };
                match op {
                    BinaryOperator::Eq => {
                        if (num - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::NotEq => {
                        if (num - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Gt => {
                        if num > target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::GtEq => {
                        if num >= target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Lt => {
                        if num < target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::LtEq => {
                        if num <= target {
                            bitmap.set(i);
                        }
                    }
                    _ => {}
                }
            }
        }

        // ---------------------------------------------------------
        // TEXT COLUMN WITH NUMERIC LITERAL (legacy path with allocation)
        // ---------------------------------------------------------
        (ColumnData::Text(col), ScalarValue::Int64(val)) => {
            let target = *val as f64;
            for i in 0..num_rows {
                if null_bitmap.is_set(i) {
                    continue;
                }
                let s = col.get_string(i);
                let Ok(num) = s.parse::<f64>() else {
                    continue;
                };
                match op {
                    BinaryOperator::Eq => {
                        if (num - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::NotEq => {
                        if (num - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Gt => {
                        if num > target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::GtEq => {
                        if num >= target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Lt => {
                        if num < target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::LtEq => {
                        if num <= target {
                            bitmap.set(i);
                        }
                    }
                    _ => {}
                }
            }
        }
        (ColumnData::Text(col), ScalarValue::Float64(val)) => {
            let target = *val;
            for i in 0..num_rows {
                if null_bitmap.is_set(i) {
                    continue;
                }
                let s = col.get_string(i);
                let Ok(num) = s.parse::<f64>() else {
                    continue;
                };
                match op {
                    BinaryOperator::Eq => {
                        if (num - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::NotEq => {
                        if (num - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Gt => {
                        if num > target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::GtEq => {
                        if num >= target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::Lt => {
                        if num < target {
                            bitmap.set(i);
                        }
                    }
                    BinaryOperator::LtEq => {
                        if num <= target {
                            bitmap.set(i);
                        }
                    }
                    _ => {}
                }
            }
        }

        _ => {
            // Mismatched types - return empty bitmap as safe fallback
        }
    }

    bitmap
}

fn build_word_bitmap<T, F>(
    values: &[T],
    null_bitmap: &Bitmap,
    num_rows: usize,
    mut predicate: F,
) -> Bitmap
where
    F: FnMut(&T) -> bool,
{
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();
    let word_count = out_words.len();

    for word_idx in 0..word_count {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);
        let mut out_word = 0u64;

        for bit in 0..count {
            let idx = base + bit;
            let cond = predicate(&values[idx]) as u64;
            let valid = 1u64 ^ ((null_word >> bit) & 1);
            out_word |= (cond & valid) << bit;
        }

        out_words[word_idx] = out_word;
    }

    bitmap
}

fn build_non_null_bitmap(num_rows: usize, null_bitmap: &Bitmap) -> Bitmap {
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();
    let word_count = out_words.len();
    let last_bits = num_rows % 64;
    let last_mask = if last_bits == 0 {
        u64::MAX
    } else {
        (1u64 << last_bits) - 1
    };

    for word_idx in 0..word_count {
        let null_word = *null_words.get(word_idx).unwrap_or(&0);
        let mut out_word = !null_word;
        if word_idx + 1 == word_count {
            out_word &= last_mask;
        }
        out_words[word_idx] = out_word;
    }

    bitmap
}

fn build_indexed_word_bitmap<F>(
    num_rows: usize,
    null_bitmap: &Bitmap,
    mut predicate: F,
) -> Bitmap
where
    F: FnMut(usize) -> bool,
{
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();
    let word_count = out_words.len();

    for word_idx in 0..word_count {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);
        let mut out_word = 0u64;

        for bit in 0..count {
            let idx = base + bit;
            let cond = predicate(idx) as u64;
            let valid = 1u64 ^ ((null_word >> bit) & 1);
            out_word |= (cond & valid) << bit;
        }

        out_words[word_idx] = out_word;
    }

    bitmap
}

fn evaluate_in_list(
    expr: &PhysicalExpr,
    list: &[PhysicalExpr],
    negated: bool,
    batch: &ColumnarBatch,
) -> Bitmap {
    let PhysicalExpr::Column { index, .. } = expr else {
        return Bitmap::new(batch.num_rows);
    };
    let Some(page) = batch.columns.get(index) else {
        return Bitmap::new(batch.num_rows);
    };
    evaluate_in_list_column(page, list, negated)
}

fn evaluate_in_list_column(page: &ColumnarPage, list: &[PhysicalExpr], negated: bool) -> Bitmap {
    let num_rows = page.len();
    let mut bitmap = Bitmap::new(num_rows);
    if list.is_empty() {
        return bitmap;
    }

    match &page.data {
        ColumnData::Int64(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let Some(i) = scalar_to_i64(val)
                {
                    set.insert(i);
                }
            }
            bitmap = build_word_bitmap(values, &page.null_bitmap, num_rows, |value| {
                let matches = set.contains(value);
                matches != negated
            });
        }
        ColumnData::Float64(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let Some(f) = scalar_to_f64(val)
                {
                    set.insert(f.to_bits());
                }
            }
            bitmap = build_word_bitmap(values, &page.null_bitmap, num_rows, |value| {
                let matches = set.contains(&value.to_bits());
                matches != negated
            });
        }
        ColumnData::Boolean(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let ScalarValue::Boolean(b) = val
                {
                    set.insert(*b);
                }
            }
            bitmap = build_word_bitmap(values, &page.null_bitmap, num_rows, |value| {
                let matches = set.contains(value);
                matches != negated
            });
        }
        ColumnData::Timestamp(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let ScalarValue::Timestamp(ts) = val
                {
                    set.insert(*ts);
                }
            }
            bitmap = build_word_bitmap(values, &page.null_bitmap, num_rows, |value| {
                let matches = set.contains(value);
                matches != negated
            });
        }
        ColumnData::Text(col) => {
            // Build HashSet of literal values as bytes for fast comparison
            let mut set: HashSet<Vec<u8>> = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let Some(text) = scalar_to_string(val)
                {
                    set.insert(text.into_bytes());
                }
            }
            bitmap = build_indexed_word_bitmap(num_rows, &page.null_bitmap, |idx| {
                let row_bytes = col.get_bytes(idx);
                let matches = set.contains(row_bytes);
                matches != negated
            });
        }
        ColumnData::Dictionary(dict) => {
            // Dictionary optimization: check dictionary values once, then scan keys
            let mut set: HashSet<Vec<u8>> = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item
                    && let Some(text) = scalar_to_string(val)
                {
                    set.insert(text.into_bytes());
                }
            }
            // Pre-compute which dictionary keys match
            let mut matching_keys = HashSet::new();
            for key in 0..dict.values.len() {
                if set.contains(dict.values.get_bytes(key)) {
                    matching_keys.insert(key as u16);
                }
            }
            bitmap = build_word_bitmap(&dict.keys, &page.null_bitmap, num_rows, |key| {
                let matches = matching_keys.contains(key);
                matches != negated
            });
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

fn evaluate_like(
    expr: &PhysicalExpr,
    pattern: &PhysicalExpr,
    case_insensitive: bool,
    negated: bool,
    batch: &ColumnarBatch,
) -> Bitmap {
    if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(ScalarValue::String(pat))) =
        (expr, pattern)
        && let Some(page) = batch.columns.get(index)
    {
        if let Some((prefix, exact)) = extract_like_prefix(pat) {
            return build_indexed_word_bitmap(batch.num_rows, &page.null_bitmap, |i| {
                let bytes = match &page.data {
                    ColumnData::Text(col) => col.get_bytes(i),
                    ColumnData::Dictionary(dict) => dict.get_bytes(i),
                    _ => return false,
                };
                if exact && bytes.len() != prefix.len() {
                    return false;
                }
                if bytes.len() < prefix.len() {
                    return false;
                }
                let slice = &bytes[..prefix.len()];
                let matches = if case_insensitive {
                    slice.eq_ignore_ascii_case(&prefix)
                } else {
                    slice == prefix.as_slice()
                };
                if negated { !matches } else { matches }
            });
        }

        match &page.data {
            ColumnData::Text(col) => {
                return build_indexed_word_bitmap(batch.num_rows, &page.null_bitmap, |i| {
                    // LIKE requires string semantics, so we use get_string here
                    let value = col.get_string(i);
                    let matches = like_match(&value, pat, !case_insensitive);
                    if negated { !matches } else { matches }
                });
            }
            ColumnData::Dictionary(dict) => {
                return build_indexed_word_bitmap(batch.num_rows, &page.null_bitmap, |i| {
                    let value = dict.get_string(i);
                    let matches = like_match(&value, pat, !case_insensitive);
                    if negated { !matches } else { matches }
                });
            }
            _ => {}
        }
    }
    Bitmap::new(batch.num_rows)
}

fn evaluate_rlike(
    expr: &PhysicalExpr,
    pattern: &PhysicalExpr,
    negated: bool,
    batch: &ColumnarBatch,
) -> Bitmap {
    if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(ScalarValue::String(pat))) =
        (expr, pattern)
        && let Some(page) = batch.columns.get(index)
        && let ColumnData::Text(col) = &page.data
    {
        let mut bitmap = Bitmap::new(batch.num_rows);
        for i in 0..col.len() {
            let value = col.get_string(i);
            let matches = regex_match(&value, pat);
            if if negated { !matches } else { matches } {
                bitmap.set(i);
            }
        }
        return bitmap;
    }
    Bitmap::new(batch.num_rows)
}

fn extract_like_prefix(pattern: &str) -> Option<(Vec<u8>, bool)> {
    let bytes = pattern.as_bytes();
    let mut first_wildcard = None;
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'_' {
            return None;
        }
        if b == b'%' {
            first_wildcard = Some(idx);
            break;
        }
    }

    match first_wildcard {
        None => Some((bytes.to_vec(), true)),
        Some(pos) => {
            if bytes[pos..].iter().all(|&b| b == b'%') {
                Some((bytes[..pos].to_vec(), false))
            } else {
                None
            }
        }
    }
}

fn evaluate_is_null(expr: &PhysicalExpr, batch: &ColumnarBatch) -> Bitmap {
    if let PhysicalExpr::Column { index, .. } = expr
        && let Some(page) = batch.columns.get(index)
    {
        return page.null_bitmap.clone();
    }
    // For literals, they are never null in our system (ScalarValue::Null is handled but usually we don't IS NULL a literal in WHERE)
    // If we support complex expressions in IS NULL, we'd need full evaluation.
    Bitmap::new(batch.num_rows)
}
