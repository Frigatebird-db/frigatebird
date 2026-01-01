use crate::sql::executor::batch::{Bitmap, ColumnData, ColumnarBatch, ColumnarPage};
use crate::sql::executor::helpers::like_match;
use crate::sql::executor::values::ScalarValue;
use crate::sql::physical_plan::PhysicalExpr;
use sqlparser::ast::BinaryOperator;
use std::collections::HashSet;

pub struct PhysicalEvaluator;

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
            } => evaluate_like(expr, pattern, *case_insensitive, batch),
            PhysicalExpr::InList {
                expr,
                list,
                negated,
            } => evaluate_in_list(expr, list, *negated, batch),
            PhysicalExpr::Column { index, .. } => {
                // Boolean column used directly as predicate
                if let Some(page) = batch.columns.get(index) {
                    if let ColumnData::Boolean(values) = &page.data {
                        let mut bitmap = Bitmap::new(batch.num_rows);
                        for (i, &v) in values.iter().enumerate() {
                            if v {
                                bitmap.set(i);
                            }
                        }
                        return bitmap;
                    }
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
        // Most common case: Column <Op> Literal
        if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(val)) = (left, right) {
            if let Some(col_page) = batch.columns.get(index) {
                return evaluate_col_lit(&col_page.data, op, val, batch.num_rows);
            }
        }

        // Handle Literal <Op> Column (Flip it)
        if let (PhysicalExpr::Literal(val), PhysicalExpr::Column { index, .. }) = (left, right) {
            if let Some(col_page) = batch.columns.get(index) {
                if let Some(rev_op) = reverse_operator(op) {
                    return evaluate_col_lit(&col_page.data, &rev_op, val, batch.num_rows);
                }
            }
        }

        Bitmap::new(batch.num_rows) // Fallback empty
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
                    for (i, &v) in vec.iter().enumerate() {
                        if v == target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v != target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Gt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v > target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v >= target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Lt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v < target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v <= target {
                            bitmap.set(i);
                        }
                    }
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
                    for (i, &v) in vec.iter().enumerate() {
                        if (v - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Gt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v > target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v >= target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Lt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v < target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v <= target {
                            bitmap.set(i);
                        }
                    }
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
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64 - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64 - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Gt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64) > target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64) >= target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Lt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64) < target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v as f64) <= target {
                            bitmap.set(i);
                        }
                    }
                }
                _ => {}
            }
        }
        (ColumnData::Float64(vec), ScalarValue::Int64(val)) => {
            let target = *val as f64;
            match op {
                BinaryOperator::Eq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v - target).abs() < f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if (v - target).abs() >= f64::EPSILON {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Gt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v > target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v >= target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::Lt => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v < target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v <= target {
                            bitmap.set(i);
                        }
                    }
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
                    for (i, &v) in vec.iter().enumerate() {
                        if v == target {
                            bitmap.set(i);
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    for (i, &v) in vec.iter().enumerate() {
                        if v != target {
                            bitmap.set(i);
                        }
                    }
                }
                _ => {}
            }
        }

        // ---------------------------------------------------------
        // LEGACY PATH: STRING (Still faster because no type guessing)
        // ---------------------------------------------------------
        (ColumnData::Text(vec), ScalarValue::String(val)) => match op {
            BinaryOperator::Eq => {
                for (i, v) in vec.iter().enumerate() {
                    if v == val {
                        bitmap.set(i);
                    }
                }
            }
            BinaryOperator::NotEq => {
                for (i, v) in vec.iter().enumerate() {
                    if v != val {
                        bitmap.set(i);
                    }
                }
            }
            BinaryOperator::Gt => {
                for (i, v) in vec.iter().enumerate() {
                    if v > val {
                        bitmap.set(i);
                    }
                }
            }
            BinaryOperator::GtEq => {
                for (i, v) in vec.iter().enumerate() {
                    if v >= val {
                        bitmap.set(i);
                    }
                }
            }
            BinaryOperator::Lt => {
                for (i, v) in vec.iter().enumerate() {
                    if v < val {
                        bitmap.set(i);
                    }
                }
            }
            BinaryOperator::LtEq => {
                for (i, v) in vec.iter().enumerate() {
                    if v <= val {
                        bitmap.set(i);
                    }
                }
            }
            _ => {}
        },

        // ---------------------------------------------------------
        // TEXT COLUMN WITH NUMERIC LITERAL
        // ---------------------------------------------------------
        (ColumnData::Text(vec), ScalarValue::Int64(val)) => {
            let target = *val as f64;
            for (i, v) in vec.iter().enumerate() {
                let Ok(num) = v.parse::<f64>() else {
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
        (ColumnData::Text(vec), ScalarValue::Float64(val)) => {
            let target = *val;
            for (i, v) in vec.iter().enumerate() {
                let Ok(num) = v.parse::<f64>() else {
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
            let mut set = HashSet::new();
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
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let ScalarValue::Boolean(b) = val {
                        set.insert(*b);
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
        ColumnData::Timestamp(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let ScalarValue::Timestamp(ts) = val {
                        set.insert(*ts);
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
        ColumnData::Text(values) => {
            let mut set = HashSet::new();
            for item in list {
                if let PhysicalExpr::Literal(val) = item {
                    if let Some(text) = scalar_to_string(val) {
                        set.insert(text);
                    }
                }
            }
            for (idx, value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let matches = set.contains(value);
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

fn evaluate_like(
    expr: &PhysicalExpr,
    pattern: &PhysicalExpr,
    case_insensitive: bool,
    batch: &ColumnarBatch,
) -> Bitmap {
    if let (PhysicalExpr::Column { index, .. }, PhysicalExpr::Literal(ScalarValue::String(pat))) =
        (expr, pattern)
    {
        if let Some(page) = batch.columns.get(index) {
            if let ColumnData::Text(values) = &page.data {
                let mut bitmap = Bitmap::new(batch.num_rows);
                for (i, value) in values.iter().enumerate() {
                    if like_match(value, pat, !case_insensitive) {
                        bitmap.set(i);
                    }
                }
                return bitmap;
            }
        }
    }
    Bitmap::new(batch.num_rows)
}
