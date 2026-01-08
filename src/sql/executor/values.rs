use crate::entry::Entry;
use crate::sql::types::DataType;
use crate::sql::utils::parse_datetime;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;

const NULL_SENTINEL: &str = "\u{0001}";

pub fn encode_null() -> String {
    NULL_SENTINEL.to_string()
}

pub fn is_encoded_null(value: &str) -> bool {
    value == NULL_SENTINEL
}

#[derive(Debug, Clone)]
pub enum CachedValue {
    Null,
    Text(String),
}

impl CachedValue {
    pub fn from_entry(entry: &Entry) -> Self {
        let data = entry.get_data();
        if is_encoded_null(data) {
            CachedValue::Null
        } else {
            CachedValue::Text(data.to_string())
        }
    }

    pub fn into_option_string(self) -> Option<String> {
        match self {
            CachedValue::Null => None,
            CachedValue::Text(text) => Some(text),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Timestamp(i64), // Unix microseconds
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => true,
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a == b,
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (ScalarValue::String(a), ScalarValue::String(b)) => a == b,
            (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl ScalarValue {
    pub fn get_datatype(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::String(_) => DataType::String,
            ScalarValue::Timestamp(_) => DataType::Timestamp,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Float64(f) => Some(*f),
            ScalarValue::Int64(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ScalarValue::Int64(i) => Some(*i),
            _ => None,
        }
    }

    pub fn into_option_string(self) -> Option<String> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Boolean(b) => Some(b.to_string()),
            ScalarValue::Int64(i) => Some(i.to_string()),
            ScalarValue::Float64(f) => Some(format_float(f)),
            ScalarValue::String(s) => Some(s),
            ScalarValue::Timestamp(t) => format_timestamp_micros(t),
        }
    }
}

impl Eq for ScalarValue {}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => Some(Ordering::Equal),
            (ScalarValue::Null, _) => Some(Ordering::Less),
            (_, ScalarValue::Null) => Some(Ordering::Greater),
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a.partial_cmp(b),
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a.partial_cmp(b),
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => a.partial_cmp(b),
            (ScalarValue::String(a), ScalarValue::String(b)) => a.partial_cmp(b),
            (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub fn cached_to_scalar(value: &CachedValue) -> ScalarValue {
    match value {
        CachedValue::Null => ScalarValue::Null,
        CachedValue::Text(s) => {
            if let Ok(value) = s.parse::<i64>() {
                ScalarValue::Int64(value)
            } else if let Ok(value) = s.parse::<f64>() {
                ScalarValue::Float64(value)
            } else if s.eq_ignore_ascii_case("true") {
                ScalarValue::Boolean(true)
            } else if s.eq_ignore_ascii_case("false") {
                ScalarValue::Boolean(false)
            } else {
                ScalarValue::String(s.clone())
            }
        }
    }
}

pub fn cached_to_scalar_with_type(value: &CachedValue, data_type: DataType) -> ScalarValue {
    match (value, data_type) {
        (CachedValue::Null, _) => ScalarValue::Null,
        (CachedValue::Text(s), DataType::Timestamp) => parse_datetime(s)
            .map(ScalarValue::Timestamp)
            .unwrap_or_else(|| ScalarValue::String(s.clone())),
        _ => cached_to_scalar(value),
    }
}

pub(super) fn format_timestamp_micros(micros: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_micros(micros)
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
}

pub(super) fn format_float(value: f64) -> String {
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

pub(super) fn scalar_from_f64(value: f64) -> ScalarValue {
    if value.is_nan() || value.is_infinite() {
        ScalarValue::Float64(value)
    } else if (value.fract().abs() - 0.0).abs() < 1e-9 {
        ScalarValue::Int64(value as i64)
    } else {
        ScalarValue::Float64(value)
    }
}

pub(super) fn combine_numeric<F>(
    left: &ScalarValue,
    right: &ScalarValue,
    op: F,
) -> Option<ScalarValue>
where
    F: Fn(f64, f64) -> f64,
{
    let lhs = left.as_f64()?;
    let rhs = right.as_f64()?;
    Some(scalar_from_f64(op(lhs, rhs)))
}

pub(super) fn compare_scalar_values(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarValue::Int64(l), ScalarValue::Float64(r)) => (*l as f64).partial_cmp(r),
        (ScalarValue::Float64(l), ScalarValue::Int64(r)) => l.partial_cmp(&(*r as f64)),
        (ScalarValue::String(l), ScalarValue::String(r)) => Some(compare_strs(l, r)),
        _ => left.partial_cmp(right),
    }
}

pub(super) fn compare_strs(left: &str, right: &str) -> Ordering {
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
