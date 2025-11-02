use crate::entry::Entry;
use std::cmp::Ordering;

const NULL_SENTINEL: &str = "\u{0001}";

pub(super) fn encode_null() -> String {
    NULL_SENTINEL.to_string()
}

pub(super) fn is_encoded_null(value: &str) -> bool {
    value == NULL_SENTINEL
}

#[derive(Debug, Clone)]
pub(super) enum CachedValue {
    Null,
    Text(String),
}

impl CachedValue {
    pub(super) fn from_entry(entry: &Entry) -> Self {
        let data = entry.get_data();
        if is_encoded_null(data) {
            CachedValue::Null
        } else {
            CachedValue::Text(data.to_string())
        }
    }

    pub(super) fn into_option_string(self) -> Option<String> {
        match self {
            CachedValue::Null => None,
            CachedValue::Text(text) => Some(text),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum ScalarValue {
    Null,
    Int(i128),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl ScalarValue {
    pub(super) fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    pub(super) fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(*value as f64),
            ScalarValue::Float(value) => Some(*value),
            ScalarValue::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
            ScalarValue::Text(text) => text.parse::<f64>().ok(),
        }
    }

    pub(super) fn as_i128(&self) -> Option<i128> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(*value),
            ScalarValue::Float(value) => Some(*value as i128),
            ScalarValue::Bool(value) => Some(if *value { 1 } else { 0 }),
            ScalarValue::Text(text) => text.parse::<i128>().ok(),
        }
    }

    pub(super) fn as_bool(&self) -> Option<bool> {
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

    pub(super) fn into_option_string(self) -> Option<String> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Int(value) => Some(value.to_string()),
            ScalarValue::Float(value) => Some(format_float(value)),
            ScalarValue::Text(text) => Some(text),
            ScalarValue::Bool(value) => Some(if value { "true".into() } else { "false".into() }),
        }
    }
}

pub(super) fn cached_to_scalar(value: &CachedValue) -> ScalarValue {
    match value {
        CachedValue::Null => ScalarValue::Null,
        CachedValue::Text(text) => ScalarValue::Text(text.clone()),
    }
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
        ScalarValue::Float(value)
    } else if (value.fract().abs() - 0.0).abs() < 1e-9 {
        ScalarValue::Int(value as i128)
    } else {
        ScalarValue::Float(value)
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
