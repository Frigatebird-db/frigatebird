use chrono::{DateTime, NaiveDateTime, Utc};

pub fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Some(true),
        "false" | "f" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

pub fn parse_datetime(s: &str) -> Option<i64> {
    if s.eq_ignore_ascii_case("now") {
        return Some(Utc::now().timestamp_micros());
    }
    // Try formats: ISO8601, etc.
    // Return Unix microseconds
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Some(dt.timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d") {
        return Some(dt.and_utc().timestamp_micros());
    }
    // Try parsing as number (timestamp)
    if let Ok(ts) = s.parse::<i64>() {
        return Some(ts);
    }
    None
}
