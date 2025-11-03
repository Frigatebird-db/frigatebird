use std::cmp::Ordering;

pub fn compare_values<F>(left: &str, right: &str, predicate: F) -> bool
where
    F: Fn(Ordering) -> bool,
{
    // Try number comparison first
    if let (Ok(left_num), Ok(right_num)) = (left.parse::<f64>(), right.parse::<f64>()) {
        return predicate(
            left_num
                .partial_cmp(&right_num)
                .unwrap_or(Ordering::Equal),
        );
    }

    // Try datetime comparison
    if let (Some(left_dt), Some(right_dt)) = (parse_datetime(left), parse_datetime(right)) {
        return predicate(left_dt.cmp(&right_dt));
    }

    // Try IP comparison
    if let (Some(left_ip), Some(right_ip)) = (parse_ip(left), parse_ip(right)) {
        return predicate(left_ip.cmp(&right_ip));
    }

    // Try version comparison
    if let (Some(left_ver), Some(right_ver)) = (parse_version(left), parse_version(right)) {
        return predicate(left_ver.cmp(&right_ver));
    }

    // Try duration comparison
    if let (Some(left_dur), Some(right_dur)) = (parse_duration(left), parse_duration(right)) {
        return predicate(left_dur.cmp(&right_dur));
    }

    // Try UUID comparison
    if let (Some(left_uuid), Some(right_uuid)) = (parse_uuid(left), parse_uuid(right)) {
        return predicate(left_uuid.cmp(&right_uuid));
    }

    // Fallback to lexicographic string comparison
    predicate(left.cmp(right))
}

pub fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" | "" => Ok(false),
        _ => Err(()),
    }
}

pub fn is_null(s: &str) -> bool {
    s.is_empty() || s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("nil")
}

pub fn parse_datetime(s: &str) -> Option<i64> {
    // Try Unix timestamp first (most efficient)
    if let Ok(timestamp) = s.parse::<i64>() {
        if timestamp >= 0 && timestamp < 2_000_000_000 {
            return Some(timestamp);
        }
    }

    // Try ISO 8601 format
    if let Some(ts) = parse_iso_date(s) {
        return Some(ts);
    }

    // Try common date formats
    parse_common_date(s)
}

pub fn parse_iso_date(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split('T').collect();
    let date_part = parts.first()?;
    let date_components: Vec<&str> = date_part.split('-').collect();
    if date_components.len() != 3 {
        return None;
    }

    let year = date_components[0].parse::<i32>().ok()?;
    let month = date_components[1].parse::<u32>().ok()?;
    let day = date_components[2].parse::<u32>().ok()?;

    let mut seconds = days_since_epoch(year, month, day)? * 86400;

    if parts.len() >= 2 {
        let time_part = parts[1].split('Z').next().unwrap_or(parts[1]);
        let time_components: Vec<&str> = time_part.split(':').collect();
        if let Some(hour_str) = time_components.first() {
            let hour = hour_str.parse::<i64>().ok()?;
            seconds += hour * 3600;
        }
        if let Some(minute_str) = time_components.get(1) {
            let minute = minute_str.parse::<i64>().ok()?;
            seconds += minute * 60;
        }
        if let Some(second_str) = time_components.get(2) {
            let second = second_str.parse::<f64>().ok()?;
            seconds += second as i64;
        }
    }

    Some(seconds)
}

pub fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i64> {
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }

    let mut days = 0i64;
    for y in 1970..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }

    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += if m == 2 && is_leap_year(year) {
            29
        } else {
            days_in_month[(m - 1) as usize] as i64
        };
    }

    days += (day - 1) as i64;
    Some(days)
}

pub fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn parse_common_date(s: &str) -> Option<i64> {
    if let Some((m, d, y)) = try_parse_mdy(s, '/') {
        return days_since_epoch(y, m, d).map(|days| days * 86400);
    }
    if let Some((m, d, y)) = try_parse_mdy(s, '-') {
        return days_since_epoch(y, m, d).map(|days| days * 86400);
    }
    if let Some((d, m, y)) = try_parse_dmy(s, '/') {
        return days_since_epoch(y, m, d).map(|days| days * 86400);
    }
    if let Some((d, m, y)) = try_parse_dmy(s, '.') {
        return days_since_epoch(y, m, d).map(|days| days * 86400);
    }
    None
}

fn try_parse_mdy(s: &str, sep: char) -> Option<(u32, u32, i32)> {
    let parts: Vec<&str> = s.split(sep).collect();
    if parts.len() == 3 {
        let m = parts[0].parse::<u32>().ok()?;
        let d = parts[1].parse::<u32>().ok()?;
        let y = parts[2].parse::<i32>().ok()?;
        Some((m, d, y))
    } else {
        None
    }
}

fn try_parse_dmy(s: &str, sep: char) -> Option<(u32, u32, i32)> {
    let parts: Vec<&str> = s.split(sep).collect();
    if parts.len() == 3 {
        let d = parts[0].parse::<u32>().ok()?;
        let m = parts[1].parse::<u32>().ok()?;
        let mut y = parts[2].parse::<i32>().ok()?;
        if y < 100 {
            y += if y < 70 { 2000 } else { 1900 };
        }
        Some((d, m, y))
    } else {
        None
    }
}

pub fn parse_ip(s: &str) -> Option<u32> {
    let octets: Vec<&str> = s.split('.').collect();
    if octets.len() != 4 {
        return None;
    }

    let mut result = 0u32;
    for (i, octet_str) in octets.iter().enumerate() {
        let octet = octet_str.parse::<u32>().ok()?;
        if octet > 255 {
            return None;
        }
        result |= octet << (8 * (3 - i));
    }

    Some(result)
}

pub fn parse_uuid(s: &str) -> Option<String> {
    let cleaned: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if cleaned.len() == 32 {
        Some(cleaned.to_lowercase())
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionTuple {
    pub parts: Vec<u32>,
}

impl PartialOrd for VersionTuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionTuple {
    fn cmp(&self, other: &Self) -> Ordering {
        let max_len = self.parts.len().max(other.parts.len());
        for i in 0..max_len {
            let left = self.parts.get(i).copied().unwrap_or(0);
            let right = other.parts.get(i).copied().unwrap_or(0);
            match left.cmp(&right) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }
}

pub fn parse_version(s: &str) -> Option<VersionTuple> {
    let trimmed = s.trim_start_matches(|c: char| !c.is_ascii_digit());
    let version_part = trimmed
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect::<String>();

    if version_part.is_empty() {
        return None;
    }

    let parts: Vec<u32> = version_part
        .split('.')
        .filter_map(|part| part.parse::<u32>().ok())
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(VersionTuple { parts })
    }
}

pub fn parse_duration(s: &str) -> Option<i64> {
    if let Ok(seconds) = s.parse::<i64>() {
        return Some(seconds);
    }

    if let Some((value, unit)) = try_parse_simple_duration(s) {
        return Some(value * unit);
    }

    try_parse_compact_duration(s)
}

fn try_parse_simple_duration(s: &str) -> Option<(i64, i64)> {
    let s = s.trim().to_lowercase();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let value = parts[0].parse::<i64>().ok()?;
    let multiplier = match parts[1] {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86400,
        _ => return None,
    };

    Some((value, multiplier))
}

fn try_parse_compact_duration(s: &str) -> Option<i64> {
    let mut total = 0i64;
    let mut current_num = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else if !current_num.is_empty() {
            let value = current_num.parse::<i64>().ok()?;
            current_num.clear();

            let multiplier = match ch.to_ascii_lowercase() {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                'd' => 86400,
                _ => return None,
            };

            total += value * multiplier;
        }
    }

    if total > 0 {
        Some(total)
    } else {
        None
    }
}
