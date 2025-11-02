use crate::page_handler::PageHandler;
use crate::sql::models::{FilterExpr, QueryPlan};
use crossbeam::channel::{self, Receiver, Sender};
use rand::{Rng, distributions::Alphanumeric};
use sqlparser::ast::Expr;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

/// Lightweight batch placeholder flowing between pipeline stages.
pub type PipelineBatch = Vec<usize>;

/// Represents a single step in the execution pipeline that filters on a specific column.
#[derive(Clone)]
pub struct PipelineStep {
    /// Produces batches for the downstream step.
    pub current_producer: Sender<PipelineBatch>,
    /// Receives batches produced by the previous step.
    pub previous_receiver: Receiver<PipelineBatch>,
    /// The column this step filters on
    pub column: String,
    /// The filter expressions to apply to this column
    pub filters: Vec<FilterExpr>,
    /// Indicates whether this is the first step in the job chain
    pub is_root: bool,
    /// The table this step operates on
    pub table: String,
    /// Page handler for accessing column data
    pub page_handler: Arc<PageHandler>,
}

impl std::fmt::Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("column", &self.column)
            .field("filters", &self.filters)
            .field("is_root", &self.is_root)
            .field("table", &self.table)
            .finish()
    }
}

impl PipelineStep {
    pub fn new(
        column: String,
        filters: Vec<FilterExpr>,
        current_producer: Sender<PipelineBatch>,
        previous_receiver: Receiver<PipelineBatch>,
        is_root: bool,
        table: String,
        page_handler: Arc<PageHandler>,
    ) -> Self {
        PipelineStep {
            current_producer,
            previous_receiver,
            column,
            filters,
            is_root,
            table,
            page_handler,
        }
    }

    /// Placeholder execution loop for step-level runtime.
    pub fn execute(&self) {
        let batch = if self.is_root {
            Some(Vec::new())
        } else {
            self.previous_receiver.recv().ok()
        };

        if let Some(mut rows) = batch {
            self.apply_filters(&mut rows);
            let _ = self.current_producer.send(rows);
        }
    }

    fn apply_filters(&self, rows: &mut PipelineBatch) {
        if self.filters.is_empty() || rows.is_empty() {
            return;
        }

        // FAST PATH: Batch fetch all needed pages once
        let first_row = *rows.first().unwrap() as u64;
        let last_row = *rows.last().unwrap() as u64;

        let slices = self.page_handler.list_range_in_table(
            &self.table,
            &self.column,
            first_row,
            last_row,
        );

        if slices.is_empty() {
            rows.clear();
            return;
        }

        let pages = self.page_handler.get_pages(
            slices.iter().map(|s| s.descriptor.clone()).collect()
        );

        // Build row->value lookup (amortized O(1) per row)
        let mut row_values: HashMap<usize, &str> = HashMap::with_capacity(rows.len());
        let mut cumulative_offset = 0u64;

        for page in pages.iter() {
            for (idx, entry) in page.page.entries.iter().enumerate() {
                let global_row = cumulative_offset + idx as u64;
                if let Ok(row_id) = usize::try_from(global_row) {
                    row_values.insert(row_id, entry.get_data());
                }
            }
            cumulative_offset += page.page.entries.len() as u64;
        }

        // CRITICAL: In-place filtering to avoid allocations
        rows.retain(|&row_id| {
            if let Some(&value) = row_values.get(&row_id) {
                // Short-circuit AND evaluation - fail fast
                self.filters.iter().all(|filter| eval_filter(filter, value))
            } else {
                false
            }
        });
    }
}

/// Represents the execution pipeline for a query.
#[derive(Debug)]
pub struct Job {
    /// The table name this pipeline operates on
    pub table_name: String,
    /// Sequential steps to execute filters, each on a specific column
    pub steps: Vec<PipelineStep>,
    /// Total number of steps, used for quick scheduling heuristics
    pub cost: usize,
    /// Tracks the next slot available for downstream execution bookkeeping
    pub next_free_slot: AtomicUsize,
    /// Identifier for tracing/logging purposes
    pub id: String,
    /// Entry point producer feeding the first pipeline step
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
            id: generate_pipeline_id(),
            entry_producer,
        }
    }

    /// Attempts to run the next pipeline step via CAS on `next_free_slot`.
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
        self.cost == other.cost
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cost.cmp(&other.cost))
    }
}

/// Minimal expression evaluator - inline for performance
#[inline]
fn eval_filter(filter: &FilterExpr, value: &str) -> bool {
    match filter {
        FilterExpr::Leaf(expr) => eval_expr(expr, value),
        FilterExpr::And(filters) => filters.iter().all(|f| eval_filter(f, value)),
        FilterExpr::Or(filters) => filters.iter().any(|f| eval_filter(f, value)),
    }
}

#[inline]
fn eval_expr(expr: &Expr, value: &str) -> bool {
    use sqlparser::ast::{BinaryOperator, Value};

    match expr {
        Expr::BinaryOp { left: _, op, right } => {
            // Extract literal from right side
            let literal = match right.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
                Expr::Value(Value::Number(n, _)) => n.as_str(),
                _ => return true, // Skip unsupported comparisons
            };

            match op {
                BinaryOperator::Eq => compare_values(value, literal, |cmp| cmp == Ordering::Equal),
                BinaryOperator::NotEq => compare_values(value, literal, |cmp| cmp != Ordering::Equal),
                BinaryOperator::Gt => compare_values(value, literal, |cmp| cmp == Ordering::Greater),
                BinaryOperator::GtEq => compare_values(value, literal, |cmp| cmp != Ordering::Less),
                BinaryOperator::Lt => compare_values(value, literal, |cmp| cmp == Ordering::Less),
                BinaryOperator::LtEq => compare_values(value, literal, |cmp| cmp != Ordering::Greater),
                _ => true, // Conservative: include row if unsure
            }
        }
        Expr::IsNull(_) => is_null(value),
        Expr::IsNotNull(_) => !is_null(value),
        Expr::Like { negated, expr: _, pattern, escape_char: _ } => {
            let pattern_str = match pattern.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
                _ => return true,
            };
            let matches = like_match(value, pattern_str, true);
            if *negated { !matches } else { matches }
        }
        Expr::ILike { negated, expr: _, pattern, escape_char: _ } => {
            let pattern_str = match pattern.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
                _ => return true,
            };
            let matches = like_match(value, pattern_str, false);
            if *negated { !matches } else { matches }
        }
        Expr::RLike { negated, expr: _, pattern, .. } => {
            let pattern_str = match pattern.as_ref() {
                Expr::Value(Value::SingleQuotedString(s)) => s.as_str(),
                _ => return true,
            };
            let matches = regex_match(value, pattern_str);
            if *negated { !matches } else { matches }
        }
        _ => true, // Include row for unsupported expressions
    }
}

/// Compare two values with type-aware comparison.
/// Tries numeric (i64, then f64), then boolean, date/time, IP, UUID, version, then falls back to lexicographic.
#[inline]
fn compare_values<F>(left: &str, right: &str, predicate: F) -> bool
where
    F: Fn(Ordering) -> bool,
{
    // Try integer comparison first
    if let (Ok(l), Ok(r)) = (left.parse::<i64>(), right.parse::<i64>()) {
        return predicate(l.cmp(&r));
    }

    // Try float comparison
    if let (Ok(l), Ok(r)) = (left.parse::<f64>(), right.parse::<f64>()) {
        if let Some(ord) = l.partial_cmp(&r) {
            return predicate(ord);
        }
    }

    // Try boolean comparison
    if let (Ok(l), Ok(r)) = (parse_bool(left), parse_bool(right)) {
        return predicate(l.cmp(&r));
    }

    // Try date/time comparison
    if let (Some(l), Some(r)) = (parse_datetime(left), parse_datetime(right)) {
        return predicate(l.cmp(&r));
    }

    // Try IP address comparison
    if let (Some(l), Some(r)) = (parse_ip(left), parse_ip(right)) {
        return predicate(l.cmp(&r));
    }

    // Try UUID comparison
    if let (Some(l), Some(r)) = (parse_uuid(left), parse_uuid(right)) {
        return predicate(l.cmp(&r));
    }

    // Try version comparison
    if let (Some(l), Some(r)) = (parse_version(left), parse_version(right)) {
        return predicate(l.cmp(&r));
    }

    // Try duration comparison
    if let (Some(l), Some(r)) = (parse_duration(left), parse_duration(right)) {
        return predicate(l.cmp(&r));
    }

    // Fallback to lexicographic string comparison
    predicate(left.cmp(right))
}

/// Parse boolean from various string representations.
#[inline]
fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.to_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => Err(()),
    }
}

/// Check if a value is NULL (empty string or "null" variants).
#[inline]
fn is_null(s: &str) -> bool {
    s.is_empty() || s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("nil")
}

/// Parse datetime from various formats, returning Unix timestamp for comparison.
/// Supports: ISO 8601, Unix timestamps, common date formats.
fn parse_datetime(s: &str) -> Option<i64> {
    let trimmed = s.trim();

    // Unix timestamp (seconds or milliseconds)
    if let Ok(ts) = trimmed.parse::<i64>() {
        // Assume milliseconds if > year 2100 in seconds (4102444800)
        if ts > 4_102_444_800 {
            return Some(ts / 1000);
        }
        return Some(ts);
    }

    // ISO 8601: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
    if let Some(ts) = parse_iso_date(trimmed) {
        return Some(ts);
    }

    // Common formats: MM/DD/YYYY, DD-MM-YYYY, etc.
    if let Some(ts) = parse_common_date(trimmed) {
        return Some(ts);
    }

    None
}

/// Parse ISO 8601 date/datetime string.
fn parse_iso_date(s: &str) -> Option<i64> {
    // Simple parser for YYYY-MM-DD and YYYY-MM-DD HH:MM:SS
    let parts: Vec<&str> = s.split(&['T', ' '][..]).collect();
    let date_part = parts.get(0)?;

    let date_components: Vec<&str> = date_part.split('-').collect();
    if date_components.len() != 3 {
        return None;
    }

    let year: i32 = date_components[0].parse().ok()?;
    let month: u32 = date_components[1].parse().ok()?;
    let day: u32 = date_components[2].parse().ok()?;

    let mut hour = 0;
    let mut minute = 0;
    let mut second = 0;

    if let Some(time_part) = parts.get(1) {
        let time_components: Vec<&str> = time_part.split(':').collect();
        if time_components.len() >= 2 {
            hour = time_components[0].parse().ok()?;
            minute = time_components[1].parse().ok()?;
            if time_components.len() >= 3 {
                // Handle seconds (might have .ms or Z suffix)
                let sec_str = time_components[2].split(&['.', 'Z', '+', '-'][..]).next()?;
                second = sec_str.parse().ok()?;
            }
        }
    }

    // Simplified timestamp calculation (not accounting for all edge cases)
    // Days since Unix epoch (1970-01-01)
    let days = days_since_epoch(year, month, day)?;
    let timestamp = days * 86400 + (hour as i64) * 3600 + (minute as i64) * 60 + (second as i64);

    Some(timestamp)
}

/// Calculate days since Unix epoch (1970-01-01).
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i64> {
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }

    let mut total_days: i64 = 0;

    // Add days for complete years
    for y in 1970..year {
        total_days += if is_leap_year(y) { 366 } else { 365 };
    }

    // Add days for complete months in the current year
    let days_in_month = [31, if is_leap_year(year) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 0..(month - 1) as usize {
        total_days += days_in_month[m] as i64;
    }

    // Add remaining days
    total_days += day as i64 - 1;

    Some(total_days)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Parse common date formats like MM/DD/YYYY, DD-MM-YYYY.
fn parse_common_date(s: &str) -> Option<i64> {
    // Try MM/DD/YYYY
    if let Some((m, d, y)) = try_parse_mdy(s, '/') {
        return parse_iso_date(&format!("{:04}-{:02}-{:02}", y, m, d));
    }

    // Try DD-MM-YYYY
    if let Some((d, m, y)) = try_parse_dmy(s, '-') {
        return parse_iso_date(&format!("{:04}-{:02}-{:02}", y, m, d));
    }

    None
}

fn try_parse_mdy(s: &str, sep: char) -> Option<(u32, u32, i32)> {
    let parts: Vec<&str> = s.split(sep).collect();
    if parts.len() != 3 {
        return None;
    }
    let m = parts[0].parse().ok()?;
    let d = parts[1].parse().ok()?;
    let y = parts[2].parse().ok()?;
    Some((m, d, y))
}

fn try_parse_dmy(s: &str, sep: char) -> Option<(u32, u32, i32)> {
    let parts: Vec<&str> = s.split(sep).collect();
    if parts.len() != 3 {
        return None;
    }
    let d = parts[0].parse().ok()?;
    let m = parts[1].parse().ok()?;
    let y = parts[2].parse().ok()?;
    Some((d, m, y))
}

/// Parse IPv4 address for comparison.
/// Returns u32 representation for proper numeric ordering.
fn parse_ip(s: &str) -> Option<u32> {
    let parts: Vec<&str> = s.trim().split('.').collect();
    if parts.len() != 4 {
        return None;
    }

    let mut result: u32 = 0;
    for (i, part) in parts.iter().enumerate() {
        let octet: u32 = part.parse().ok()?;
        if octet > 255 {
            return None;
        }
        result |= octet << (8 * (3 - i));
    }

    Some(result)
}

/// Parse UUID string for comparison.
/// Returns the UUID as a normalized string (lowercase, no hyphens).
fn parse_uuid(s: &str) -> Option<String> {
    let cleaned = s.trim().to_lowercase().replace('-', "");

    // UUID should be 32 hex characters
    if cleaned.len() != 32 {
        return None;
    }

    // Check all characters are hex
    if cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(cleaned)
    } else {
        None
    }
}

/// Parse semantic version string (e.g., "1.2.3", "2.0.0-beta").
/// Returns VersionTuple for comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionTuple {
    major: u32,
    minor: u32,
    patch: u32,
    prerelease: String,
}

impl PartialOrd for VersionTuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionTuple {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare major, minor, patch first
        match self.major.cmp(&other.major) {
            Ordering::Equal => {}
            other => return other,
        }
        match self.minor.cmp(&other.minor) {
            Ordering::Equal => {}
            other => return other,
        }
        match self.patch.cmp(&other.patch) {
            Ordering::Equal => {}
            other => return other,
        }

        // For prerelease: empty string (release) > any prerelease
        match (&self.prerelease.is_empty(), &other.prerelease.is_empty()) {
            (true, false) => Ordering::Greater,  // Release > prerelease
            (false, true) => Ordering::Less,     // Prerelease < release
            (true, true) => Ordering::Equal,     // Both releases
            (false, false) => self.prerelease.cmp(&other.prerelease), // Compare prereleases
        }
    }
}

fn parse_version(s: &str) -> Option<VersionTuple> {
    let trimmed = s.trim();

    // Split on '-' to separate version from prerelease
    let parts: Vec<&str> = trimmed.split('-').collect();
    let version_part = parts[0];
    let prerelease = parts.get(1).unwrap_or(&"").to_string();

    // Parse version numbers
    let nums: Vec<&str> = version_part.split('.').collect();
    if nums.is_empty() || nums.len() > 3 {
        return None;
    }

    let major: u32 = nums.get(0)?.parse().ok()?;
    let minor: u32 = nums.get(1).unwrap_or(&"0").parse().ok().unwrap_or(0);
    let patch: u32 = nums.get(2).unwrap_or(&"0").parse().ok().unwrap_or(0);

    Some(VersionTuple {
        major,
        minor,
        patch,
        prerelease,
    })
}

/// Parse duration/interval strings (e.g., "5 days", "2h30m", "90 minutes").
/// Returns total seconds for comparison.
fn parse_duration(s: &str) -> Option<i64> {
    let trimmed = s.trim().to_lowercase();

    // Simple format: "N unit" where unit is days, hours, minutes, seconds
    if let Some((num, unit)) = try_parse_simple_duration(&trimmed) {
        return Some(num * unit);
    }

    // Compact format: "2h30m15s"
    if let Some(secs) = try_parse_compact_duration(&trimmed) {
        return Some(secs);
    }

    None
}

fn try_parse_simple_duration(s: &str) -> Option<(i64, i64)> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let num: i64 = parts[0].parse().ok()?;
    let multiplier = match parts[1] {
        "day" | "days" | "d" => 86400,
        "hour" | "hours" | "h" => 3600,
        "minute" | "minutes" | "min" | "m" => 60,
        "second" | "seconds" | "sec" | "s" => 1,
        "week" | "weeks" | "w" => 604800,
        _ => return None,
    };

    Some((num, multiplier))
}

fn try_parse_compact_duration(s: &str) -> Option<i64> {
    let mut total_seconds = 0i64;
    let mut current_num = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else if !current_num.is_empty() {
            let num: i64 = current_num.parse().ok()?;
            let multiplier = match ch {
                'd' => 86400,
                'h' => 3600,
                'm' => 60,
                's' => 1,
                'w' => 604800,
                _ => return None,
            };
            total_seconds += num * multiplier;
            current_num.clear();
        }
    }

    if total_seconds > 0 {
        Some(total_seconds)
    } else {
        None
    }
}

/// Pattern matching for LIKE operator.
/// Supports % (any characters) and _ (single character) wildcards.
fn like_match(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    let val = if case_sensitive { value.to_string() } else { value.to_lowercase() };
    let pat = if case_sensitive { pattern.to_string() } else { pattern.to_lowercase() };

    like_match_recursive(&val, &pat)
}

fn like_match_recursive(value: &str, pattern: &str) -> bool {
    let mut v_chars = value.chars().peekable();
    let mut p_chars = pattern.chars().peekable();

    loop {
        match p_chars.peek() {
            Some(&'%') => {
                p_chars.next();
                if p_chars.peek().is_none() {
                    return true; // % at end matches everything
                }
                // Try matching rest of pattern at each position
                for i in 0..=value.len() {
                    if like_match_recursive(&value[i..], p_chars.clone().collect::<String>().as_str()) {
                        return true;
                    }
                }
                return false;
            }
            Some(&'_') => {
                p_chars.next();
                if v_chars.next().is_none() {
                    return false; // _ must match exactly one char
                }
            }
            Some(&p) => {
                p_chars.next();
                match v_chars.next() {
                    Some(v) if v == p => continue,
                    _ => return false,
                }
            }
            None => return v_chars.peek().is_none(),
        }
    }
}

/// Regex matching using simple patterns (no external dependency).
/// Supports basic patterns: ^, $, for anchors.
fn regex_match(value: &str, pattern: &str) -> bool {
    // For now, just implement anchor patterns
    // A full regex engine would require an external crate like `regex`

    // Check for anchors
    let starts_with_anchor = pattern.starts_with('^');
    let ends_with_anchor = pattern.ends_with('$');

    let clean_pattern = pattern
        .trim_start_matches('^')
        .trim_end_matches('$');

    if starts_with_anchor && ends_with_anchor {
        // Exact match needed
        value == clean_pattern
    } else if starts_with_anchor {
        // Must start with pattern
        value.starts_with(clean_pattern)
    } else if ends_with_anchor {
        // Must end with pattern
        value.ends_with(clean_pattern)
    } else {
        // Pattern can appear anywhere
        value.contains(clean_pattern)
    }
}

/// Build a pipeline from a query plan.
/// For now, this randomly orders the filter steps with no optimization.
pub fn build_pipeline(plan: &QueryPlan, page_handler: Arc<PageHandler>) -> Vec<Job> {
    let mut jobs = Vec::with_capacity(plan.tables.len());

    for table in &plan.tables {
        if let Some(filter) = &table.filters {
            // Extract all leaf filters from the filter tree
            let leaf_filters = extract_leaf_filters(filter);

            // Group filters by the column they operate on
            let grouped = group_filters_by_column(leaf_filters);

            // Convert groups into pipeline steps (random order for now)
            let grouped_steps: Vec<(String, Vec<FilterExpr>)> = grouped.into_iter().collect();
            let (entry_producer, steps) = attach_channels(
                grouped_steps,
                table.table_name.clone(),
                Arc::clone(&page_handler),
            );

            jobs.push(Job::new(table.table_name.clone(), steps, entry_producer));
        } else {
            // No filters, empty pipeline
            let (entry_producer, _) = channel::unbounded::<PipelineBatch>();
            jobs.push(Job::new(
                table.table_name.clone(),
                Vec::new(),
                entry_producer,
            ));
        }
    }

    jobs
}

/// Extracts all leaf filter expressions from the filter tree.
fn extract_leaf_filters(filter: &FilterExpr) -> Vec<FilterExpr> {
    let mut leaves = Vec::new();
    collect_leaves(filter, &mut leaves);
    leaves
}

fn collect_leaves(filter: &FilterExpr, leaves: &mut Vec<FilterExpr>) {
    match filter {
        FilterExpr::Leaf(expr) => leaves.push(FilterExpr::Leaf(expr.clone())),
        FilterExpr::And(filters) | FilterExpr::Or(filters) => {
            for f in filters {
                collect_leaves(f, leaves);
            }
        }
    }
}

/// Groups filters by the primary column they operate on.
fn group_filters_by_column(filters: Vec<FilterExpr>) -> HashMap<String, Vec<FilterExpr>> {
    let mut groups: HashMap<String, Vec<FilterExpr>> = HashMap::new();

    for filter in filters {
        if let FilterExpr::Leaf(expr) = &filter {
            let column = extract_primary_column(expr).unwrap_or_else(|| "*".to_string());
            groups.entry(column).or_default().push(filter);
        }
    }

    groups
}

/// Extracts the primary column name from an expression.
/// Returns None if no clear column is found.
fn extract_primary_column(expr: &Expr) -> Option<String> {
    use Expr::*;

    match expr {
        Identifier(ident) => Some(ident.value.clone()),
        CompoundIdentifier(idents) => idents.last().map(|id| id.value.clone()),
        BinaryOp { left, .. } => extract_primary_column(left),
        UnaryOp { expr, .. } | Nested(expr) | Cast { expr, .. } | TryCast { expr, .. } => {
            extract_primary_column(expr)
        }
        IsFalse(expr) | IsNotFalse(expr) | IsTrue(expr) | IsNotTrue(expr) | IsNull(expr)
        | IsNotNull(expr) | IsUnknown(expr) | IsNotUnknown(expr) => extract_primary_column(expr),
        InList { expr, .. } | InSubquery { expr, .. } | InUnnest { expr, .. } => {
            extract_primary_column(expr)
        }
        Between { expr, .. } => extract_primary_column(expr),
        Like { expr, .. } | ILike { expr, .. } | SimilarTo { expr, .. } | RLike { expr, .. } => {
            extract_primary_column(expr)
        }
        _ => None,
    }
}

fn attach_channels(
    grouped_steps: Vec<(String, Vec<FilterExpr>)>,
    table: String,
    page_handler: Arc<PageHandler>,
) -> (Sender<PipelineBatch>, Vec<PipelineStep>) {
    let (entry_producer, mut previous_receiver) = channel::unbounded::<PipelineBatch>();
    let mut steps = Vec::with_capacity(grouped_steps.len());
    let mut is_root = true;

    for (column, filters) in grouped_steps {
        let (current_producer, next_receiver) = channel::unbounded::<PipelineBatch>();
        steps.push(PipelineStep::new(
            column,
            filters,
            current_producer,
            previous_receiver,
            is_root,
            table.clone(),
            Arc::clone(&page_handler),
        ));
        previous_receiver = next_receiver;
        is_root = false;
    }

    (entry_producer, steps)
}

fn generate_pipeline_id() -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();
    format!("pipe-{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
    use crate::helpers::compressor::Compressor;
    use crate::metadata_store::PageDirectory;
    use crate::page_handler::{PageFetcher, PageLocator, PageMaterializer};
    use crate::page_handler::page_io::PageIO;
    use crate::sql::models::TableAccess;
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};
    use std::sync::{Arc, RwLock};

    fn make_ident_expr(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn make_binary_op(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn create_mock_page_handler() -> Arc<PageHandler> {
        let meta_store = Arc::new(RwLock::new(crate::metadata_store::TableMetaStore::new()));
        let directory = Arc::new(PageDirectory::new(meta_store));
        let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));

        let compressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
        let page_io = Arc::new(PageIO {});
        let fetcher = Arc::new(PageFetcher::new(compressed_cache, page_io));

        let uncompressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
        let compressor = Arc::new(Compressor::new());
        let materializer = Arc::new(PageMaterializer::new(uncompressed_cache, compressor));

        Arc::new(PageHandler::new(locator, fetcher, materializer))
    }

    #[test]
    fn test_extract_primary_column_simple() {
        let expr = make_ident_expr("id");
        assert_eq!(extract_primary_column(&expr), Some("id".to_string()));
    }

    #[test]
    fn test_extract_primary_column_binary_op() {
        let expr = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        assert_eq!(extract_primary_column(&expr), Some("age".to_string()));
    }

    #[test]
    fn test_extract_leaf_filters() {
        let filter1 = FilterExpr::Leaf(make_ident_expr("x"));
        let filter2 = FilterExpr::Leaf(make_ident_expr("y"));
        let combined = FilterExpr::and(filter1, filter2);

        let leaves = extract_leaf_filters(&combined);
        assert_eq!(leaves.len(), 2);
    }

    #[test]
    fn test_build_pipeline_no_filters() {
        let table = TableAccess::new("users");
        let plan = QueryPlan::new(vec![table]);
        let page_handler = create_mock_page_handler();

        let pipelines = build_pipeline(&plan, page_handler);
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].steps.len(), 0);
    }

    #[test]
    fn test_build_pipeline_with_filters() {
        let mut table = TableAccess::new("users");
        let filter1 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));
        let filter2 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("John".to_string())),
        ));
        table.add_filter(FilterExpr::and(filter1, filter2));

        let plan = QueryPlan::new(vec![table]);
        let page_handler = create_mock_page_handler();
        let pipelines = build_pipeline(&plan, page_handler);

        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].steps.len(), 2); // Two columns: age and name
    }

    // Tests for eval_expr
    #[test]
    fn test_eval_expr_eq_matches() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_eval_expr_not_eq() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::NotEq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(!eval_expr(&expr, "Alice"));
        assert!(eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_eval_expr_gt_lexicographic() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(eval_expr(&expr, "Bob"));
        assert!(eval_expr(&expr, "Charlie"));
        assert!(!eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "Aaron"));
    }

    #[test]
    fn test_eval_expr_gte() {
        // Note: Uses lexicographic comparison, so "20" > "10" works
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::GtEq,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "z"));
        assert!(eval_expr(&expr, "m"));
        assert!(!eval_expr(&expr, "a"));
    }

    #[test]
    fn test_eval_expr_lt() {
        // Note: Uses lexicographic comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "a"));
        assert!(eval_expr(&expr, "f"));
        assert!(!eval_expr(&expr, "m"));
        assert!(!eval_expr(&expr, "z"));
    }

    #[test]
    fn test_eval_expr_lte() {
        // Note: Uses lexicographic comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::LtEq,
            Expr::Value(Value::SingleQuotedString("m".to_string())),
        );
        assert!(eval_expr(&expr, "m"));
        assert!(eval_expr(&expr, "a"));
        assert!(!eval_expr(&expr, "z"));
    }

    #[test]
    fn test_eval_expr_unsupported_returns_true() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Plus,
            Expr::Value(Value::Number("5".to_string(), false)),
        );
        // Conservative: include row if unsure
        assert!(eval_expr(&expr, "anything"));
    }

    #[test]
    fn test_eval_expr_unsupported_right_side() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Eq,
            Expr::Identifier(Ident::new("other_column")),
        );
        // Can't compare to another column yet, conservatively include
        assert!(eval_expr(&expr, "anything"));
    }

    // Tests for eval_filter
    #[test]
    fn test_eval_filter_leaf() {
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        let filter = FilterExpr::Leaf(expr);
        assert!(eval_filter(&filter, "Alice"));
        assert!(!eval_filter(&filter, "Bob"));
    }

    #[test]
    fn test_eval_filter_and_both_true() {
        let expr1 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        let expr2 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("65".to_string(), false)),
        );
        let filter = FilterExpr::And(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        assert!(eval_filter(&filter, "25"));
        assert!(eval_filter(&filter, "50"));
        assert!(!eval_filter(&filter, "10")); // Too young
        assert!(!eval_filter(&filter, "70")); // Too old
    }

    #[test]
    fn test_eval_filter_and_short_circuit() {
        let expr1 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("100".to_string(), false)),
        );
        let expr2 = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("200".to_string(), false)),
        );
        let filter = FilterExpr::And(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        // First condition false, should short-circuit
        assert!(!eval_filter(&filter, "50"));
    }

    #[test]
    fn test_eval_filter_or_any_true() {
        let expr1 = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        );
        let expr2 = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("pending".to_string())),
        );
        let filter = FilterExpr::Or(vec![
            FilterExpr::Leaf(expr1),
            FilterExpr::Leaf(expr2),
        ]);
        assert!(eval_filter(&filter, "active"));
        assert!(eval_filter(&filter, "pending"));
        assert!(!eval_filter(&filter, "inactive"));
    }

    #[test]
    fn test_eval_filter_nested_and_or() {
        // (age > 18 OR age < 5) AND status = "valid"
        let age_gt = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        );
        let age_lt = make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("5".to_string(), false)),
        );
        let status_eq = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("valid".to_string())),
        );

        let age_filter = FilterExpr::Or(vec![
            FilterExpr::Leaf(age_gt.clone()),
            FilterExpr::Leaf(age_lt),
        ]);
        let combined = FilterExpr::And(vec![
            age_filter,
            FilterExpr::Leaf(status_eq.clone()),
        ]);

        // This would require actually checking both columns, which our current
        // implementation doesn't support (it only checks one column at a time)
        // So we test just the filter evaluation logic separately
        assert!(eval_filter(&FilterExpr::Leaf(age_gt), "25"));
        assert!(eval_filter(&FilterExpr::Leaf(status_eq), "valid"));
    }

    // Tests for apply_filters
    use crate::entry::Entry;
    use crate::page::Page;
    use crate::metadata_store::{TableDefinition, ColumnDefinition};

    #[test]
    fn test_apply_filters_empty_batch() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = Vec::new();
        step.apply_filters(&mut batch);
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_apply_filters_no_filters() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let step = PipelineStep::new(
            "age".to_string(),
            vec![], // No filters
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2, 3];
        step.apply_filters(&mut batch);
        // Should remain unchanged when no filters
        assert_eq!(batch.len(), 4);
        assert_eq!(batch, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_apply_filters_no_pages_clears_batch() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "nonexistent_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2, 3];
        step.apply_filters(&mut batch);
        // No pages found, batch should be cleared
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_apply_filters_filters_rows() {
        // This test demonstrates the filtering logic even though we don't have
        // actual page data set up. In practice, this would need actual pages
        // with entries registered in the metadata store.
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter],
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2];
        step.apply_filters(&mut batch);

        // Without actual data, all rows will be filtered out
        // (This is expected behavior - rows not found are removed)
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_pipeline_step_execute_with_filters() {
        let page_handler = create_mock_page_handler();
        let (prev_tx, prev_rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, out_rx) = channel::unbounded::<PipelineBatch>();

        let filter = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        ));

        let step = PipelineStep::new(
            "status".to_string(),
            vec![filter],
            out_tx,
            prev_rx,
            false, // Not root
            "test_table".to_string(),
            page_handler,
        );

        // Send a batch through the pipeline
        prev_tx.send(vec![0, 1, 2, 3]).unwrap();
        drop(prev_tx);

        // Execute the step
        step.execute();

        // Should receive filtered results
        let result = out_rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok());
        let batch = result.unwrap();
        // Without real data, all rows filtered out
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_pipeline_step_execute_root() {
        let page_handler = create_mock_page_handler();
        let (_prev_tx, prev_rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, out_rx) = channel::unbounded::<PipelineBatch>();

        let step = PipelineStep::new(
            "col".to_string(),
            vec![],
            out_tx,
            prev_rx,
            true, // Is root
            "test_table".to_string(),
            page_handler,
        );

        // Execute the root step
        step.execute();

        // Root should send an empty batch
        let result = out_rx.recv_timeout(std::time::Duration::from_millis(100));
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_multiple_filters_and_logic() {
        let page_handler = create_mock_page_handler();
        let (_tx, rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, _out_rx) = channel::unbounded::<PipelineBatch>();

        // Multiple filters on the same column (AND logic)
        let filter1 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("18".to_string(), false)),
        ));
        let filter2 = FilterExpr::Leaf(make_binary_op(
            make_ident_expr("age"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("65".to_string(), false)),
        ));

        let step = PipelineStep::new(
            "age".to_string(),
            vec![filter1, filter2], // Both must be true
            out_tx,
            rx,
            false,
            "test_table".to_string(),
            page_handler,
        );

        let mut batch = vec![0, 1, 2];
        step.apply_filters(&mut batch);

        // Without actual data, all filtered out
        assert_eq!(batch.len(), 0);
    }

    // Tests for type-aware comparison (compare_values)

    #[test]
    fn test_compare_values_integer_gt() {
        // This would FAIL with lexicographic comparison: "10" < "2"
        // But succeeds with numeric comparison: 10 > 2
        let expr = make_binary_op(
            make_ident_expr("count"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("2".to_string(), false)),
        );
        assert!(eval_expr(&expr, "10"));
        assert!(eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "2"));
        assert!(!eval_expr(&expr, "1"));
    }

    #[test]
    fn test_compare_values_integer_lt() {
        let expr = make_binary_op(
            make_ident_expr("count"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("100".to_string(), false)),
        );
        assert!(eval_expr(&expr, "10"));
        assert!(eval_expr(&expr, "99"));
        assert!(!eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "200"));
    }

    #[test]
    fn test_compare_values_integer_eq() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::Number("42".to_string(), false)),
        );
        assert!(eval_expr(&expr, "42"));
        assert!(!eval_expr(&expr, "43"));
        assert!(!eval_expr(&expr, "41"));
    }

    #[test]
    fn test_compare_values_integer_negative() {
        let expr = make_binary_op(
            make_ident_expr("temperature"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("-10".to_string(), false)),
        );
        assert!(eval_expr(&expr, "0"));
        assert!(eval_expr(&expr, "-5"));
        assert!(!eval_expr(&expr, "-10"));
        assert!(!eval_expr(&expr, "-20"));
    }

    #[test]
    fn test_compare_values_float_gt() {
        let expr = make_binary_op(
            make_ident_expr("price"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("19.99".to_string(), false)),
        );
        assert!(eval_expr(&expr, "20.0"));
        assert!(eval_expr(&expr, "100.5"));
        assert!(!eval_expr(&expr, "19.99"));
        assert!(!eval_expr(&expr, "10.5"));
    }

    #[test]
    fn test_compare_values_float_lte() {
        let expr = make_binary_op(
            make_ident_expr("score"),
            BinaryOperator::LtEq,
            Expr::Value(Value::Number("3.14159".to_string(), false)),
        );
        assert!(eval_expr(&expr, "3.14159"));
        assert!(eval_expr(&expr, "3.0"));
        assert!(eval_expr(&expr, "0.5"));
        assert!(!eval_expr(&expr, "3.2"));
        assert!(!eval_expr(&expr, "10.0"));
    }

    #[test]
    fn test_compare_values_float_scientific() {
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1e3".to_string(), false)),
        );
        assert!(eval_expr(&expr, "2000"));
        assert!(eval_expr(&expr, "1001"));
        assert!(!eval_expr(&expr, "1000"));
        assert!(!eval_expr(&expr, "999"));
    }

    #[test]
    fn test_compare_values_boolean_true() {
        let expr = make_binary_op(
            make_ident_expr("active"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("true".to_string())),
        );
        assert!(eval_expr(&expr, "true"));
        assert!(eval_expr(&expr, "True"));
        assert!(eval_expr(&expr, "TRUE"));
        assert!(eval_expr(&expr, "t"));
        assert!(eval_expr(&expr, "1"));
        assert!(eval_expr(&expr, "yes"));
        assert!(!eval_expr(&expr, "false"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_compare_values_boolean_false() {
        let expr = make_binary_op(
            make_ident_expr("disabled"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("false".to_string())),
        );
        assert!(eval_expr(&expr, "false"));
        assert!(eval_expr(&expr, "False"));
        assert!(eval_expr(&expr, "FALSE"));
        assert!(eval_expr(&expr, "f"));
        assert!(eval_expr(&expr, "0"));
        assert!(eval_expr(&expr, "no"));
        assert!(!eval_expr(&expr, "true"));
        assert!(!eval_expr(&expr, "1"));
    }

    #[test]
    fn test_compare_values_boolean_gt() {
        // true > false
        let expr = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("false".to_string())),
        );
        assert!(eval_expr(&expr, "true"));
        assert!(eval_expr(&expr, "1"));
        assert!(!eval_expr(&expr, "false"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_compare_values_string_fallback() {
        // When not numbers or bools, falls back to lexicographic
        let expr = make_binary_op(
            make_ident_expr("name"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("Alice".to_string())),
        );
        assert!(eval_expr(&expr, "Bob"));
        assert!(eval_expr(&expr, "Charlie"));
        assert!(!eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "Aaron"));
    }

    #[test]
    fn test_compare_values_string_eq() {
        let expr = make_binary_op(
            make_ident_expr("status"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("active".to_string())),
        );
        assert!(eval_expr(&expr, "active"));
        assert!(!eval_expr(&expr, "inactive"));
        assert!(!eval_expr(&expr, "pending"));
    }

    #[test]
    fn test_compare_values_mixed_numeric_string() {
        // If both can't be parsed as same type, falls back to string comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("abc".to_string())),
        );
        // Comparing number to non-number falls back to string
        assert!(eval_expr(&expr, "def"));
        assert!(!eval_expr(&expr, "123")); // "123" < "abc" lexicographically
    }

    #[test]
    fn test_compare_values_integer_vs_float() {
        // Integer parsing takes precedence, but if one is float, falls back to float comparison
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("10.5".to_string(), false)),
        );
        // "11" vs "10.5" - 11 can be parsed as i64, but 10.5 can't, so uses f64
        assert!(eval_expr(&expr, "11"));
        assert!(eval_expr(&expr, "10.6"));
        assert!(!eval_expr(&expr, "10"));
        assert!(!eval_expr(&expr, "10.5"));
    }

    #[test]
    fn test_compare_values_not_eq_integer() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::NotEq,
            Expr::Value(Value::Number("42".to_string(), false)),
        );
        assert!(eval_expr(&expr, "43"));
        assert!(eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, "42"));
    }

    #[test]
    fn test_compare_values_gte_float() {
        let expr = make_binary_op(
            make_ident_expr("score"),
            BinaryOperator::GtEq,
            Expr::Value(Value::Number("3.14".to_string(), false)),
        );
        assert!(eval_expr(&expr, "3.14"));
        assert!(eval_expr(&expr, "3.15"));
        assert!(eval_expr(&expr, "10.0"));
        assert!(!eval_expr(&expr, "3.13"));
        assert!(!eval_expr(&expr, "0"));
    }

    #[test]
    fn test_parse_bool_variations() {
        // Test the parse_bool helper directly through eval_expr
        let expr_true = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("yes".to_string())),
        );
        assert!(eval_expr(&expr_true, "y"));
        assert!(eval_expr(&expr_true, "Y"));
        assert!(eval_expr(&expr_true, "YES"));

        let expr_false = make_binary_op(
            make_ident_expr("flag"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("no".to_string())),
        );
        assert!(eval_expr(&expr_false, "n"));
        assert!(eval_expr(&expr_false, "N"));
        assert!(eval_expr(&expr_false, "NO"));
    }

    #[test]
    fn test_compare_values_zero_comparisons() {
        let expr = make_binary_op(
            make_ident_expr("balance"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("0".to_string(), false)),
        );
        assert!(eval_expr(&expr, "1"));
        assert!(eval_expr(&expr, "100"));
        assert!(!eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, "-1"));
    }

    #[test]
    fn test_compare_values_large_numbers() {
        let expr = make_binary_op(
            make_ident_expr("population"),
            BinaryOperator::Lt,
            Expr::Value(Value::Number("1000000".to_string(), false)),
        );
        assert!(eval_expr(&expr, "999999"));
        assert!(eval_expr(&expr, "1"));
        assert!(!eval_expr(&expr, "1000000"));
        assert!(!eval_expr(&expr, "1000001"));
    }

    // ========== Date/Time Comparison Tests ==========

    #[test]
    fn test_datetime_iso_date_comparison() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("2024-01-01".to_string())),
        );
        assert!(eval_expr(&expr, "2024-12-31"));
        assert!(eval_expr(&expr, "2025-01-01"));
        assert!(!eval_expr(&expr, "2024-01-01"));
        assert!(!eval_expr(&expr, "2023-12-31"));
    }

    #[test]
    fn test_datetime_iso_timestamp_comparison() {
        let expr = make_binary_op(
            make_ident_expr("timestamp"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2024-06-15 12:00:00".to_string())),
        );
        assert!(eval_expr(&expr, "2024-06-15 11:59:59"));
        assert!(eval_expr(&expr, "2024-01-01 00:00:00"));
        assert!(!eval_expr(&expr, "2024-06-15 12:00:00"));
        assert!(!eval_expr(&expr, "2024-06-15 12:00:01"));
    }

    #[test]
    fn test_datetime_unix_timestamp() {
        let expr = make_binary_op(
            make_ident_expr("ts"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1704067200".to_string(), false)), // 2024-01-01 00:00:00 UTC
        );
        assert!(eval_expr(&expr, "1735689600")); // 2025-01-01
        assert!(!eval_expr(&expr, "1672531200")); // 2023-01-01
    }

    #[test]
    fn test_datetime_common_format_mdy() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("01/15/2024".to_string())),
        );
        assert!(eval_expr(&expr, "2024-01-15")); // Same date, different format
    }

    #[test]
    fn test_datetime_leap_year() {
        let expr = make_binary_op(
            make_ident_expr("date"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2024-03-01".to_string())),
        );
        assert!(eval_expr(&expr, "2024-02-29")); // Leap year
        assert!(!eval_expr(&expr, "2024-03-01"));
    }

    // ========== NULL Handling Tests ==========

    #[test]
    fn test_is_null_empty_string() {
        let expr = Expr::IsNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, ""));
        assert!(!eval_expr(&expr, "something"));
    }

    #[test]
    fn test_is_null_null_string() {
        let expr = Expr::IsNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, "null"));
        assert!(eval_expr(&expr, "NULL"));
        assert!(eval_expr(&expr, "Null"));
        assert!(eval_expr(&expr, "nil"));
        assert!(!eval_expr(&expr, "not null"));
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(make_ident_expr("value")));
        assert!(eval_expr(&expr, "something"));
        assert!(eval_expr(&expr, "0"));
        assert!(!eval_expr(&expr, ""));
        assert!(!eval_expr(&expr, "null"));
    }

    // ========== LIKE Pattern Matching Tests ==========

    #[test]
    fn test_like_exact_match() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "Alice"));
        assert!(!eval_expr(&expr, "alice")); // Case sensitive
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_like_starts_with() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("John%".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "John"));
        assert!(eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "Johnson"));
        assert!(!eval_expr(&expr, "Bob"));
        assert!(!eval_expr(&expr, "john")); // Case sensitive
    }

    #[test]
    fn test_like_ends_with() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("email")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("%@gmail.com".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "user@gmail.com"));
        assert!(eval_expr(&expr, "test.user@gmail.com"));
        assert!(!eval_expr(&expr, "user@yahoo.com"));
    }

    #[test]
    fn test_like_contains() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("%world%".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "world"));
        assert!(eval_expr(&expr, "the world is big"));
        assert!(!eval_expr(&expr, "hello earth"));
    }

    #[test]
    fn test_like_single_char_wildcard() {
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(make_ident_expr("code")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("AB_123".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "ABC123"));
        assert!(eval_expr(&expr, "ABX123"));
        assert!(!eval_expr(&expr, "AB123")); // Missing one char
        assert!(!eval_expr(&expr, "ABCD123")); // Too many chars
    }

    #[test]
    fn test_like_negated() {
        let expr = Expr::Like {
            negated: true,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("John%".to_string()))),
            escape_char: None,
        };
        assert!(!eval_expr(&expr, "John"));
        assert!(!eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_ilike_case_insensitive() {
        let expr = Expr::ILike {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("ALICE".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "alice"));
        assert!(eval_expr(&expr, "Alice"));
        assert!(eval_expr(&expr, "ALICE"));
        assert!(!eval_expr(&expr, "Bob"));
    }

    #[test]
    fn test_ilike_starts_with_case_insensitive() {
        let expr = Expr::ILike {
            negated: false,
            expr: Box::new(make_ident_expr("name")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("john%".to_string()))),
            escape_char: None,
        };
        assert!(eval_expr(&expr, "JOHN"));
        assert!(eval_expr(&expr, "Johnny"));
        assert!(eval_expr(&expr, "JOHNSON"));
    }

    // ========== IP Address Comparison Tests ==========

    #[test]
    fn test_ip_address_comparison_gt() {
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("192.168.1.1".to_string())),
        );
        assert!(eval_expr(&expr, "192.168.1.2"));
        assert!(eval_expr(&expr, "192.168.1.100"));
        assert!(eval_expr(&expr, "192.168.2.1"));
        assert!(!eval_expr(&expr, "192.168.1.1"));
        assert!(!eval_expr(&expr, "192.168.0.255"));
    }

    #[test]
    fn test_ip_address_comparison_lexicographic_wrong() {
        // This demonstrates why we need special IP parsing
        // Lexicographically: "192.168.1.100" < "192.168.1.2" (wrong!)
        // Numerically: 192.168.1.100 > 192.168.1.2 (correct!)
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("192.168.1.2".to_string())),
        );
        assert!(eval_expr(&expr, "192.168.1.100")); // Correct with IP parsing
    }

    #[test]
    fn test_ip_address_different_octets() {
        let expr = make_binary_op(
            make_ident_expr("ip"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("10.0.0.1".to_string())),
        );
        assert!(eval_expr(&expr, "9.255.255.255"));
        assert!(!eval_expr(&expr, "10.0.0.1"));
        assert!(!eval_expr(&expr, "10.0.0.2"));
    }

    // ========== UUID Comparison Tests ==========

    #[test]
    fn test_uuid_comparison() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString(
                "550e8400-e29b-41d4-a716-446655440001".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
        assert!(!eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440001"));
        assert!(!eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440002"));
    }

    #[test]
    fn test_uuid_case_insensitive() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString(
                "550E8400-E29B-41D4-A716-446655440000".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
        assert!(eval_expr(&expr, "550E8400-E29B-41D4-A716-446655440000"));
    }

    #[test]
    fn test_uuid_no_hyphens() {
        let expr = make_binary_op(
            make_ident_expr("id"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString(
                "550e8400e29b41d4a716446655440000".to_string(),
            )),
        );
        assert!(eval_expr(&expr, "550e8400-e29b-41d4-a716-446655440000"));
    }

    // ========== Version String Comparison Tests ==========

    #[test]
    fn test_version_major_comparison() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.0.0".to_string())),
        );
        assert!(eval_expr(&expr, "2.0.0"));
        assert!(eval_expr(&expr, "10.0.0"));
        assert!(!eval_expr(&expr, "1.0.0"));
        assert!(!eval_expr(&expr, "0.9.9"));
    }

    #[test]
    fn test_version_minor_comparison() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("1.5.0".to_string())),
        );
        assert!(eval_expr(&expr, "1.4.0"));
        assert!(eval_expr(&expr, "1.0.0"));
        assert!(!eval_expr(&expr, "1.5.0"));
        assert!(!eval_expr(&expr, "1.6.0"));
    }

    #[test]
    fn test_version_patch_comparison() {
        // This would FAIL with lexicographic: "1.2.9" > "1.2.10"
        // But works with version parsing: 1.2.10 > 1.2.9
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.2.9".to_string())),
        );
        assert!(eval_expr(&expr, "1.2.10"));
        assert!(eval_expr(&expr, "1.2.100"));
        assert!(!eval_expr(&expr, "1.2.9"));
        assert!(!eval_expr(&expr, "1.2.8"));
    }

    #[test]
    fn test_version_short_format() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("2.0".to_string())),
        );
        assert!(eval_expr(&expr, "2.0.0"));
        assert!(eval_expr(&expr, "2.0"));
    }

    #[test]
    fn test_version_with_prerelease() {
        let expr = make_binary_op(
            make_ident_expr("version"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1.0.0-alpha".to_string())),
        );
        assert!(eval_expr(&expr, "1.0.0-beta")); // beta > alpha lexicographically
        assert!(eval_expr(&expr, "1.0.0"));       // Release > prerelease
    }

    // ========== Duration Comparison Tests ==========

    #[test]
    fn test_duration_simple_format() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1 hour".to_string())),
        );
        assert!(eval_expr(&expr, "2 hours"));
        assert!(eval_expr(&expr, "90 minutes")); // 90 min > 60 min
        assert!(!eval_expr(&expr, "30 minutes"));
    }

    #[test]
    fn test_duration_days_vs_hours() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Lt,
            Expr::Value(Value::SingleQuotedString("2 days".to_string())),
        );
        assert!(eval_expr(&expr, "1 day"));
        assert!(eval_expr(&expr, "24 hours"));
        assert!(eval_expr(&expr, "47 hours")); // 47h < 48h (2 days)
        assert!(!eval_expr(&expr, "48 hours")); // Equal
        assert!(!eval_expr(&expr, "3 days"));
    }

    #[test]
    fn test_duration_compact_format() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("90 minutes".to_string())),
        );
        assert!(eval_expr(&expr, "1h30m")); // Same duration
    }

    #[test]
    fn test_duration_complex_compact() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("1h".to_string())),
        );
        assert!(eval_expr(&expr, "1h1s"));
        assert!(eval_expr(&expr, "2h30m15s"));
        assert!(!eval_expr(&expr, "59m"));
    }

    #[test]
    fn test_duration_weeks() {
        let expr = make_binary_op(
            make_ident_expr("duration"),
            BinaryOperator::Eq,
            Expr::Value(Value::SingleQuotedString("1 week".to_string())),
        );
        assert!(eval_expr(&expr, "7 days"));
        assert!(eval_expr(&expr, "168 hours"));
    }

    // ========== Regex Pattern Matching Tests ==========

    #[test]
    fn test_regex_simple_contains() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "this is a test"));
        assert!(eval_expr(&expr, "test"));
        assert!(!eval_expr(&expr, "no match"));
    }

    #[test]
    fn test_regex_starts_with_anchor() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("^hello".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "hello"));
        assert!(!eval_expr(&expr, "say hello"));
    }

    #[test]
    fn test_regex_ends_with_anchor() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("world$".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "hello world"));
        assert!(eval_expr(&expr, "world"));
        assert!(!eval_expr(&expr, "world peace"));
    }

    #[test]
    fn test_regex_both_anchors() {
        let expr = Expr::RLike {
            negated: false,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("^exact$".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "exact"));
        assert!(!eval_expr(&expr, "exact match"));
        assert!(!eval_expr(&expr, "not exact"));
    }

    #[test]
    fn test_regex_negated() {
        let expr = Expr::RLike {
            negated: true,
            expr: Box::new(make_ident_expr("text")),
            pattern: Box::new(Expr::Value(Value::SingleQuotedString("error".to_string()))),
            regexp: false,
        };
        assert!(eval_expr(&expr, "success"));
        assert!(!eval_expr(&expr, "error occurred"));
    }

    // ========== Integration Tests (Multiple Type Comparisons) ==========

    #[test]
    fn test_mixed_types_fallback_correctly() {
        // When comparing different types, should fall back gracefully
        let expr = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("2024-01-01".to_string())),
        );
        // Both parsed as dates
        assert!(eval_expr(&expr, "2024-12-31"));

        // If one isn't a date, falls back to string comparison
        let expr2 = make_binary_op(
            make_ident_expr("value"),
            BinaryOperator::Gt,
            Expr::Value(Value::SingleQuotedString("not-a-date".to_string())),
        );
        assert!(eval_expr(&expr2, "zebra")); // String comparison
    }

    #[test]
    fn test_number_vs_date_priority() {
        // Numbers are tried before dates, so Unix timestamps work
        let expr = make_binary_op(
            make_ident_expr("timestamp"),
            BinaryOperator::Gt,
            Expr::Value(Value::Number("1704067200".to_string(), false)),
        );
        assert!(eval_expr(&expr, "1735689600"));
    }
}
