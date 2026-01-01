use super::SqlExecutionError;
use super::values::{compare_strs, is_encoded_null};
use regex::Regex;
use sqlparser::ast::{
    DateTimeField, Expr, ObjectName, Offset, TableFactor, TableWithJoins, Value,
    WildcardAdditionalOptions,
};
use std::collections::{BTreeSet, HashMap};

pub(super) fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn table_with_joins_to_name(
    table: &TableWithJoins,
) -> Result<String, SqlExecutionError> {
    if !table.joins.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "UPDATE with JOINs is not supported".into(),
        ));
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(SqlExecutionError::Unsupported(
            "unsupported table reference".into(),
        )),
    }
}

pub(super) fn expr_to_string(expr: &Expr) -> Result<String, SqlExecutionError> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        Expr::Value(Value::Number(n, _)) => Ok(n.clone()),
        Expr::Value(Value::Boolean(b)) => Ok(if *b { "true" } else { "false" }.into()),
        Expr::Value(Value::Null) => Ok(super::values::encode_null()),
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("default") => {
            Ok(super::values::encode_null())
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "unsupported literal expression: {expr:?}"
        ))),
    }
}

pub(super) fn column_name_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        Expr::Nested(inner) => column_name_from_expr(inner),
        _ => None,
    }
}

pub(super) fn parse_limit(limit: Option<Expr>) -> Result<Option<usize>, SqlExecutionError> {
    match limit {
        None => Ok(None),
        Some(Expr::Identifier(ident)) if ident.value.eq_ignore_ascii_case("all") => Ok(None),
        Some(expr) => parse_usize_literal(&expr, "LIMIT").map(Some),
    }
}

pub(super) fn parse_offset(offset: Option<Offset>) -> Result<usize, SqlExecutionError> {
    match offset {
        None => Ok(0),
        Some(offset) => parse_usize_literal(&offset.value, "OFFSET"),
    }
}

pub(super) fn parse_usize_literal(expr: &Expr, context: &str) -> Result<usize, SqlExecutionError> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<usize>().map_err(|_| {
            SqlExecutionError::Unsupported(format!(
                "{context} requires a non-negative integer literal"
            ))
        }),
        _ => Err(SqlExecutionError::Unsupported(format!(
            "{context} requires a numeric literal"
        ))),
    }
}

pub(super) fn parse_interval_seconds(expr: &Expr, context: &str) -> Result<f64, SqlExecutionError> {
    match expr {
        Expr::Value(Value::Number(value, _)) => value.parse::<f64>().map_err(|_| {
            SqlExecutionError::Unsupported(format!("{context} requires numeric interval"))
        }),
        Expr::Value(Value::SingleQuotedString(s)) => parse_interval_string(s).ok_or_else(|| {
            SqlExecutionError::Unsupported(format!(
                "{context} interval literal '{s}' is not supported"
            ))
        }),
        Expr::Interval(interval) => {
            let base = parse_interval_seconds(interval.value.as_ref(), context)?;
            let multiplier = interval
                .leading_field
                .map(interval_unit_multiplier)
                .transpose()
                .map_err(|_| {
                    SqlExecutionError::Unsupported(format!(
                        "{context} interval unit is not supported"
                    ))
                })?
                .unwrap_or(1.0);
            Ok(base * multiplier)
        }
        _ => Err(SqlExecutionError::Unsupported(format!(
            "{context} requires literal interval"
        ))),
    }
}

fn parse_interval_string(input: &str) -> Option<f64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let value_part = parts.next()?;
    let mut value = value_part.parse::<f64>().ok()?;

    if let Some(unit_part) = parts.next() {
        if parts.next().is_some() {
            return None;
        }
        let multiplier = string_unit_multiplier(unit_part)?;
        value *= multiplier;
    }

    Some(value)
}

fn string_unit_multiplier(unit: &str) -> Option<f64> {
    let normalized = unit.trim().to_lowercase();
    match normalized.as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => Some(1.0),
        "millisecond" | "milliseconds" | "ms" => Some(0.001),
        "microsecond" | "microseconds" | "us" => Some(0.000001),
        "minute" | "minutes" | "min" | "mins" => Some(60.0),
        "hour" | "hours" | "hr" | "hrs" => Some(3_600.0),
        "day" | "days" => Some(86_400.0),
        "week" | "weeks" => Some(604_800.0),
        _ => None,
    }
}

fn interval_unit_multiplier(field: DateTimeField) -> Result<f64, ()> {
    match field {
        DateTimeField::Second => Ok(1.0),
        DateTimeField::Millisecond => Ok(0.001),
        DateTimeField::Microsecond => Ok(0.000001),
        DateTimeField::Minute => Ok(60.0),
        DateTimeField::Hour => Ok(3_600.0),
        DateTimeField::Day => Ok(86_400.0),
        DateTimeField::Week => Ok(604_800.0),
        DateTimeField::Month => Err(()),
        DateTimeField::Quarter => Err(()),
        DateTimeField::Year => Err(()),
        DateTimeField::Decade => Err(()),
        DateTimeField::Century => Err(()),
        DateTimeField::Millennium => Err(()),
        _ => Err(()),
    }
}

pub(super) fn extract_equality_filters(
    expr: &Expr,
) -> Result<HashMap<String, String>, SqlExecutionError> {
    let mut filters = HashMap::new();
    gather_filters(expr, &mut filters)?;
    Ok(filters)
}

fn gather_filters(
    expr: &Expr,
    filters: &mut HashMap<String, String>,
) -> Result<(), SqlExecutionError> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == sqlparser::ast::BinaryOperator::And => {
            gather_filters(left, filters)?;
            gather_filters(right, filters)
        }
        Expr::BinaryOp { left, op, right } if *op == sqlparser::ast::BinaryOperator::Eq => {
            let column = column_name_from_expr(left).ok_or_else(|| {
                SqlExecutionError::Unsupported(
                    "only column = literal predicates are supported".into(),
                )
            })?;
            let value = expr_to_string(right)?;
            match filters.get(&column) {
                Some(existing) if compare_strs(existing, &value) != std::cmp::Ordering::Equal => {
                    return Err(SqlExecutionError::Unsupported(format!(
                        "conflicting predicates for column {column}"
                    )));
                }
                _ => {
                    filters.insert(column, value);
                }
            }
            Ok(())
        }
        Expr::Nested(inner) => gather_filters(inner, filters),
        _ => Err(SqlExecutionError::Unsupported(
            "only ANDed column = literal predicates are supported".into(),
        )),
    }
}

pub(super) fn collect_expr_column_ordinals(
    expr: &Expr,
    column_ordinals: &HashMap<String, usize>,
    table: &str,
) -> Result<BTreeSet<usize>, SqlExecutionError> {
    let mut names = BTreeSet::new();
    collect_expr_column_names(expr, &mut names);

    let mut ordinals = BTreeSet::new();
    for name in names {
        let ordinal = column_ordinals.get(&name).copied().ok_or_else(|| {
            SqlExecutionError::ColumnMismatch {
                table: table.to_string(),
                column: name.clone(),
            }
        })?;
        ordinals.insert(ordinal);
    }

    Ok(ordinals)
}

pub(super) fn collect_expr_column_names(expr: &Expr, columns: &mut BTreeSet<String>) {
    use sqlparser::ast::*;

    match expr {
        Expr::Identifier(ident) => {
            columns.insert(ident.value.clone());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.insert(last.value.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_column_names(left, columns);
            collect_expr_column_names(right, columns);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_expr_column_names(expr, columns);
            collect_expr_column_names(pattern, columns);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_column_names(expr, columns);
            collect_expr_column_names(low, columns);
            collect_expr_column_names(high, columns);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_column_names(expr, columns);
            for item in list {
                collect_expr_column_names(item, columns);
            }
        }
        Expr::UnaryOp { expr, .. } => collect_expr_column_names(expr, columns),
        Expr::Nested(inner) => collect_expr_column_names(inner, columns),
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::SafeCast { expr: inner, .. }
        | Expr::TryCast { expr: inner, .. }
        | Expr::Convert { expr: inner, .. }
        | Expr::Extract { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::Ceil { expr: inner, .. }
        | Expr::Floor { expr: inner, .. }
        | Expr::AtTimeZone {
            timestamp: inner, ..
        } => collect_expr_column_names(inner, columns),
        Expr::JsonAccess { left, right, .. } => {
            collect_expr_column_names(left, columns);
            collect_expr_column_names(right, columns);
        }
        Expr::GroupingSets(sets) => {
            for group in sets {
                for expr in group {
                    collect_expr_column_names(expr, columns);
                }
            }
        }
        Expr::Rollup(groups) => {
            for group in groups {
                for expr in group {
                    collect_expr_column_names(expr, columns);
                }
            }
        }
        Expr::Function(function) => {
            for function_arg in &function.args {
                match function_arg {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            collect_expr_column_names(expr, columns);
                        }
                    }
                }
            }
            if let Some(filter) = &function.filter {
                collect_expr_column_names(filter, columns);
            }
            for order in &function.order_by {
                collect_expr_column_names(&order.expr, columns);
            }
            if let Some(over) = &function.over {
                if let WindowType::WindowSpec(spec) = over {
                    for expr in &spec.partition_by {
                        collect_expr_column_names(expr, columns);
                    }
                    for order in &spec.order_by {
                        collect_expr_column_names(&order.expr, columns);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_expr_column_names(op, columns);
            }
            for condition in conditions {
                collect_expr_column_names(condition, columns);
            }
            for result in results {
                collect_expr_column_names(result, columns);
            }
            if let Some(else_expr) = else_result {
                collect_expr_column_names(else_expr, columns);
            }
        }
        _ => {}
    }
}

pub(super) fn wildcard_options_supported(options: &WildcardAdditionalOptions) -> bool {
    options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_rename.is_none()
        && options.opt_replace.is_none()
}

pub(super) fn object_name_matches_table(
    object_name: &ObjectName,
    table_name: &str,
    table_alias: Option<&str>,
) -> bool {
    object_name
        .0
        .last()
        .map(|ident| {
            ident.value == table_name
                || table_alias
                    .map(|alias| ident.value == alias)
                    .unwrap_or(false)
        })
        .unwrap_or(false)
}

pub(super) fn is_null_value(value: &str) -> bool {
    value.is_empty()
        || value.eq_ignore_ascii_case("null")
        || value.eq_ignore_ascii_case("nil")
        || is_encoded_null(value)
}

pub(crate) fn like_match(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    let val = if case_sensitive {
        value.to_string()
    } else {
        value.to_lowercase()
    };
    let pat = if case_sensitive {
        pattern.to_string()
    } else {
        pattern.to_lowercase()
    };

    like_match_recursive(&val, &pat)
}

fn like_match_recursive(value: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return value.is_empty();
    }

    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();

    like_match_dfs(&value_chars, &pattern_chars, 0, 0)
}

fn like_match_dfs(value: &[char], pattern: &[char], mut v_idx: usize, mut p_idx: usize) -> bool {
    let v_len = value.len();
    let p_len = pattern.len();

    while p_idx < p_len {
        match pattern[p_idx] {
            '%' => {
                while p_idx + 1 < p_len && pattern[p_idx + 1] == '%' {
                    p_idx += 1;
                }
                if p_idx + 1 == p_len {
                    return true;
                }
                p_idx += 1;
                while v_idx <= v_len {
                    if like_match_dfs(&value[v_idx..], &pattern[p_idx..], 0, 0) {
                        return true;
                    }
                    if v_idx == v_len {
                        break;
                    }
                    v_idx += 1;
                }
                return false;
            }
            '_' => {
                if v_idx >= v_len {
                    return false;
                }
                v_idx += 1;
                p_idx += 1;
            }
            ch => {
                if v_idx >= v_len || value[v_idx] != ch {
                    return false;
                }
                v_idx += 1;
                p_idx += 1;
            }
        }
    }

    v_idx == v_len
}

pub(super) fn regex_match(value: &str, pattern: &str) -> bool {
    match Regex::new(pattern) {
        Ok(re) => re.is_match(value),
        Err(_) => false,
    }
}
