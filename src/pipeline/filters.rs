use crate::sql::models::FilterExpr;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

pub fn eval_filter(filter: &FilterExpr, value: &str) -> bool {
    match filter {
        FilterExpr::Leaf(expr) => eval_expr(expr, value),
        FilterExpr::And(filters) => filters.iter().all(|f| eval_filter(f, value)),
        FilterExpr::Or(filters) => filters.iter().any(|f| eval_filter(f, value)),
    }
}

pub fn eval_expr(expr: &Expr, value: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**left {
                match &**right {
                    Expr::Value(Value::SingleQuotedString(pattern))
                    | Expr::Value(Value::DoubleQuotedString(pattern)) => {
                        return eval_binary_op_string(value, op, pattern);
                    }
                    Expr::Value(Value::Number(num_str, _)) => {
                        return eval_binary_op_number(value, op, num_str);
                    }
                    Expr::Value(Value::Boolean(b)) => {
                        return eval_binary_op_bool(value, op, *b);
                    }
                    Expr::Value(Value::Null) => {
                        return eval_binary_op_null(value, op);
                    }
                    _ => {}
                }
            }
            false
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                let result = eval_between(value, low, high);
                return if *negated { !result } else { result };
            }
            false
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                let result = list.iter().any(|item| {
                    if let Expr::Value(val) = item {
                        match val {
                            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                                value == s
                            }
                            Value::Number(n, _) => {
                                if let Ok(val_num) = value.parse::<f64>() {
                                    if let Ok(target) = n.parse::<f64>() {
                                        (val_num - target).abs() < f64::EPSILON
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                            Value::Null => super::parsers::is_null(value),
                            _ => false,
                        }
                    } else {
                        false
                    }
                });
                return if *negated { !result } else { result };
            }
            false
        }
        Expr::Like {
            negated,
            expr: inner,
            pattern,
            escape_char: _,
        } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                if let Expr::Value(Value::SingleQuotedString(pat))
                | Expr::Value(Value::DoubleQuotedString(pat)) = &**pattern
                {
                    let result = super::pattern_matching::like_match(value, pat, true);
                    return if *negated { !result } else { result };
                }
            }
            false
        }
        Expr::ILike {
            negated,
            expr: inner,
            pattern,
            escape_char: _,
        } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                if let Expr::Value(Value::SingleQuotedString(pat))
                | Expr::Value(Value::DoubleQuotedString(pat)) = &**pattern
                {
                    let result = super::pattern_matching::like_match(value, pat, false);
                    return if *negated { !result } else { result };
                }
            }
            false
        }
        Expr::RLike {
            negated,
            expr: inner,
            pattern,
            ..
        } => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                if let Expr::Value(Value::SingleQuotedString(pat))
                | Expr::Value(Value::DoubleQuotedString(pat)) = &**pattern
                {
                    let result = super::pattern_matching::regex_match(value, pat);
                    return if *negated { !result } else { result };
                }
            }
            false
        }
        Expr::IsNull(inner) => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                return super::parsers::is_null(value);
            }
            false
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &**inner {
                return !super::parsers::is_null(value);
            }
            false
        }
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Not => !eval_expr(inner, value),
            _ => false,
        },
        Expr::Nested(inner) => eval_expr(inner, value),
        _ => false,
    }
}

fn eval_binary_op_string(value: &str, op: &BinaryOperator, pattern: &str) -> bool {
    use BinaryOperator::*;
    match op {
        Eq => {
            super::parsers::compare_values(value, pattern, |ord| ord == std::cmp::Ordering::Equal)
        }
        NotEq => {
            super::parsers::compare_values(value, pattern, |ord| ord != std::cmp::Ordering::Equal)
        }
        Lt => super::parsers::compare_values(value, pattern, |ord| ord == std::cmp::Ordering::Less),
        LtEq => {
            super::parsers::compare_values(value, pattern, |ord| ord != std::cmp::Ordering::Greater)
        }
        Gt => {
            super::parsers::compare_values(value, pattern, |ord| ord == std::cmp::Ordering::Greater)
        }
        GtEq => {
            super::parsers::compare_values(value, pattern, |ord| ord != std::cmp::Ordering::Less)
        }
        _ => false,
    }
}

fn eval_binary_op_number(value: &str, op: &BinaryOperator, num_str: &str) -> bool {
    let val_num = match value.parse::<f64>() {
        Ok(n) => n,
        Err(_) => return false,
    };
    let target = match num_str.parse::<f64>() {
        Ok(n) => n,
        Err(_) => return false,
    };

    use BinaryOperator::*;
    match op {
        Eq => (val_num - target).abs() < f64::EPSILON,
        NotEq => (val_num - target).abs() >= f64::EPSILON,
        Lt => val_num < target,
        LtEq => val_num <= target,
        Gt => val_num > target,
        GtEq => val_num >= target,
        _ => false,
    }
}

fn eval_binary_op_bool(value: &str, op: &BinaryOperator, target: bool) -> bool {
    let val_bool = match super::parsers::parse_bool(value) {
        Ok(b) => b,
        Err(_) => return false,
    };

    use BinaryOperator::*;
    match op {
        Eq => val_bool == target,
        NotEq => val_bool != target,
        _ => false,
    }
}

fn eval_binary_op_null(value: &str, op: &BinaryOperator) -> bool {
    let is_null = super::parsers::is_null(value);
    use BinaryOperator::*;
    match op {
        Eq => is_null,
        NotEq => !is_null,
        _ => false,
    }
}

fn eval_between(value: &str, low: &Expr, high: &Expr) -> bool {
    let low_str = match low {
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => s,
        _ => return false,
    };
    let high_str = match high {
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => s,
        _ => return false,
    };

    let ge_low =
        super::parsers::compare_values(value, low_str, |ord| ord != std::cmp::Ordering::Less);
    let le_high =
        super::parsers::compare_values(value, high_str, |ord| ord != std::cmp::Ordering::Greater);
    ge_low && le_high
}
