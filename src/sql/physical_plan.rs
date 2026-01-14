use crate::sql::runtime::values::ScalarValue;
use crate::sql::types::DataType;
use sqlparser::ast::{BinaryOperator, UnaryOperator};
use std::fmt;

/// A Physical Expression is ready to be executed without looking up metadata.
/// - Column names are resolved to Ordinals (indices).
/// - Literals are parsed into ScalarValues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalExpr {
    /// A constant value (e.g., 25, true, "2024-01-01" parsed as int)
    Literal(ScalarValue),

    /// A reference to a column by index in the ColumnarBatch
    Column {
        name: String, // Kept for debugging
        index: usize,
        data_type: DataType,
    },

    /// A binary operation (e.g., age > 25)
    BinaryOp {
        left: Box<PhysicalExpr>,
        op: BinaryOperator,
        right: Box<PhysicalExpr>,
    },

    /// A unary operation (e.g. NOT true, -5)
    UnaryOp {
        op: UnaryOperator,
        expr: Box<PhysicalExpr>,
    },

    /// LIKE / ILIKE pattern match
    Like {
        expr: Box<PhysicalExpr>,
        pattern: Box<PhysicalExpr>,
        case_insensitive: bool,
        negated: bool,
    },
    /// RLIKE regex match
    RLike {
        expr: Box<PhysicalExpr>,
        pattern: Box<PhysicalExpr>,
        negated: bool,
    },

    /// IN list match
    InList {
        expr: Box<PhysicalExpr>,
        list: Vec<PhysicalExpr>,
        negated: bool,
    },

    /// Cast operation (Explicit or Implicit)
    Cast {
        expr: Box<PhysicalExpr>,
        target_type: DataType,
    },

    /// IS NULL check
    IsNull(Box<PhysicalExpr>),

    /// IS NOT NULL check
    IsNotNull(Box<PhysicalExpr>),
}

impl fmt::Display for PhysicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalExpr::Literal(val) => write!(f, "{}", format_scalar(val)),
            PhysicalExpr::Column { name, .. } => write!(f, "{name}"),
            PhysicalExpr::BinaryOp { left, op, right } => {
                if let Some(op_str) = format_binary_op(op) {
                    write!(f, "{} {} {}", left, op_str, right)
                } else {
                    write!(f, "{} {:?} {}", left, op, right)
                }
            }
            PhysicalExpr::UnaryOp { op, expr } => {
                if let Some(op_str) = format_unary_op(op) {
                    write!(f, "{} {}", op_str, expr)
                } else {
                    write!(f, "{:?} {}", op, expr)
                }
            }
            PhysicalExpr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
            } => {
                let op = match (*case_insensitive, *negated) {
                    (true, true) => "NOT ILIKE",
                    (true, false) => "ILIKE",
                    (false, true) => "NOT LIKE",
                    (false, false) => "LIKE",
                };
                write!(f, "{} {} {}", expr, op, pattern)
            }
            PhysicalExpr::RLike {
                expr,
                pattern,
                negated,
            } => {
                let op = if *negated { "NOT RLIKE" } else { "RLIKE" };
                write!(f, "{} {} {}", expr, op, pattern)
            }
            PhysicalExpr::InList {
                expr,
                list,
                negated,
            } => {
                let op = if *negated { "NOT IN" } else { "IN" };
                let list_str = list
                    .iter()
                    .map(format_scalar_expr)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} ({})", expr, op, list_str)
            }
            PhysicalExpr::Cast { expr, target_type } => {
                write!(f, "CAST({} AS {})", expr, target_type)
            }
            PhysicalExpr::IsNull(expr) => write!(f, "{} IS NULL", expr),
            PhysicalExpr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
        }
    }
}

fn format_scalar(val: &ScalarValue) -> String {
    match val {
        ScalarValue::Null => "NULL".to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(i) => i.to_string(),
        ScalarValue::Float64(v) => v.to_string(),
        ScalarValue::String(s) => format!("'{}'", s),
        ScalarValue::Timestamp(ts) => ts.to_string(),
    }
}

fn format_scalar_expr(expr: &PhysicalExpr) -> String {
    match expr {
        PhysicalExpr::Literal(val) => format_scalar(val),
        other => other.to_string(),
    }
}

fn format_binary_op(op: &BinaryOperator) -> Option<&'static str> {
    use BinaryOperator::*;
    match op {
        And => Some("AND"),
        Or => Some("OR"),
        Eq => Some("="),
        NotEq => Some("!="),
        Gt => Some(">"),
        GtEq => Some(">="),
        Lt => Some("<"),
        LtEq => Some("<="),
        Plus => Some("+"),
        Minus => Some("-"),
        Multiply => Some("*"),
        Divide => Some("/"),
        Modulo => Some("%"),
        _ => None,
    }
}

fn format_unary_op(op: &UnaryOperator) -> Option<&'static str> {
    use UnaryOperator::*;
    match op {
        Not => Some("NOT"),
        Plus => Some("+"),
        Minus => Some("-"),
        _ => None,
    }
}
