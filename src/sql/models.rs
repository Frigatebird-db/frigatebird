use sqlparser::ast::Expr;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAccess {
    pub table_name: String,
    pub read_columns: BTreeSet<String>,
    pub write_columns: BTreeSet<String>,
    pub filters: Option<FilterExpr>,
}

impl TableAccess {
    pub fn new(table_name: impl Into<String>) -> Self {
        TableAccess {
            table_name: table_name.into(),
            read_columns: BTreeSet::new(),
            write_columns: BTreeSet::new(),
            filters: None,
        }
    }

    pub fn merge_from(&mut self, other: &TableAccess) {
        self.read_columns.extend(other.read_columns.iter().cloned());
        self.write_columns
            .extend(other.write_columns.iter().cloned());
        if let Some(filter) = &other.filters {
            self.add_filter(filter.clone());
        }
    }

    pub fn add_filter(&mut self, filter: FilterExpr) {
        self.filters = Some(match self.filters.take() {
            Some(existing) => FilterExpr::and(existing, filter),
            None => filter,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub tables: Vec<TableAccess>,
}

impl QueryPlan {
    pub fn new(tables: Vec<TableAccess>) -> Self {
        QueryPlan { tables }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    EmptyStatement,
    MissingTable(String),
    Unsupported(String),
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::EmptyStatement => write!(f, "expected a SQL statement to plan"),
            PlannerError::MissingTable(context) => {
                write!(f, "unable to determine target table: {context}")
            }
            PlannerError::Unsupported(reason) => write!(f, "unsupported SQL: {reason}"),
        }
    }
}

impl std::error::Error for PlannerError {}

pub type PlannerResult<T> = Result<T, PlannerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpr {
    Leaf(Expr),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}

impl FilterExpr {
    pub fn leaf(expr: Expr) -> Self {
        FilterExpr::Leaf(expr)
    }

    pub fn and(lhs: FilterExpr, rhs: FilterExpr) -> Self {
        match (lhs, rhs) {
            (FilterExpr::And(mut left_items), FilterExpr::And(mut right_items)) => {
                left_items.append(&mut right_items);
                FilterExpr::And(left_items)
            }
            (FilterExpr::And(mut items), other) => {
                items.push(other);
                FilterExpr::And(items)
            }
            (other, FilterExpr::And(mut items)) => {
                let mut combined = vec![other];
                combined.append(&mut items);
                FilterExpr::And(combined)
            }
            (lhs, rhs) => FilterExpr::And(vec![lhs, rhs]),
        }
    }

    pub fn or(lhs: FilterExpr, rhs: FilterExpr) -> Self {
        match (lhs, rhs) {
            (FilterExpr::Or(mut left_items), FilterExpr::Or(mut right_items)) => {
                left_items.append(&mut right_items);
                FilterExpr::Or(left_items)
            }
            (FilterExpr::Or(mut items), other) => {
                items.push(other);
                FilterExpr::Or(items)
            }
            (other, FilterExpr::Or(mut items)) => {
                let mut combined = vec![other];
                combined.append(&mut items);
                FilterExpr::Or(combined)
            }
            (lhs, rhs) => FilterExpr::Or(vec![lhs, rhs]),
        }
    }
}

impl fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::Leaf(expr) => write!(f, "{expr}"),
            FilterExpr::And(parts) => display_joined("AND", parts, f),
            FilterExpr::Or(parts) => display_joined("OR", parts, f),
        }
    }
}

fn display_joined(
    separator: &str,
    parts: &[FilterExpr],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let mut iter = parts.iter();
    if let Some(first) = iter.next() {
        write!(f, "({first}")?;
        for part in iter {
            write!(f, " {separator} {part}")?;
        }
        write!(f, ")")
    } else {
        write!(f, "()")
    }
}
