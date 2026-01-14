pub mod executor;
pub mod models;
pub mod parser;
pub mod physical_plan;
pub mod planner;
pub mod runtime;
pub mod types;
pub mod utils;

pub use models::{ColumnSpec, CreateTablePlan, FilterExpr, PlannerError, QueryPlan, TableAccess};
pub use parser::parse_sql;
pub use planner::{plan_create_table_statement, plan_statement};
pub use types::DataType;

use crate::metadata_store::PageDirectory;
use sqlparser::ast::Statement;

/// Groups parsing and planning errors under a single type for consumers.
#[derive(Debug)]
pub enum SqlPlannerError {
    Parse(sqlparser::parser::ParserError),
    Plan(PlannerError),
}

impl std::fmt::Display for SqlPlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlPlannerError::Parse(err) => write!(f, "failed to parse SQL: {err}"),
            SqlPlannerError::Plan(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SqlPlannerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SqlPlannerError::Parse(err) => Some(err),
            SqlPlannerError::Plan(err) => Some(err),
        }
    }
}

impl From<sqlparser::parser::ParserError> for SqlPlannerError {
    fn from(value: sqlparser::parser::ParserError) -> Self {
        SqlPlannerError::Parse(value)
    }
}

impl From<PlannerError> for SqlPlannerError {
    fn from(value: PlannerError) -> Self {
        SqlPlannerError::Plan(value)
    }
}

/// Parse and plan a single SQL statement.
pub fn plan_sql(sql: &str, directory: &PageDirectory) -> Result<QueryPlan, SqlPlannerError> {
    let mut statements = parse_sql(sql)?;
    if statements.is_empty() {
        return Err(PlannerError::EmptyStatement.into());
    }
    if statements.len() > 1 {
        return Err(PlannerError::Unsupported(
            "only single SQL statements are supported for now".into(),
        )
        .into());
    }
    let statement = statements.pop().expect("len checked above");
    plan_statement(&statement, directory).map_err(Into::into)
}

/// Parse and plan a CREATE TABLE statement.
pub fn plan_create_table_sql(sql: &str) -> Result<CreateTablePlan, SqlPlannerError> {
    let mut statements = parse_sql(sql)?;
    if statements.is_empty() {
        return Err(PlannerError::EmptyStatement.into());
    }
    if statements.len() > 1 {
        return Err(PlannerError::Unsupported(
            "only single SQL statements are supported for now".into(),
        )
        .into());
    }
    let statement = statements.pop().expect("len checked above");
    plan_create_table_statement(&statement).map_err(Into::into)
}

/// Convenience for callers that already parsed the SQL.
pub fn plan_statement_ref(
    statement: &Statement,
    directory: &PageDirectory,
) -> Result<QueryPlan, SqlPlannerError> {
    plan_statement(statement, directory).map_err(Into::into)
}
