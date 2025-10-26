use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse SQL text into AST statements using the generic SQL dialect.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
}
