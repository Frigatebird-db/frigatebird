use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {};
    let sql = std::env::args().nth(1).expect("sql");
    let ast = Parser::parse_sql(&dialect, &sql).expect("parse");
    for stmt in ast {
        println!("{stmt:#?}");
    }
}
