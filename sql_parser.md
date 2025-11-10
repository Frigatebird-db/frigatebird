---
title: "SQL Parser"
layout: default
nav_order: 8
---

# SQL Parser

The SQL parser provides a thin wrapper around the `sqlparser` crate to convert raw SQL text into structured Abstract Syntax Trees (AST). This is the first stage in the query processing pipeline.

## Purpose

The parser transforms SQL strings into machine-readable AST nodes that can be analyzed by the query planner. It handles syntax validation and produces a standardized representation of SQL statements regardless of minor dialect variations.

## Architecture

```
╔══════════════════════════════════════════════════════════════╗
║                         Client Code                          ║
║  parse_sql("SELECT id FROM users WHERE age > 18")            ║
╚══════════════════════════════╤═══════════════════════════════╝
                               │ &str (SQL text)
                               ▼
╔══════════════════════════════════════════════════════════════╗
║                      Parser Module                           ║
║                     (parser.rs)                              ║
║                                                              ║
║  ┌────────────────────────────────────────────────────┐     ║
║  │ parse_sql(sql: &str)                               │     ║
║  │  • Uses GenericDialect                             │     ║
║  │  • Delegates to sqlparser::Parser                  │     ║
║  │  • Returns Vec<Statement>                          │     ║
║  └────────────────────────────────────────────────────┘     ║
║                               │                              ║
║                               ▼                              ║
║  ┌────────────────────────────────────────────────────┐     ║
║  │        sqlparser Crate                             │     ║
║  │  • Lexical analysis (tokenization)                 │     ║
║  │  • Syntax analysis (parsing)                       │     ║
║  │  • AST construction                                │     ║
║  └────────────────────────────────────────────────────┘     ║
╚══════════════════════════════╤═══════════════════════════════╝
                               │ Vec<Statement>
                               ▼
╔══════════════════════════════════════════════════════════════╗
║                  Abstract Syntax Tree (AST)                  ║
║                                                              ║
║  Statement::Query(Query {                                    ║
║    body: Select {                                            ║
║      projection: [Identifier("id")],                         ║
║      from: [TableWithJoins {                                 ║
║        relation: Table { name: "users" }                     ║
║      }],                                                     ║
║      selection: BinaryOp {                                   ║
║        left: Identifier("age"),                              ║
║        op: Gt,                                               ║
║        right: Value(Number("18"))                            ║
║      }                                                       ║
║    }                                                         ║
║  })                                                          ║
╚══════════════════════════════════════════════════════════════╝
```

## Implementation

The parser is intentionally minimal to keep dependencies clean and parsing fast:

```rust
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
}
```

## Usage

### Basic Parsing

```rust
use idk_uwu_ig::sql::parse_sql;

let sql = "SELECT id, name FROM users WHERE active = true";
let statements = parse_sql(sql)?;

// Returns: Vec<Statement>
assert_eq!(statements.len(), 1);
```

### Multi-Statement Parsing

```rust
let sql = "SELECT * FROM users; DELETE FROM sessions;";
let statements = parse_sql(sql)?;

assert_eq!(statements.len(), 2);
// statements[0] is SELECT
// statements[1] is DELETE
```

### Error Handling

```rust
let sql = "SELECT id FROM"; // Invalid SQL
match parse_sql(sql) {
    Ok(_) => println!("Parsed successfully"),
    Err(e) => println!("Parse error: {}", e),
}
```

## Statement Types

The parser returns `sqlparser::ast::Statement` variants including:

| Statement Type | Example | Description |
|----------------|---------|-------------|
| `Query` | `SELECT id FROM users` | Data retrieval queries |
| `Insert` | `INSERT INTO users (id) VALUES (1)` | Row insertion |
| `Update` | `UPDATE users SET name = 'John'` | Row modification |
| `Delete` | `DELETE FROM users WHERE id = 1` | Row deletion |
| `CreateTable` | `CREATE TABLE users (id INT)` | Schema creation |
| `AlterTable` | `ALTER TABLE users ADD COLUMN age INT` | Schema modification |
| `Drop` | `DROP TABLE users` | Object deletion |

**Note:** The query planner currently only supports `Query`, `Insert`, `Update`, and `Delete`. Other statement types will be rejected during planning.

## AST Structure

The Abstract Syntax Tree represents SQL hierarchically:

```
Statement::Query
    ├── body: SetExpr
    │   └── Select
    │       ├── projection: Vec<SelectItem>
    │       │   ├── UnnamedExpr(column)
    │       │   ├── ExprWithAlias { expr, alias }
    │       │   └── Wildcard(*)
    │       ├── from: Vec<TableWithJoins>
    │       │   └── relation: TableFactor
    │       │       └── Table { name, alias }
    │       ├── selection: Option<Expr>  // WHERE clause
    │       ├── group_by: Vec<Expr>
    │       ├── having: Option<Expr>
    │       └── ...
    ├── order_by: Vec<OrderByExpr>
    ├── limit: Option<Expr>
    └── offset: Option<Offset>
```

## Performance

- Single-pass parsing
- Zero-copy where possible

## Error Handling

Parse errors include detailed information:

```rust
ParserError {
    message: "Expected identifier, found: FROM",
    location: Location { line: 1, column: 12 }
}
```

Common error scenarios:
- **Syntax errors**: Malformed SQL (missing keywords, unmatched parens)
- **Unexpected tokens**: Invalid token sequences
- **Incomplete statements**: Truncated SQL

## Integration with Query Planner

The parser output feeds directly into the query planner:

```
SQL Text
   ↓
parse_sql()
   ↓
Vec<Statement>
   ↓
plan_statement()  ← Query Planner
   ↓
QueryPlan
```

The planner walks the AST to extract:
- Table references
- Column accesses (reads/writes)
- Filter predicates (WHERE, HAVING, etc.)
- Join conditions (future)

## Testing

The parser is tested indirectly through query planner tests:

```rust
#[test]
fn parse_and_plan() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18").unwrap();
    assert_eq!(plan.tables[0].table_name, "users");
}
```

Direct parser tests would look like:

```rust
#[test]
fn parses_select() {
    let stmts = parse_sql("SELECT * FROM users").unwrap();
    assert_eq!(stmts.len(), 1);
    assert!(matches!(stmts[0], Statement::Query(_)));
}
```

## Module Location

- **Source**: `src/sql/parser.rs`
- **Module**: `src/sql/mod.rs`
- **Dependencies**: `sqlparser` crate

## Related Documentation

- [Query Planner](query_planner) - Consumes parser output
- [Pipeline Builder](pipeline) - Uses query plans for execution
- [Architecture Overview](architecture_overview) - System-wide context
