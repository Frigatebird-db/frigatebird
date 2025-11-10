---
title: "Query Planner"
layout: default
nav_order: 9
---

# Query Planner

The query planner analyzes parsed SQL statements and produces logical query plans describing table accesses, column operations, and filter predicates. It transforms the Abstract Syntax Tree into a structured representation optimized for execution planning.

## Purpose

The planner bridges the gap between raw SQL (via the parser) and execution engines. It:
- Identifies which tables are accessed and how (read/write)
- Tracks which columns are involved in operations
- Normalizes filter predicates into a canonical tree structure
- Validates that queries are supported by the current implementation

## Architecture

```
╔══════════════════════════════════════════════════════════════════════╗
║                           Parser Output                              ║
║                       Vec<Statement> (AST)                           ║
╚══════════════════════════════════╤═══════════════════════════════════╝
                                   │ Statement
                                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║                        Query Planner                                 ║
║                      (planner.rs)                                    ║
║                                                                      ║
║  ┌─────────────────────────────────────────────────────────┐        ║
║  │ plan_statement(stmt: &Statement)                        │        ║
║  │  • Dispatches to statement-specific handlers            │        ║
║  └──────────────┬──────────────────────────────────────────┘        ║
║                 │                                                    ║
║    ┌────────────┴────────────────────────────────┐                  ║
║    ▼            ▼           ▼            ▼       ▼                  ║
║  SELECT      INSERT      UPDATE      DELETE    Other                ║
║    │            │           │            │        │                  ║
║    ▼            ▼           ▼            ▼        ▼                  ║
║  plan_query  plan_insert plan_update plan_delete Error              ║
║    │            │           │            │                           ║
║    └────────────┴───────────┴────────────┘                           ║
║                 │                                                    ║
║                 ▼                                                    ║
║  ┌─────────────────────────────────────────────────────────┐        ║
║  │ Table Analysis                                          │        ║
║  │  • Extract table names                                  │        ║
║  │  • Collect read columns (SELECT, WHERE, etc.)           │        ║
║  │  • Collect write columns (INSERT, UPDATE)               │        ║
║  │  • Build filter expression trees                        │        ║
║  └─────────────────────────────────────────────────────────┘        ║
║                 │                                                    ║
║                 ▼                                                    ║
║  ┌─────────────────────────────────────────────────────────┐        ║
║  │ Merge Tables                                            │        ║
║  │  • Consolidate multiple accesses to same table          │        ║
║  │  • Combine read/write columns                           │        ║
║  │  • Merge filter predicates with AND                     │        ║
║  └─────────────────────────────────────────────────────────┘        ║
╚══════════════════════════════════╤═══════════════════════════════════╝
                                   │ QueryPlan
                                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║                           Query Plan                                 ║
║                                                                      ║
║  QueryPlan {                                                         ║
║    tables: Vec<TableAccess>                                          ║
║  }                                                                   ║
║                                                                      ║
║  TableAccess {                                                       ║
║    table_name: String,                                               ║
║    read_columns: BTreeSet<String>,    // Columns we read from       ║
║    write_columns: BTreeSet<String>,   // Columns we write to        ║
║    filters: Option<FilterExpr>        // Predicates to apply        ║
║  }                                                                   ║
║                                                                      ║
║  FilterExpr::And([                    // Filter tree structure      ║
║    FilterExpr::Leaf(age > 18),                                       ║
║    FilterExpr::Or([                                                  ║
║      FilterExpr::Leaf(region = 'US'),                                ║
║      FilterExpr::Leaf(vip = true)                                    ║
║    ])                                                                ║
║  ])                                                                  ║
╚══════════════════════════════════════════════════════════════════════╝
```

## Core Data Structures

### QueryPlan

The top-level plan containing all table accesses:

```rust
pub struct QueryPlan {
    pub tables: Vec<TableAccess>,
}
```

Multiple `TableAccess` entries occur when:
- `INSERT ... SELECT` accesses both target and source tables
- Future: Joins access multiple tables

### TableAccess

Describes how a single table is used in the query:

```rust
pub struct TableAccess {
    pub table_name: String,
    pub read_columns: BTreeSet<String>,
    pub write_columns: BTreeSet<String>,
    pub filters: Option<FilterExpr>,
}
```

**Column tracking:**
- `"*"` in the set means "all columns" or "unknown columns"
- Empty set means no columns accessed (e.g., `DELETE FROM table` with no WHERE)
- Specific names track exact columns

### FilterExpr

Represents filter predicates in a normalized tree:

```rust
pub enum FilterExpr {
    Leaf(Expr),              // Base predicate (e.g., age > 18)
    And(Vec<FilterExpr>),    // Conjunction of filters
    Or(Vec<FilterExpr>),     // Disjunction of filters
}
```

**Design benefits:**
- Preserves original SQL expressions in leaves
- Normalized structure simplifies analysis
- Easy to traverse for optimization
- Can reconstruct SQL or convert to execution steps

## Planning Process

### 1. SELECT Queries

```sql
SELECT id, name
FROM users
WHERE age > 18 AND status = 'active'
ORDER BY name
```

**Planning steps:**
1. Extract table name: `users`
2. Collect projection columns: `id`, `name`
3. Collect WHERE columns: `age`, `status`
4. Collect ORDER BY columns: `name`
5. Combine into `read_columns`: `{age, id, name, status}`
6. Build filter tree:
   ```
   And([
     Leaf(age > 18),
     Leaf(status = 'active')
   ])
   ```

**Result:**
```rust
TableAccess {
    table_name: "users",
    read_columns: {"age", "id", "name", "status"},
    write_columns: {},
    filters: Some(And([...]))
}
```

### 2. INSERT Queries

```sql
INSERT INTO users (id, name)
VALUES (1, 'John')
```

**Planning steps:**
1. Extract table name: `users`
2. Collect target columns: `id`, `name`
3. Add to `write_columns`
4. No filters (INSERTs don't filter)

**With SELECT source:**
```sql
INSERT INTO archive (id)
SELECT id FROM users WHERE published = true
```

Produces **two** TableAccess entries:
- `archive`: write to `id`
- `users`: read `id`, `published`, with filter

### 3. UPDATE Queries

```sql
UPDATE accounts
SET balance = balance + 10
WHERE id = 42
```

**Planning steps:**
1. Extract table name: `accounts`
2. Collect SET columns: `balance` → `write_columns`
3. Collect expression columns: `balance` → `read_columns` (right side)
4. Collect WHERE columns: `id` → `read_columns`
5. Build filter from WHERE clause

**Result:**
```rust
TableAccess {
    table_name: "accounts",
    read_columns: {"balance", "id"},
    write_columns: {"balance"},
    filters: Some(Leaf(id = 42))
}
```

### 4. DELETE Queries

```sql
DELETE FROM sessions
WHERE expires_at < NOW()
```

**Planning steps:**
1. Extract table name: `sessions`
2. No write columns tracked (row deletion)
3. Collect WHERE columns: `expires_at` → `read_columns`
4. Build filter from WHERE clause

## Filter Building

The planner normalizes WHERE/HAVING/QUALIFY clauses into `FilterExpr` trees:

```rust
fn build_filter(expr: &Expr) -> FilterExpr {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => FilterExpr::and(
                build_filter(left),
                build_filter(right)
            ),
            BinaryOperator::Or => FilterExpr::or(
                build_filter(left),
                build_filter(right)
            ),
            _ => FilterExpr::leaf(expr.clone()),
        },
        Expr::Nested(inner) => build_filter(inner),
        _ => FilterExpr::leaf(expr.clone()),
    }
}
```

**Normalization rules:**
- AND operations create `FilterExpr::And` nodes
- OR operations create `FilterExpr::Or` nodes
- All other expressions become `FilterExpr::Leaf` nodes
- Nested expressions are unwrapped

**Example:**
```sql
WHERE (a > 5 AND b < 10) OR (c = 'X')
```

Becomes:
```
Or([
  And([
    Leaf(a > 5),
    Leaf(b < 10)
  ]),
  Leaf(c = 'X')
])
```

## Column Collection

The planner tracks all columns involved in expressions:

### Comprehensive Expression Handling

```rust
fn collect_expr_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Identifier(ident) => columns.insert(ident.value),
        CompoundIdentifier(idents) => /* last identifier */,
        BinaryOp { left, right, .. } => {
            collect_expr_columns(left, columns);
            collect_expr_columns(right, columns);
        }
        // ... 50+ expression types handled
    }
}
```

Handles:
- Simple identifiers: `id`, `name`
- Qualified names: `users.id`, `schema.table.column`
- Expressions: `balance + 10`, `UPPER(name)`
- Subqueries: marked as `*` (reads everything)
- Functions: `COUNT(*)`, `SUM(amount)`, etc.
- CASE/WHEN: all branches
- Aggregates with FILTER/OVER clauses

## Table Merging

When a statement accesses the same table multiple times, entries are merged:

```rust
fn merge_tables(tables: Vec<TableAccess>) -> QueryPlan {
    let mut merged: BTreeMap<String, TableAccess> = BTreeMap::new();
    for table in tables {
        merged
            .entry(table.table_name.clone())
            .and_modify(|existing| existing.merge_from(&table))
            .or_insert(table);
    }
    QueryPlan::new(merged.into_values().collect())
}
```

**Merge rules:**
- `read_columns` are unioned
- `write_columns` are unioned
- `filters` are combined with AND

## Supported SQL Features

| Feature | Support | Notes |
|---------|---------|-------|
| SELECT | ✅ Full | Single table only |
| INSERT | ✅ Full | With VALUES or SELECT source |
| UPDATE | ✅ Full | Single table, no FROM clause |
| DELETE | ✅ Full | Single table |
| WHERE | ✅ Full | All predicates |
| HAVING | ✅ Full | Post-aggregation filters |
| QUALIFY | ✅ Full | Window function filters |
| ORDER BY | ✅ Full | Columns tracked |
| LIMIT/OFFSET | ✅ Full | Expressions tracked |
| GROUP BY | ✅ Full | Columns tracked |
| JOINs | ❌ Not yet | Returns `Unsupported` error |
| Subqueries | ⚠️ Partial | Tracked as `*` reads |
| CTEs | ❌ Not yet | Future enhancement |
| Window functions | ✅ Full | Columns tracked |

## Error Handling

The planner returns `PlannerError` for unsupported or invalid SQL:

```rust
pub enum PlannerError {
    EmptyStatement,                // No SQL provided
    MissingTable(String),          // Can't determine table
    Unsupported(String),           // Feature not implemented
}
```

**Common error scenarios:**

```rust
// Empty input
plan_sql("")  // → PlannerError::EmptyStatement

// Multiple statements
plan_sql("SELECT * FROM a; SELECT * FROM b;")  // → Unsupported

// JOINs not supported yet
plan_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
// → Unsupported("JOINs are not supported yet")

// CREATE/ALTER/DROP
plan_sql("CREATE TABLE users (id INT)")
// → Unsupported("only SELECT, INSERT, UPDATE, and DELETE...")
```

## Performance

- Single AST traversal
- BTreeSet for columns: O(log n) inserts
- Filter tree construction: O(n)

## Testing

Comprehensive tests in `tests/sql_planner_tests.rs`:

```rust
#[test]
fn plans_basic_select_reads() {
    let plan = plan_sql("SELECT id, name FROM users WHERE status = 'active'").unwrap();
    let users = table_by_name(&plan, "users");

    assert!(users.write_columns.is_empty());
    assert!(users.read_columns.contains("id"));
    assert!(users.read_columns.contains("name"));
    assert!(users.read_columns.contains("status"));
}

#[test]
fn captures_and_or_filters() {
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND (region = 'US' OR vip = true)",
    ).unwrap();

    let filter = accounts.filters.as_ref().unwrap();
    // Assert tree structure matches expected AND/OR nesting
}
```

## Integration Points

The query planner sits between the parser and execution stages:

```
SQL Text
   ↓
[Parser] → Vec<Statement>
   ↓
[Planner] → QueryPlan
   ↓
[Pipeline Builder] → Job
   ↓
[Executor] → Results
```

**Downstream consumers:**
- **Pipeline Builder**: Converts plans into execution jobs
- **Pipeline Executor**: Schedules jobs across main/reserve worker pools
- **Validation**: Checks permissions, schema compatibility
- **Optimization**: Future cost-based query optimization
- **Observability**: Query logging and tracing

## Module Location

- **Source**: `src/sql/planner.rs`
- **Models**: `src/sql/models.rs`
- **Module**: `src/sql/mod.rs`
- **Tests**: `tests/sql_planner_tests.rs`

## Related Documentation

- [SQL Parser](sql_parser) - Produces AST consumed by planner
- [Pipeline Builder](pipeline) - Consumes query plans
- [Architecture Overview](architecture_overview) - System context
