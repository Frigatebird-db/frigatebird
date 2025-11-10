---
title: "SQL Executor"
layout: default
nav_order: 14
---

# SQL Executor

`SqlExecutor` is the thinnest bridge between SQL text and the existing
page/metadata primitives. It currently supports:

- `CREATE TABLE ... ORDER BY ...`
- `INSERT INTO ... VALUES ...` for tables that declare an `ORDER BY`
- `UPDATE ... SET ... WHERE ...` (equality predicates on all ORDER BY columns)
- `DELETE FROM ... WHERE ...` (equality predicates on all ORDER BY columns)

It intentionally avoids building a full planner/executor stack. Instead it
parses the statement once, then delegates to the `ops_handler` helpers that
already know how to rewrite column pages.

## Components

```
┌────────────────────┐
│  SqlExecutor       │
│  (this module)     │
└─────────┬──────────┘
          │ parse SQL (sqlparser)
          ▼
┌───────────────────────┐
│ schema helpers        │◄── plan_create_table_statement
└─────────┬─────────────┘
          │                    ┌─────────────────────┐
          ├─ CREATE TABLE ───► │ metadata::PageDirectory
          │                    └─────────────────────┘
          │
          ▼
┌───────────────────────┐
│ mutating helpers      │◄── insert_sorted_row / overwrite_row / delete_row
└─────────┬─────────────┘
          │
          ▼
┌──────────────────────────────┐
│ page_handler (locator+caches)│
└──────────────────────────────┘
```

## Execution Flow

### CREATE TABLE
1. Parse statement and validate using `plan_create_table_statement`
2. Invoke `ops_handler::create_table_from_plan`
3. No pages are allocated yet; they are created lazily on the first insert

### INSERT (ORDER BY tables)
1. Parse the `INSERT` AST, ensuring the source is a `VALUES` clause
2. Resolve table metadata to get canonical column order and the sort key ordinals
3. For each VALUES row:
   - Materialise a `Vec<String>` sized to the full column count
   - Populate the specified columns, leaving the rest as empty strings
   - If the table has no pages yet, write the row into freshly allocated pages
   - Otherwise call `insert_sorted_row`, which finds the sorted position and
     splices the entry into every column while keeping metadata entry counts in
     sync

### UPDATE
1. Require a `WHERE` clause composed of `column = literal` predicates
2. Validate that every ORDER BY column has an equality predicate
3. Binary-search the leading ORDER BY column to locate candidate rows, then
   confirm the full tuple with `read_row`
4. Apply assignments:
   - If ORDER BY columns remain unchanged, call `overwrite_row`
   - If an ORDER BY column changes, `delete_row` the old tuple and reinsert via
     `insert_sorted_row` so the row lands in the correct position

### DELETE
1. Same predicate restrictions as UPDATE (equality on ORDER BY columns)
2. Locate the target row(s) via the ORDER BY tuple search
3. Remove rows from all columns using `delete_row`, which also updates
   metadata prefix counts

Unsupported constructs (subqueries, RETURNING, INSERT ... SELECT, tables without
an `ORDER BY` clause) immediately return `SqlExecutionError::Unsupported`
so the caller knows the mutation was not applied.

## SELECT Execution (`src/sql/executor/mod.rs`)

The SQL executor provides full SELECT support with aggregates, expressions, WHERE filtering, ORDER BY, LIMIT/OFFSET, and GROUP BY.

### Query Processing Pipeline

```
SQL SELECT statement
      │
      ├─▶ Parse table name and validate metadata
      │
      ├─▶ Determine if aggregates present
      │     └─▶ Plan aggregate projection
      │
      ├─▶ Identify required columns from:
      │     • SELECT projections
      │     • WHERE filters
      │     • ORDER BY columns
      │     • GROUP BY columns
      │
      ├─▶ Locate row range via metadata
      │
      ├─▶ Materialize required columns into memory
      │
      ├─▶ Apply WHERE filters (row-by-row evaluation)
      │
      ├─▶ If aggregates:
      │     └─▶ Evaluate aggregate functions → single result row
      │   Else:
      │     └─▶ Project selected columns → multiple rows
      │
      ├─▶ Apply ORDER BY sorting
      │
      └─▶ Apply LIMIT/OFFSET
```

---

## Aggregate Functions (`src/sql/executor/aggregates.rs`)

The executor supports SQL aggregate functions with GROUP BY (implicitly grouping all rows into one group when no GROUP BY clause is present).

### Supported Aggregates

| Function | Description | Implementation |
|----------|-------------|----------------|
| `COUNT(*)` | Counts all rows | Returns row count |
| `COUNT(column)` | Counts non-null values | Filters out null sentinel |
| `SUM(column)` | Sums numeric values | Parses as f64, accumulates |
| `AVG(column)` | Average of numeric values | Sum / count of non-null values |
| `MIN(column)` | Minimum value | Numeric or lexicographic comparison |
| `MAX(column)` | Maximum value | Numeric or lexicographic comparison |
| `VARIANCE(column)` | Population variance | σ² = Σ(x - μ)² / n |
| `STDDEV(column)` | Population standard deviation | σ = √variance |
| `PERCENTILE_CONT(fraction)` | Continuous percentile | Linear interpolation between values |

### Aggregate Evaluation Flow

```
AggregateProjectionPlan
      │
      ├─▶ required_ordinals: columns needed for aggregates
      ├─▶ outputs: Vec<AggregateProjection> (one per SELECT item)
      └─▶ headers: column names for result
      │
      ▼
Materialize required columns
      │
      ▼
For each output in outputs:
      │
      ├─▶ evaluate_aggregate_function(expr, dataset)
      │     │
      │     ├─▶ Extract function name and arguments
      │     │
      │     ├─▶ For each row in dataset:
      │     │     └─▶ Evaluate argument expression
      │     │
      │     ├─▶ Apply aggregate logic:
      │     │     • COUNT: count non-null values
      │     │     • SUM: sum parsed f64 values
      │     │     • AVG: sum / count
      │     │     • MIN/MAX: compare all values
      │     │     • VARIANCE: compute σ²
      │     │     • STDDEV: √variance
      │     │     • PERCENTILE_CONT: sort and interpolate
      │     │
      │     └─▶ Return ScalarValue result
      │
      └─▶ Collect all outputs into single result row
```

### Example: AVG Computation

```sql
SELECT AVG(price) FROM products WHERE category = 'electronics'
```

**Execution:**
1. Filter rows where `category = 'electronics'` → rows [5, 12, 18, 27]
2. Materialize `price` column for these rows → values ["99.99", "149.50", "200.00", "75.00"]
3. Parse as f64 → [99.99, 149.50, 200.00, 75.00]
4. Sum → 524.49
5. Count → 4
6. Result → 524.49 / 4 = 131.1225

---

## Expression Evaluation (`src/sql/executor/expressions.rs`)

### Types of Expression Evaluation

1. **Scalar expressions** - Constant folding, type coercion
2. **Row expressions** - Column references, function calls evaluated per-row
3. **Selection expressions** - WHERE clause predicates (boolean evaluation)

### Supported Operators

| Operator | Type | Example |
|----------|------|---------|
| `+`, `-`, `*`, `/` | Arithmetic | `price * quantity` |
| `=`, `!=`, `<`, `>`, `<=`, `>=` | Comparison | `age >= 18` |
| `AND`, `OR`, `NOT` | Logical | `status = 'active' AND age < 65` |
| `LIKE`, `ILIKE` | Pattern matching | `email LIKE '%@example.com'` |
| `RLIKE` | Regex matching | `name RLIKE '^[A-Z]'` |
| `BETWEEN` | Range | `price BETWEEN 10 AND 100` |
| `IN` | Membership | `status IN ('active', 'pending')` |
| `IS NULL`, `IS NOT NULL` | Null checks | `deleted_at IS NULL` |

### Expression Evaluation Flow

```
evaluate_selection_expr(expr, row_idx, materialized_columns)
      │
      ▼
Match expr type:
      │
      ├─▶ Identifier → lookup column value
      │
      ├─▶ Value (literal) → return literal
      │
      ├─▶ BinaryOp { left, op, right }
      │     ├─▶ Evaluate left
      │     ├─▶ Evaluate right
      │     └─▶ Apply operator:
      │           • AND: left && right (short-circuit)
      │           • OR: left || right (short-circuit)
      │           • =, !=, <, >, <=, >=: compare_operands()
      │
      ├─▶ LIKE/ILIKE → like_match(pattern, value, case_insensitive)
      │
      ├─▶ RLIKE → regex_match(pattern, value)
      │
      ├─▶ BETWEEN → value >= low && value <= high
      │
      ├─▶ IN → list.contains(value)
      │
      ├─▶ IS NULL → value == NULL_SENTINEL
      │
      └─▶ Function → evaluate_row_function()
```

### Comparison Logic

```rust
fn compare_operands(left, right, op) -> bool {
    // Numeric-aware comparison
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => {
            // Numeric comparison: 2 < 10 < 100
            match op {
                Eq => l == r,
                NotEq => l != r,
                Lt => l < r,
                Gt => l > r,
                LtEq => l <= r,
                GtEq => l >= r,
            }
        }
        _ => {
            // Lexicographic comparison: "10" < "2" !
            left.cmp(right) matches op
        }
    }
}
```

---

## Scalar Functions (`src/sql/executor/scalar_functions.rs` + `row_functions.rs`)

### Supported Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(x)` | Absolute value | `ABS(-5)` → 5 |
| `ROUND(x, digits)` | Round to digits | `ROUND(3.14159, 2)` → 3.14 |
| `CEIL(x)` | Ceiling | `CEIL(3.1)` → 4 |
| `FLOOR(x)` | Floor | `FLOOR(3.9)` → 3 |
| `EXP(x)` | e^x | `EXP(1)` → 2.71828 |
| `LN(x)` | Natural log | `LN(2.71828)` → 1 |
| `LOG(base, x)` | Logarithm | `LOG(10, 100)` → 2 |
| `POWER(x, y)` | x^y | `POWER(2, 3)` → 8 |
| `WIDTH_BUCKET(val, min, max, buckets)` | Histogram bucket | `WIDTH_BUCKET(5, 0, 10, 5)` → 3 |

### Function Evaluation

```
evaluate_row_function(function_name, args, row_idx, materialized)
      │
      ├─▶ Extract and parse arguments
      │
      ├─▶ Match function_name:
      │     │
      │     ├─▶ ABS: arg.abs()
      │     ├─▶ ROUND: round to n digits
      │     ├─▶ CEIL: arg.ceil()
      │     ├─▶ FLOOR: arg.floor()
      │     ├─▶ EXP: e.powf(arg)
      │     ├─▶ LN: arg.ln()
      │     ├─▶ LOG: arg2.log(arg1)
      │     ├─▶ POWER: arg1.powf(arg2)
      │     └─▶ WIDTH_BUCKET: compute histogram bucket
      │
      └─▶ Return ScalarValue::Float or ScalarValue::Int
```

---

## Value Types (`src/sql/executor/values.rs`)

### Value Representations

```rust
pub enum ScalarValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

pub enum CachedValue {
    Null,
    Text(String),
}
```

### Null Encoding

- **Null sentinel**: `"__NULL_1735938241__"` (constant)
- Stored in columns as special string value
- Detected via `is_encoded_null(value) -> bool`
- Converted to `ScalarValue::Null` during evaluation

### Type Coercion

```
compare_scalar_values(left: ScalarValue, right: ScalarValue)
      │
      ├─▶ If both numeric (Int or Float):
      │     └─▶ Convert to f64, compare numerically
      │
      ├─▶ If both Text:
      │     └─▶ Try numeric parse first, fallback to lexicographic
      │
      ├─▶ If both Bool:
      │     └─▶ Boolean comparison
      │
      └─▶ If any Null:
            └─▶ NULL comparison semantics (NULL != NULL)
```

---

## Error Handling

`SqlExecutionError` surfaces the smallest set of errors necessary for the
inline executor:

| Variant | Meaning |
| --- | --- |
| `Parse(ParserError)` | SQL did not parse |
| `Plan(PlannerError)` | Planner rejected the DDL/DML |
| `Unsupported(String)` | Construct outside the current scope |
| `TableNotFound(String)` | Metadata missing for the target table |
| `ColumnMismatch { .. }` | INSERT referenced an unknown column |
| `ValueMismatch(String)` | Column/value arity mismatch |
| `OperationFailed(String)` | Underlying page/metadata mutation failed |

The executor keeps allocations minimal (one `Vec<String>` per row) and reuses
existing caches, so the hot path remains the `InsertAt` splice in
`insert_sorted_row`.

---

## Module Structure

```
src/sql/executor/
├── mod.rs                 - Main SELECT execution logic
├── aggregates.rs          - Aggregate function evaluation
├── expressions.rs         - Expression evaluation (WHERE, projections)
├── helpers.rs             - Utility functions (name extraction, LIKE/RLIKE)
├── row_functions.rs       - Per-row scalar functions
├── scalar_functions.rs    - Scalar function implementations
└── values.rs              - Value types and null encoding
```
