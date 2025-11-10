---
title: "SQL Executor"
layout: default
nav_order: 14
---

# SQL Executor

The SQL executor is a **full-featured query engine** supporting SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE operations. It implements ~10,000 lines of columnar batch processing with advanced features including:

- Full SELECT with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX, VARIANCE, STDDEV, PERCENTILE_CONT
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM OVER, etc.
- Expression evaluation: arithmetic, comparisons, pattern matching (LIKE/RLIKE)
- Scalar functions: ABS, ROUND, CEIL, FLOOR, EXP, LN, LOG, POWER
- Columnar batch processing for performance
- Type-specific storage (Int64, Float64, Text)
- Bitmap-based filtering

## Architecture

### High-Level Flow

```
SQL Text
    │
    ├─▶ Parse (sqlparser crate)
    │      └─▶ AST (Statement)
    │
    ├─▶ Plan (identify columns, aggregates, windows)
    │      └─▶ Execution Plan
    │
    ├─▶ Load Data
    │      ├─▶ PageDirectory::locate_range()
    │      ├─▶ PageHandler::get_pages()
    │      └─▶ Convert to ColumnarBatch
    │
    ├─▶ Execute Query
    │      ├─▶ Apply WHERE → selection bitmap
    │      ├─▶ Apply GROUP BY → hash aggregation
    │      ├─▶ Apply window functions → partitioned computation
    │      ├─▶ Apply ORDER BY → multi-column sort
    │      └─▶ Apply LIMIT/OFFSET → slice results
    │
    └─▶ Return Results (Vec<Vec<String>>)
```

### Components

```
┌──────────────────────────────────────────────┐
│           SQL Executor Modules               │
├──────────────────────────────────────────────┤
│ mod.rs              - Main execution loop    │
│ batch.rs            - Columnar processing    │
│ expressions.rs      - WHERE/projection eval  │
│ aggregates.rs       - GROUP BY/aggregates    │
│ window_helpers.rs   - Window functions       │
│ ordering.rs         - ORDER BY sorting       │
│ grouping_helpers.rs - Multi-column grouping  │
│ values.rs           - Type system            │
│ row_functions.rs    - Scalar functions       │
└──────────────────────────────────────────────┘
           │
           ├─▶ PageDirectory (metadata lookup)
           ├─▶ PageHandler (data retrieval)
           └─▶ Writer (DML operations)
```

## DML Execution (INSERT/UPDATE/DELETE)

### CREATE TABLE
1. Parse statement and validate using `plan_create_table_statement`
2. Invoke `ops_handler::create_table_from_plan`
3. No pages are allocated yet; they are created lazily on the first insert

### INSERT (ORDER BY tables)
1. Parse the `INSERT` AST, ensuring the source is a `VALUES` clause
2. Resolve table metadata to get canonical column order and the sort key ordinals
3. For each VALUES row:
   - Materialize a `Vec<String>` sized to the full column count
   - Populate the specified columns, leaving the rest as empty strings
   - Submit to Writer as BufferRow operations
   - Writer batches rows until ROWS_PER_PAGE_GROUP threshold
   - Flush writes complete page group atomically across all columns

### UPDATE
1. Require a `WHERE` clause for selective rewrites (full-table updates still fall back to a scan)
2. Attempt to extract equality predicates over the leading ORDER BY columns; any prefix match
   enables a binary search over that subset, while the remaining predicate (ranges, `IN`, `OR`, etc.)
   is evaluated after materialising only the matching slice
3. If no ORDER BY prefix is available, fall back to the vectorised full-table scan
4. Apply assignments:
   - If ORDER BY columns remain unchanged, call `overwrite_row`
   - If an ORDER BY column changes, `delete_row` + `insert_sorted_row`
5. Submit UpdateJob to Writer for atomic persistence

### DELETE
1. Same predicate handling as UPDATE (ORDER BY prefix lookup when possible, otherwise vectorised scan)
2. Locate the target row(s) via the ORDER BY tuple search or scan fallback
3. Remove rows from all columns using `delete_row`, which updates metadata
4. Submit UpdateJob to Writer

## SELECT Execution (`src/sql/executor/mod.rs`)

The SQL executor provides full SELECT support with aggregates, expressions, WHERE filtering, ORDER BY, LIMIT/OFFSET, and GROUP BY.
Predicates that constrain a leading prefix of the table's ORDER BY columns reuse the same binary
search fast path as UPDATE/DELETE, falling back to a vectorised scan only when no sortable prefix
can be extracted.

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

---

## Window Functions (`src/sql/executor/window_helpers.rs`)

Window functions perform calculations across sets of rows related to the current row, without collapsing results like GROUP BY.

### Supported Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ROW_NUMBER()` | Sequential number within partition | `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)` |
| `RANK()` | Rank with gaps for ties | `RANK() OVER (ORDER BY score DESC)` |
| `DENSE_RANK()` | Rank without gaps | `DENSE_RANK() OVER (ORDER BY score DESC)` |
| `LAG(expr, offset)` | Access previous row | `LAG(price, 1) OVER (ORDER BY date)` |
| `LEAD(expr, offset)` | Access next row | `LEAD(price, 1) OVER (ORDER BY date)` |
| `FIRST_VALUE(expr)` | First value in window | `FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC)` |
| `LAST_VALUE(expr)` | Last value in window | `LAST_VALUE(name) OVER (...)` |
| `NTH_VALUE(expr, n)` | Nth value in window | `NTH_VALUE(price, 3) OVER (...)` |
| `SUM(expr) OVER` | Running sum | `SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| `AVG(expr) OVER` | Moving average | `AVG(price) OVER (ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)` |
| `COUNT(expr) OVER` | Running count | `COUNT(*) OVER (PARTITION BY category)` |
| `MIN(expr) OVER` | Minimum in window | `MIN(price) OVER (...)` |
| `MAX(expr) OVER` | Maximum in window | `MAX(price) OVER (...)` |

### Window Frames

Supported frame specifications:
- `ROWS BETWEEN <start> AND <end>`
- `RANGE BETWEEN <start> AND <end>`

**Frame bounds:**
- `UNBOUNDED PRECEDING` - Start of partition
- `n PRECEDING` - n rows/range units before current row
- `CURRENT ROW` - Current row
- `n FOLLOWING` - n rows/range units after current row
- `UNBOUNDED FOLLOWING` - End of partition

**Example:**
```sql
SELECT
  date,
  price,
  AVG(price) OVER (
    ORDER BY date
    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7day
FROM stock_prices
```

### Execution Model

**Window Function Processing Pipeline:**

```
ColumnarBatch (input rows)
    │
    ├─▶ Step 1: Identify window function expressions
    │     └─▶ Extract PARTITION BY, ORDER BY, frame specs
    │
    ├─▶ Step 2: Partition rows
    │     ├─▶ Evaluate partition keys on batch
    │     ├─▶ Group rows by partition key: HashMap<PartitionKey, Vec<row_idx>>
    │     └─▶ For queries with no PARTITION BY, all rows in one partition
    │
    ├─▶ Step 3: Sort within partitions
    │     └─▶ For each partition, sort by ORDER BY clauses
    │
    ├─▶ Step 4: Compute window function
    │     ├─▶ For ROW_NUMBER/RANK/DENSE_RANK: assign ranks
    │     ├─▶ For LAG/LEAD: access offset rows
    │     ├─▶ For aggregates: accumulate over frame
    │     └─▶ Store result for each row
    │
    ├─▶ Step 5: Materialize output column
    │     └─▶ Create ColumnarPage with window function results
    │
    └─▶ Add column to batch with generated alias
```

**Example: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)**

```
Input batch (4 rows):
  dept    salary
  ----    ------
  eng     100000
  sales   80000
  eng     95000
  sales   85000

Step 1: Partition by dept
  Partition "eng": [0, 2]    (rows 0 and 2)
  Partition "sales": [1, 3]  (rows 1 and 3)

Step 2: Sort within partitions by salary DESC
  Partition "eng": [0, 2]  (already sorted: 100000 > 95000)
  Partition "sales": [3, 1] (reorder: 85000 > 80000)

Step 3: Assign ROW_NUMBER
  Row 0 (eng, 100000): 1
  Row 2 (eng, 95000): 2
  Row 3 (sales, 85000): 1
  Row 1 (sales, 80000): 2

Output column: [1, 2, 1, 2]  (indexed by original row position)
```

### Window Frame Evaluation

**Sliding Window for Aggregates:**

```sql
SUM(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```

**Execution:**
```
Sorted rows (by date):
  date       amount
  2024-01-01   10
  2024-01-02   20
  2024-01-03   30
  2024-01-04   40

Window computation:
  Row 0: SUM([10])           = 10   (only current row, no preceding)
  Row 1: SUM([10, 20])       = 30   (1 preceding + current)
  Row 2: SUM([10, 20, 30])   = 60   (2 preceding + current)
  Row 3: SUM([20, 30, 40])   = 90   (2 preceding + current, window slides)
```

**Implementation** (`WindowOperator::evaluate_frame`):
```rust
for current_row_idx in partition_rows {
    let frame_start = max(0, current_row_idx - preceding_offset);
    let frame_end = min(partition_rows.len(), current_row_idx + following_offset + 1);

    let frame_rows = &partition_rows[frame_start..frame_end];

    // Accumulate aggregate over frame
    let result = match window_fn {
        Sum => frame_rows.iter().map(|&i| values[i]).sum(),
        Avg => frame_rows.iter().map(|&i| values[i]).sum() / frame_rows.len(),
        Count => frame_rows.len(),
        Min => frame_rows.iter().map(|&i| values[i]).min(),
        Max => frame_rows.iter().map(|&i| values[i]).max(),
    };

    output[current_row_idx] = result;
}
```

### Multiple Window Functions

When a query contains multiple window functions, they are evaluated sequentially:

```sql
SELECT
  name,
  salary,
  ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank,
  LAG(salary, 1) OVER (ORDER BY salary DESC) AS prev_salary,
  AVG(salary) OVER () AS company_avg
FROM employees
```

**Execution:**
1. Evaluate `ROW_NUMBER() OVER (ORDER BY salary DESC)` → add column 'rank'
2. Evaluate `LAG(salary, 1) OVER (ORDER BY salary DESC)` → add column 'prev_salary'
3. Evaluate `AVG(salary) OVER ()` → add column 'company_avg'
4. Project final columns: name, salary, rank, prev_salary, company_avg

---

## Module Structure

```
src/sql/executor/
├── mod.rs                 - Main SELECT execution (3,445 lines)
├── batch.rs               - Columnar batch processing (655 lines)
├── expressions.rs         - Expression evaluation (1,631 lines)
├── aggregates.rs          - Aggregate functions (1,089 lines)
├── window_helpers.rs      - Window functions (1,375 lines)
├── ordering.rs            - Sorting with NULLS (424 lines)
├── grouping_helpers.rs    - GROUP BY/HAVING (170 lines)
├── helpers.rs             - Utilities (460 lines)
├── row_functions.rs       - Scalar functions (408 lines)
├── scalar_functions.rs    - Additional scalars (167 lines)
├── projection_helpers.rs  - Result materialization (223 lines)
├── scan_helpers.rs        - Scan optimization (110 lines)
├── values.rs              - Type system (174 lines)
└── spill.rs               - Spill-to-disk (24 lines)

Total: ~10,355 lines
```
