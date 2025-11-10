---
title: "Columnar Batch Processing"
layout: default
nav_order: 16
---

# Columnar Batch Processing

The SQL executor uses a **columnar batch processing** architecture for efficient query execution. This document describes the data structures and algorithms that power the batch processing system.

---

## Overview

Traditional row-oriented processing evaluates expressions one row at a time, incurring significant per-row overhead. Columnar batch processing amortizes this cost across thousands of rows by:

1. **Columnar storage**: Data organized by column, not row
2. **Type-specific arrays**: Avoiding string parsing on hot paths
3. **Vectorized operations**: Processing multiple values per instruction
4. **Bitmap filtering**: Compact representation of selection predicates

```
Row-Oriented (traditional):
┌────────────────────────────────┐
│ Row 0: { id: "1", age: "25" }  │
│ Row 1: { id: "2", age: "30" }  │
│ Row 2: { id: "3", age: "22" }  │
└────────────────────────────────┘
  → Evaluate WHERE age > 24 per row

Columnar (batch):
┌──────────────┬──────────────────┐
│ id   [Int64] │ age      [Int64] │
│ ├─ 1         │ ├─ 25            │
│ ├─ 2         │ ├─ 30            │
│ └─ 3         │ └─ 22            │
└──────────────┴──────────────────┘
  → Evaluate WHERE age > 24 in batch
  → Bitmap: [1, 1, 0]
  → Gather indices: [0, 1]
```

---

## Data Structures

### Bitmap (`src/sql/executor/batch.rs`)

Compact bit-packed representation for tracking nulls and filter results.

**Structure:**
```rust
pub struct Bitmap {
    bits: Vec<u64>,    // 64-bit words for bit storage
    len: usize,        // Logical length in bits
}
```

**Storage:**
- Each `u64` word stores 64 bits
- Bit index `i` maps to word `i / 64`, bit `i % 64`
- Unused trailing bits in last word are masked to zero

**Example:**
```
Bitmap with len=100:
  bits.len() = 2 words (⌈100/64⌉ = 2)

  bits[0] = 0b...110101  (first 64 bits)
  bits[1] = 0b...001100  (bits 64-99, upper 28 bits masked)
```

**Operations:**

| Method | Complexity | Description |
|--------|-----------|-------------|
| `set(idx)` | O(1) | Set bit at index to 1 |
| `clear(idx)` | O(1) | Set bit at index to 0 |
| `is_set(idx)` | O(1) | Check if bit is 1 |
| `and(other)` | O(words) | Bitwise AND with another bitmap |
| `or(other)` | O(words) | Bitwise OR with another bitmap |
| `invert()` | O(words) | Bitwise NOT (with trailing mask) |
| `count_ones()` | O(words) | Count set bits via `u64::count_ones()` |
| `iter_ones()` | O(set bits) | Iterator over indices of set bits |
| `fill(bool)` | O(words) | Set all bits to 0 or 1 |
| `extend_from(other)` | O(other.len) | Append another bitmap |

**Iterator Performance:**

`iter_ones()` uses `trailing_zeros()` to skip unset bits efficiently:
```rust
impl Iterator for BitmapOnesIter<'_> {
    fn next(&mut self) -> Option<usize> {
        while self.current_word != 0 {
            let tz = self.current_word.trailing_zeros();
            let idx = self.word_idx * 64 + tz;
            self.current_word &= self.current_word - 1;  // Clear lowest set bit
            return Some(idx);
        }
        // Move to next word...
    }
}
```

This is **~64× faster** than iterating all indices when sparsely populated (1% selectivity).

---

### ColumnData (`src/sql/executor/batch.rs`)

Type-specific column storage to avoid string parsing.

**Variants:**
```rust
pub enum ColumnData {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Text(Vec<String>),
}
```

**Type Selection:**

When loading from storage (string-based `Entry`):
```
Try parse as i64:
  ├─▶ Success → ColumnData::Int64
  └─▶ Fail
        ├─▶ Try parse as f64:
        │     ├─▶ Success → ColumnData::Float64
        │     └─▶ Fail → ColumnData::Text
```

**Benefits:**
- **Numeric comparisons**: Direct `<, >, ==` without parsing
- **Aggregate efficiency**: SUM/AVG directly accumulate floats
- **Sort performance**: Numeric sort faster than lexicographic

**Conversion Back to Text:**

Window functions and projections convert back to strings:
```rust
match column_data {
    Int64(vals) => vals.iter().map(|v| v.to_string()),
    Float64(vals) => vals.iter().map(|v| format_float(*v)),  // Smart formatting
    Text(vals) => vals.iter().cloned(),
}
```

---

### ColumnarPage (`src/sql/executor/batch.rs`)

Single column's data with null tracking.

**Structure:**
```rust
pub struct ColumnarPage {
    pub page_metadata: String,
    pub data: ColumnData,
    pub null_bitmap: Bitmap,
    pub num_rows: usize,
}
```

**Null Encoding:**
- Storage layer uses sentinel string `"__NULL_1735938241__"` for nulls
- Batch processing converts to bitmap representation:
  ```
  Storage:  ["42", "__NULL__", "99"]
  Batch:    Int64([42, 0, 99]) + null_bitmap[0, 1, 0]
  ```

**Gather Operation:**

Efficiently select rows by indices:
```rust
fn gather(&self, indices: &[usize]) -> ColumnarPage {
    let data = match &self.data {
        Int64(values) => Int64(indices.iter().map(|&i| values[i]).collect()),
        Float64(values) => Float64(indices.iter().map(|&i| values[i]).collect()),
        Text(values) => Text(indices.iter().map(|&i| values[i].clone()).collect()),
    };

    let mut null_bitmap = Bitmap::new(indices.len());
    for (out_idx, &in_idx) in indices.iter().enumerate() {
        if self.null_bitmap.is_set(in_idx) {
            null_bitmap.set(out_idx);
        }
    }

    ColumnarPage { data, null_bitmap, num_rows: indices.len(), .. }
}
```

---

### ColumnarBatch (`src/sql/executor/batch.rs`)

Multi-column batch representing a query result or intermediate state.

**Structure:**
```rust
pub struct ColumnarBatch {
    pub columns: HashMap<usize, ColumnarPage>,  // Keyed by ordinal
    pub num_rows: usize,
    pub aliases: HashMap<String, usize>,        // Name → ordinal mapping
}
```

**Why Ordinal Keys?**

Columns are indexed by ordinal position, not name, to handle:
- Duplicate column names (e.g., multiple aggregates)
- Expression results without natural names
- Aliasing without column renaming

**Example:**
```sql
SELECT id, price * 1.1 AS discounted, COUNT(*) AS cnt
FROM products
```

Batch structure:
```
columns:
  0 → ColumnarPage { data: Int64([...]), ... }      // id
  1 → ColumnarPage { data: Float64([...]), ... }    // price * 1.1
  2 → ColumnarPage { data: Int64([...]), ... }      // COUNT(*)

aliases:
  "id" → 0
  "discounted" → 1
  "cnt" → 2
```

**Gather:**

Filter batch by row indices:
```rust
fn gather(&self, indices: &[usize]) -> ColumnarBatch {
    let mut columns = HashMap::with_capacity(self.columns.len());
    for (ordinal, page) in &self.columns {
        columns.insert(*ordinal, page.gather(indices));
    }
    ColumnarBatch {
        columns,
        num_rows: indices.len(),
        aliases: self.aliases.clone(),
    }
}
```

---

## Expression Evaluation on Batches

### WHERE Clause Evaluation

**Goal:** Build a selection bitmap indicating which rows satisfy the predicate.

**Algorithm** (`src/sql/executor/expressions.rs::evaluate_selection_on_batch`):

```
Input: WHERE age > 25 AND status = 'active'
       batch with columns [age: Int64, status: Text]

Step 1: Initialize selection bitmap
  bitmap ← Bitmap::new(batch.num_rows)
  bitmap.fill(true)  // Start with all rows selected

Step 2: Evaluate "age > 25"
  result_bitmap ← Bitmap::new(batch.num_rows)
  age_col ← batch.columns[age_ordinal].data

  match age_col {
    Int64(values) =>
      for (idx, value) in values.iter().enumerate() {
        if value > 25 {
          result_bitmap.set(idx)
        }
      }
  }

  bitmap.and(&result_bitmap)  // Combine with AND

Step 3: Evaluate "status = 'active'"
  result_bitmap ← Bitmap::new(batch.num_rows)
  status_col ← batch.columns[status_ordinal].data

  match status_col {
    Text(values) =>
      for (idx, value) in values.iter().enumerate() {
        if value == "active" {
          result_bitmap.set(idx)
        }
      }
  }

  bitmap.and(&result_bitmap)  // Combine with AND

Step 4: Return selection bitmap
  bitmap  // [1, 0, 1, 1, 0, ...]
```

**Gathering Results:**

```rust
let selection_bitmap = evaluate_selection_on_batch(&where_expr, &batch)?;
let selected_indices: Vec<usize> = selection_bitmap.iter_ones().collect();
let filtered_batch = batch.gather(&selected_indices);
```

---

### Projection Evaluation

**Goal:** Compute projected columns from expressions.

**Algorithm** (`src/sql/executor/expressions.rs::evaluate_expression_on_batch`):

```
Input: SELECT price * 1.1 AS discounted
       batch with column [price: Float64([10.0, 20.0, 30.0])]

Step 1: Evaluate "price" identifier
  price_col ← batch.columns[price_ordinal]
  → Float64([10.0, 20.0, 30.0])

Step 2: Evaluate literal "1.1"
  → ScalarValue::Float(1.1)

Step 3: Evaluate multiplication
  results ← Vec::with_capacity(batch.num_rows)

  for row_idx in 0..batch.num_rows {
    left ← price_col.data[row_idx]  // 10.0, 20.0, 30.0
    right ← 1.1
    results.push(left * right)      // 11.0, 22.0, 33.0
  }

  → ColumnData::Float64([11.0, 22.0, 33.0])

Step 4: Create ColumnarPage
  ColumnarPage {
    data: Float64([11.0, 22.0, 33.0]),
    null_bitmap: Bitmap::new(3),  // All non-null
    num_rows: 3,
  }
```

---

## Aggregation with Batches

### Hash-Based GROUP BY

**Goal:** Partition rows by group key, accumulate aggregates per group.

**Algorithm** (`src/sql/executor/aggregates.rs::AggregationHashTable`):

```
Input: SELECT region, SUM(sales) FROM orders GROUP BY region
       batch with columns [region: Text, sales: Float64]

Step 1: Evaluate group keys
  group_keys ← evaluate_group_keys_on_batch(&[region_expr], &batch)
  → [ GroupKey("US"), GroupKey("EU"), GroupKey("US"), GroupKey("EU") ]

Step 2: Initialize hash table
  table ← HashMap<GroupKey, AggregateState>

Step 3: Accumulate per row
  for (row_idx, key) in group_keys.iter().enumerate() {
    state ← table.entry(key).or_insert_with(AggregateState::new)

    // Extract sales value for this row
    sales_value ← batch.columns[sales_ordinal].data[row_idx]

    // Update SUM accumulator
    state.sum += sales_value
    state.count += 1
  }

Step 4: Materialize results
  result_batch ← ColumnarBatch::new()

  for (key, state) in table {
    result_batch.append_row([
      key.to_string(),        // region
      state.sum.to_string(),  // SUM(sales)
    ])
  }
```

**GroupKey Structure:**

```rust
struct GroupKey(Vec<ScalarValue>);  // Multi-column keys

impl Hash for GroupKey { ... }
impl Eq for GroupKey { ... }
```

**Multi-column example:**
```sql
GROUP BY region, status
→ GroupKey([Text("US"), Text("active")])
→ GroupKey([Text("EU"), Text("pending")])
```

---

### Vectorized Aggregate Updates

For better performance on single-group aggregates (no GROUP BY), specialized vectorized paths:

**COUNT(*):**
```rust
fn vectorized_count_star_update(state: &mut AggregateState, batch: &ColumnarBatch) {
    state.count += batch.num_rows as i64;
}
```

**SUM(column):**
```rust
fn vectorized_sum_update(state: &mut AggregateState, column: &ColumnarPage) {
    match &column.data {
        Float64(values) => {
            for (idx, value) in values.iter().enumerate() {
                if !column.null_bitmap.is_set(idx) {
                    state.sum += value;
                    state.count += 1;
                }
            }
        }
        Int64(values) => {
            for (idx, value) in values.iter().enumerate() {
                if !column.null_bitmap.is_set(idx) {
                    state.sum += *value as f64;
                    state.count += 1;
                }
            }
        }
        _ => { /* fallback */ }
    }
}
```

**Benefits:**
- Direct array access, no expression evaluation
- Branch predictor-friendly loops
- LLVM auto-vectorization potential

---

## Sorting with Batches

### Multi-Column Sort Keys

**Goal:** Sort batch by multiple ORDER BY clauses with NULLS placement.

**Algorithm** (`src/sql/executor/ordering.rs::sort_batch_in_memory`):

```
Input: ORDER BY age DESC NULLS LAST, name ASC
       batch with columns [age: Int64, name: Text]

Step 1: Build sort keys for each row
  keys ← Vec::with_capacity(batch.num_rows)

  for row_idx in 0..batch.num_rows {
    key ← OrderKey {
      values: [
        evaluate_expr(&age_expr, row_idx, batch),   // ScalarValue::Int(25)
        evaluate_expr(&name_expr, row_idx, batch),  // ScalarValue::Text("Alice")
      ]
    }
    keys.push((key, row_idx))
  }

Step 2: Sort with custom comparator
  keys.sort_unstable_by(|(left_key, _), (right_key, _)| {
    compare_order_keys(left_key, right_key, order_clauses)
  })

Step 3: Gather batch in sorted order
  sorted_indices ← keys.iter().map(|(_, row_idx)| row_idx).collect()
  sorted_batch ← batch.gather(&sorted_indices)
```

**Comparison Logic:**

```rust
fn compare_order_keys(left: &OrderKey, right: &OrderKey, clauses: &[OrderClause]) -> Ordering {
    for (idx, clause) in clauses.iter().enumerate() {
        let lhs = &left.values[idx];
        let rhs = &right.values[idx];

        // Handle nulls per NULLS FIRST/LAST
        let ord = match (lhs, rhs) {
            (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
            (ScalarValue::Null, _) => match clause.nulls {
                NullsPlacement::First => Ordering::Less,
                NullsPlacement::Last => Ordering::Greater,
                NullsPlacement::Default => if clause.descending {
                    Ordering::Greater  // DESC defaults to NULLS LAST
                } else {
                    Ordering::Less     // ASC defaults to NULLS FIRST
                },
            },
            (_, ScalarValue::Null) => /* symmetric */,
            _ => compare_scalar_values(lhs, rhs),
        };

        // Apply DESC reversal
        let ord = if clause.descending {
            ord.reverse()
        } else {
            ord
        };

        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
```

---

## Performance Characteristics

### Batch Size Trade-offs

| Batch Size | Memory | Throughput | Latency |
|------------|--------|------------|---------|
| 100 rows | Low | Poor (overhead dominates) | Excellent |
| 1,000 rows | **Optimal** | **Good** | **Good** |
| 10,000 rows | High | Excellent (vectorization) | Poor (blocking) |
| 100,000 rows | Very high | Diminishing returns | Very poor |

**Current default:** `ROWS_PER_PAGE_GROUP = 1000` provides good balance.

---

### Bitmap vs Array Indexing

**Filtering 1% of 10,000 rows:**

**Array approach:**
```rust
let mut result = Vec::new();
for i in 0..10_000 {
    if predicate(i) {
        result.push(i);
    }
}
// 10,000 predicate evaluations
// 100 vector pushes
```

**Bitmap approach:**
```rust
let mut bitmap = Bitmap::new(10_000);
for i in 0..10_000 {
    if predicate(i) {
        bitmap.set(i);
    }
}
let result: Vec<usize> = bitmap.iter_ones().collect();
// 10,000 predicate evaluations
// ~157 word iterations (10,000 / 64)
// 100 indices collected
```

**Bitmap wins for:**
- Boolean operations (AND/OR of filters)
- Counting selected rows (`count_ones()` is 1 CPU instruction per word)
- Sparse selections (iterator skips unset bits)

---

## Conversion Overhead

### From Storage to Batch

**Loading 1,000 rows × 3 columns:**

```
Page format (storage):
  Vec<Entry> where Entry { data: String, ... }

Conversion pipeline:
  ├─▶ For each column:
  │     ├─▶ Extract Entry.data: &str
  │     ├─▶ Check null sentinel
  │     │     ├─▶ Is null → set null_bitmap
  │     │     └─▶ Not null → try_parse
  │     ├─▶ try_parse::<i64>()
  │     │     ├─▶ Success → append to Int64 array
  │     │     └─▶ Fail → try_parse::<f64>()
  │     │           ├─▶ Success → append to Float64 array
  │     │           └─▶ Fail → clone to Text array
  │     └─▶ Result: ColumnarPage
  │
  └─▶ Batch: ColumnarBatch { columns: [page1, page2, page3] }
```

**Cost:** ~1-2 μs per row (dominated by string parsing), amortized over batch.

---

### From Batch to Result Strings

**Window function output:**

```rust
// Convert Int64 column back to strings
match &column.data {
    Int64(values) => {
        for (idx, value) in values.iter().enumerate() {
            if null_bitmap.is_set(idx) {
                result.push("__NULL__".to_string());
            } else {
                result.push(value.to_string());
            }
        }
    }
}
```

**Cost:** ~100 ns per value (integer to string formatting).

---

## Future Optimizations

### Vectorization Opportunities

Current implementation is **scalar** (one value at a time). Potential SIMD optimizations:

**Bitmap operations:**
```rust
// Current: word-by-word
for (lhs, rhs) in self.bits.iter_mut().zip(other.bits.iter()) {
    *lhs &= *rhs;
}

// SIMD potential: 4 words at a time (256-bit AVX2)
// Requires explicit SIMD or relying on LLVM auto-vectorization
```

**Numeric comparisons:**
```rust
// Current: element-by-element
for (idx, value) in values.iter().enumerate() {
    if *value > threshold {
        bitmap.set(idx);
    }
}

// SIMD potential: 4 i64s at a time
// _mm256_cmpgt_epi64 (AVX2)
```

**Estimated speedup:** 2-4× for predicate evaluation on numeric columns.

---

### Late Materialization

Current approach materializes all columns at start of query. Alternative:

```
SELECT id, price
FROM products
WHERE category = 'electronics' AND price > 100
```

**Current:**
1. Load all columns: id, price, category
2. Filter on category → bitmap
3. Filter on price → bitmap
4. Gather id, price

**Late materialization:**
1. Load category only
2. Filter on category → bitmap (small selection)
3. Load price for selected rows only
4. Filter on price → refine bitmap
5. Load id for final selection only

**Benefit:** Avoid loading/parsing columns that won't appear in final result.

---

## Related Documentation

- **SQL Executor**: [sql_executor](sql_executor) - Query execution pipeline
- **Aggregates**: [sql_executor#aggregates](sql_executor#aggregates) - Aggregate functions
- **Expressions**: [sql_executor#expressions](sql_executor#expressions) - Expression evaluation
- **Ordering**: [sql_executor#ordering](sql_executor#ordering) - Sorting and ORDER BY
