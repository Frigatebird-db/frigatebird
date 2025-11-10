---
title: "Sorted Inserts (ORDER BY)"
layout: default
nav_order: 18
---

# Sorted Inserts (ORDER BY)

Tables created with an `ORDER BY` clause maintain sorted order across all columns. The `ops_handler` module provides different insertion strategies depending on whether the ORDER BY is single-column or multi-column.

## Table Creation

```sql
CREATE TABLE users (
    id INT,
    email TEXT,
    age INT
) ORDER BY age, email;
```

This creates a table with:
- **Sort key**: `[age, email]` (composite)
- **Sort ordinals**: `[2, 1]` (column positions)

## Implementation Overview

```
╔══════════════════════════════════════════════════════════════╗
║                  Sorted Insert Strategy                      ║
╚══════════════════════════════════════════════════════════════╝
                          │
                          ▼
               ┌──────────────────────┐
               │ Check sort key size  │
               └──────────┬───────────┘
                          │
         ┌────────────────┴────────────────┐
         │                                 │
         ▼                                 ▼
  ┌──────────────┐                ┌──────────────────┐
  │ Single-column│                │ Multi-column     │
  │ ORDER BY     │                │ ORDER BY         │
  └──────┬───────┘                └────────┬─────────┘
         │                                 │
         │ binary_search_insert_index      │ find_insert_position
         │ O(log n)                        │ O(n)
         │                                 │
         └────────────────┬────────────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │ Insert at found index  │
              │ across ALL columns     │
              └────────────────────────┘
```

## Single-Column ORDER BY

### Algorithm

Uses Rust's `binary_search_by` for O(log n) insertion point discovery.

**Code path:** `sorted_insert_single_column()`

```rust
fn sorted_insert_single_column(handler, table, col, data) {
    // 1. Load latest page for the sort column
    let page_meta = handler.locate_latest_in_table(table, col)?;
    let page_arc = handler.get_page(page_meta.clone())?;

    // 2. Binary search for insertion index
    let insert_idx = binary_search_insert_index(&page.entries, data);

    // 3. Insert entry at found position
    page.entries.insert(insert_idx, Entry::new(data));

    // 4. Update metadata entry count
    handler.update_entry_count_in_table(table, col, new_count)?;

    // 5. Write back to cache
    handler.write_back_uncompressed(&page_meta.id, updated);
}
```

### Binary Search Implementation

```rust
fn binary_search_insert_index(entries: &[Entry], value: &str) -> usize {
    match entries.binary_search_by(|entry| compare_entry_value(entry, value)) {
        Ok(idx) | Err(idx) => idx,
    }
}
```

**Behavior:**
- `Ok(idx)`: Exact match found → insert **at** that position (before existing)
- `Err(idx)`: No match → insert at position where value would maintain sort order

### Example: Single-Column Insert

```
Existing page (sorted by age):
┌───────────┐
│ Entry: 18 │  idx=0
│ Entry: 25 │  idx=1
│ Entry: 42 │  idx=2
│ Entry: 50 │  idx=3
└───────────┘

Insert "30":
  binary_search_by(|e| compare_strs(e.data, "30"))

  Comparisons:
    18 vs 30 → Less    → continue search
    25 vs 30 → Less    → continue search
    42 vs 30 → Greater → found! insert before idx=2

Result:
┌───────────┐
│ Entry: 18 │  idx=0
│ Entry: 25 │  idx=1
│ Entry: 30 │  idx=2 ← inserted
│ Entry: 42 │  idx=3
│ Entry: 50 │  idx=4
└───────────┘
```

**Complexity:** O(log n) comparisons

## Multi-Column ORDER BY

### Algorithm

Uses linear scan with type-aware comparison across sort key columns.

**Code path:** `sorted_insert_row(handler, table, row)`

```
╔═══════════════════════════════════════════════════════════════╗
║               insert_sorted_row(row)                          ║
╚═══════════════════════════════════════════════════════════════╝
                          │
                          ▼
      ┌────────────────────────────────────────┐
      │ 1. Load TableCatalog                   │
      │    Get sort_ordinals: [ord_1, ord_2]   │
      └────────────┬───────────────────────────┘
                   │
                   ▼
      ┌────────────────────────────────────────┐
      │ 2. Validate sort key columns provided  │
      │    Ensure row contains values for      │
      │    all columns in ORDER BY             │
      └────────────┬───────────────────────────┘
                   │
                   ▼
      ┌────────────────────────────────────────┐
      │ 3. Build full row vector               │
      │    row_values[col] = provided value    │
      │                   OR ""                │
      └────────────┬───────────────────────────┘
                   │
                   ▼
      ┌────────────────────────────────────────┐
      │ 4. find_insert_position()              │
      │    Linear scan: 0..row_count           │
      │      compare_row_against_existing()    │
      └────────────┬───────────────────────────┘
                   │
                   ▼
      ┌────────────────────────────────────────┐
      │ 5. For each column:                    │
      │    - Load latest page                  │
      │    - Insert entry at insert_idx        │
      │    - Update metadata entry count       │
      │    - Write back to cache               │
      └────────────────────────────────────────┘
```

### Linear Scan Details

```rust
fn find_insert_position(
    handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    sort_ordinals: &[usize],
    new_row: &[String],
    row_count: usize,
) -> Result<usize> {
    for idx in 0..row_count {
        let cmp = compare_row_against_existing(
            handler,
            table,
            columns,
            sort_ordinals,
            new_row,
            idx as u64,
        )?;
        if cmp == Ordering::Less {
            return Ok(idx);  // Insert before this row
        }
    }
    Ok(row_count)  // Append at end
}
```

**Complexity:** O(n) where n = number of existing rows

**Why not binary search?**
- Multi-column comparison requires reading multiple pages per row
- Each comparison involves `PageHandler::read_entry_at()` calls
- Binary search would amplify cache misses and I/O

### Row Comparison Logic

```rust
fn compare_row_against_existing(
    handler: &PageHandler,
    table: &str,
    columns: &[ColumnCatalog],
    sort_ordinals: &[usize],
    new_row: &[String],
    existing_idx: u64,
) -> Result<Ordering> {
    // Compare columns in sort key order
    for &ordinal in sort_ordinals {
        let column_name = &columns[ordinal].name;

        // Read existing row's value for this column
        let existing_entry = handler.read_entry_at(
            table,
            column_name,
            existing_idx
        )?;

        // Compare new vs existing
        let cmp = compare_strs(&new_row[ordinal], existing_entry.get_data());

        // Early return on first difference
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }

    // All sort columns equal
    Ok(Ordering::Equal)
}
```

### Example: Multi-Column Insert

```
Table: users ORDER BY age, email
Existing rows:
┌─────┬─────────────────┐
│ age │ email           │
├─────┼─────────────────┤
│ 18  │ alice@test.com  │  row 0
│ 25  │ bob@test.com    │  row 1
│ 25  │ charlie@test.com│  row 2
│ 42  │ dave@test.com   │  row 3
└─────┴─────────────────┘

Insert: {age: "25", email: "carol@test.com"}

Linear scan comparisons:
  Row 0: compare("25", "18") → Greater → continue
  Row 1: compare("25", "25") → Equal
         compare("carol@test.com", "bob@test.com") → Greater → continue
  Row 2: compare("25", "25") → Equal
         compare("carol@test.com", "charlie@test.com") → Less → FOUND!
         Insert at idx=2

Result:
┌─────┬─────────────────┐
│ age │ email           │
├─────┼─────────────────┤
│ 18  │ alice@test.com  │  row 0
│ 25  │ bob@test.com    │  row 1
│ 25  │ carol@test.com  │  row 2 ← inserted
│ 25  │ charlie@test.com│  row 3 (shifted)
│ 42  │ dave@test.com   │  row 4 (shifted)
└─────┴─────────────────┘
```

## Type-Aware Comparison

All comparisons use `compare_strs()` which attempts numeric comparison first:

```rust
fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),  // Lexicographic fallback
    }
}
```

### Comparison Rules

```
╔═══════════════════════════════════════════════════════════╗
║                  compare_strs(left, right)                ║
╚═══════════════════════════════════════════════════════════╝
                          │
                          ▼
              ┌───────────────────────┐
              │ Try parse both as f64 │
              └───────────┬───────────┘
                          │
         ┌────────────────┴────────────────┐
         │                                 │
         ▼ Both parse OK                   ▼ At least one fails
  ┌──────────────┐                  ┌──────────────────┐
  │ Numeric      │                  │ Lexicographic    │
  │ comparison   │                  │ string compare   │
  │ 2 < 10 < 100 │                  │ "2" < "10" fails │
  └──────────────┘                  │ "10" < "2" (!)   │
                                    └──────────────────┘
```

### Examples

| Left | Right | Comparison | Result | Reason |
|------|-------|------------|--------|--------|
| `"2"` | `"10"` | Numeric | `Less` | 2.0 < 10.0 |
| `"3.14"` | `"2.718"` | Numeric | `Greater` | 3.14 > 2.718 |
| `"alice"` | `"bob"` | Lexicographic | `Less` | 'a' < 'b' |
| `"10"` | `"abc"` | Lexicographic | `Less` | '1' < 'a' (parse fails for "abc") |
| `"true"` | `"false"` | Lexicographic | `Greater` | 't' > 'f' |

**Important:** Mixing numeric and non-numeric values in the same ORDER BY column leads to inconsistent sort order (numeric values treated as strings).

## Missing/Empty Values

Non-provided columns default to empty string `""`:

```rust
let mut row_values: Vec<Option<String>> = vec![None; columns.len()];

// ... populate from provided values ...

let final_row: Vec<String> = row_values
    .into_iter()
    .map(|opt| opt.unwrap_or_else(|| "".to_string()))
    .collect();
```

### Behavior

```
Table: users(id, email, age) ORDER BY age, email

Insert: {email: "alice@test.com"}  // age not provided

Actual row inserted:
  id:    ""  (empty)
  email: "alice@test.com"
  age:   ""  (empty, used for sorting!)

Sort position: "" compares lexicographically
  "" < "18" → inserted at beginning
```

**Warning:** Empty strings sort **before** all non-empty values lexicographically, but this behavior may be surprising when mixing with numeric columns.

## Metadata Synchronization

After inserting at index `idx`, ALL columns must be updated:

```rust
for (ordinal, column) in columns.iter().enumerate() {
    let descriptor = handler.locate_latest_in_table(table, &column.name)?;
    let page_arc = handler.get_page(descriptor.clone())?;

    let mut updated = (*page_arc).clone();
    let insert_pos = insert_idx.min(updated.page.entries.len());

    // Insert entry at same position across all columns
    updated.page.entries.insert(insert_pos, Entry::new(&final_row[ordinal]));

    handler.write_back_uncompressed(&descriptor.id, updated);

    // Update metadata entry count
    handler.update_entry_count_in_table(
        table,
        &column.name,
        new_count as u64
    )?;
}
```

**Critical invariant:** All columns must have the same entry count after insertion, or row alignment breaks.

## UPDATE Operations with ORDER BY

When updating a row in an ORDER BY table:

```sql
UPDATE users SET age = 30 WHERE id = 42;
```

### Decision Tree

```
╔═══════════════════════════════════════════════════════════╗
║         Does UPDATE modify ORDER BY columns?              ║
╚═══════════════════════════════════════════════════════════╝
                          │
         ┌────────────────┴────────────────┐
         │                                 │
         ▼ NO                              ▼ YES
  ┌──────────────┐                  ┌──────────────┐
  │ overwrite_row│                  │ delete_row + │
  │ (in-place)   │                  │insert_sorted │
  └──────────────┘                  │  _row        │
                                    └──────────────┘
```

**Rationale:** If ORDER BY columns change, the row's position in the sort order changes. Rather than implementing in-place reordering, we delete and reinsert.

### Implementation (from `sql/executor.rs`)

```rust
// Check if ORDER BY columns changed
let order_by_changed = catalog
    .sort_key()
    .iter()
    .any(|col| assignments.contains_key(&col.name));

if order_by_changed {
    // Delete old row
    delete_row(handler, table, row_idx)?;

    // Build new row with updates
    let mut new_values = old_values.clone();
    for (col_name, new_val) in assignments {
        new_values[col.ordinal] = new_val;
    }

    // Reinsert in sorted position
    insert_sorted_row(handler, table, &row_tuples)?;
} else {
    // In-place overwrite
    overwrite_row(handler, table, row_idx, &new_values)?;
}
```

## DELETE Operations

Deletion removes entries at the same index from ALL columns:

```rust
pub fn delete_row(
    handler: &PageHandler,
    table: &str,
    row_idx: u64,
) -> Result<()> {
    let catalog = handler.table_catalog(table)?;

    // Iterate columns in REVERSE order (consistency during partial failures)
    for column in catalog.columns().iter().rev() {
        let descriptor = handler.locate_latest_in_table(table, &column.name)?;
        let page_arc = handler.get_page(descriptor.clone())?;

        let mut updated = (*page_arc).clone();
        updated.page.entries.remove(row_idx as usize);

        let new_len = updated.page.entries.len() as u64;
        handler.write_back_uncompressed(&descriptor.id, updated);

        handler.update_entry_count_in_table(table, &column.name, new_len)?;
    }

    Ok(())
}
```

**Why reverse order?** If deletion fails partway through, earlier columns retain the old row while later columns are already modified. Reverse order minimizes partial state visibility.

## Performance Characteristics

| Operation | Single-Column | Multi-Column | Notes |
|-----------|---------------|--------------|-------|
| **Insert** | O(log n) | O(n) | Single-column uses binary search |
| **Comparison** | O(1) | O(k) | k = number of columns in ORDER BY |
| **Page reads** | 1 | n × k | Multi-column reads k columns for each of n rows |
| **Cache pressure** | Low | High | Multi-column thrashes cache with random access |

### Bottlenecks

**Multi-column sorted inserts are slow:**
- Linear scan: O(n) row comparisons
- Each comparison: k column reads via `PageHandler::read_entry_at`
- Each read: potential cache miss → CPC/disk fetch
- Worst case: n × k page fetches for single insert

**Example:**
- Table with 10,000 rows
- ORDER BY on 3 columns
- Insert requires up to 10,000 × 3 = 30,000 column reads

## Module Location

- **Source:** `src/ops_handler.rs`
- **SQL Integration:** `src/sql/executor.rs`
- **Tests:** `tests/end_to_end_tests.rs`

## Related Documentation

- [Metadata Store](metadata_store) - Entry count tracking and prefix arrays
- [Page Handler](page_handler) - `read_entry_at()` implementation
- [SQL Executor](sql_executor) - UPDATE/DELETE logic with ORDER BY
- [Ops Handler](ops_handler) - General row operations
