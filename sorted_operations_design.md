# Design: Sorted Operations for ORDER BY Tables

## Problem Statement

Currently, the database supports `ORDER BY` in `CREATE TABLE` statements, but lacks the infrastructure to maintain sorted order during `INSERT`, `UPDATE`, and `DELETE` operations.

**Current limitations:**
- `INSERT` appends to the end of pages, ignoring sort order
- `UPDATE` to sort-key columns doesn't reposition rows
- `DELETE` is not implemented
- INSERT only generates operations for specified columns, causing row inconsistency

**Example scenario:**
```sql
CREATE TABLE users (id, name, age) ORDER BY age;
INSERT INTO users (name, age) VALUES ('Alice', 30);  -- Should insert at sorted position
UPDATE users SET age = 50 WHERE id = 1;              -- Should reposition row
DELETE FROM users WHERE id = 1;                      -- Should remove from all columns
```

## Current Architecture

### Component Overview

```
SQL String
    ↓
Parser (src/sql/parser.rs)
    ↓
Planner (src/sql/planner.rs) → QueryPlan
    ↓
[MISSING: Executor Layer]
    ↓
Writer (src/writer/executor.rs) → UpdateJob
    ↓
Worker Thread → apply_operations()
    ↓
Disk + Metadata
```

### Key Data Structures

**UpdateJob** (`src/writer/update_job.rs`):
```rust
pub struct UpdateJob {
    pub table: String,
    pub columns: Vec<ColumnUpdate>,
}

pub struct ColumnUpdate {
    pub column: String,
    pub operations: Vec<UpdateOp>,
}

pub enum UpdateOp {
    Append { entry: Entry },
    Overwrite { row: u64, entry: Entry },
}
```

**TableCatalog** (`src/metadata_store/mod.rs`):
```rust
pub struct TableCatalog {
    name: String,
    columns: Vec<ColumnCatalog>,
    sort_key_ordinals: Vec<usize>,  // Ordinal positions of ORDER BY columns
}
```

**ColumnChain** (`src/metadata_store/mod.rs`):
```rust
pub struct ColumnChain {
    pub pages: Vec<PageDescriptor>,
    pub prefix_entry_counts: Vec<u64>,  // Cumulative entry counts [0, 1000, 2500, ...]
}
```

**Page** (`src/page/mod.rs`):
```rust
pub struct Page {
    pub page_metadata: String,
    pub entries: Vec<Entry>,  // Variable-size vector
}
```

### Key Insight: Variable-Size Pages

**Pages can have different numbers of entries** - there are no fixed boundaries. This means:
- Inserting into page 0 doesn't cascade to other pages
- Only need to rewrite the affected page(s)
- Update `prefix_entry_counts` metadata to reflect new sizes

## Design Goals

1. **Maintain sort order** for tables with `ORDER BY`
2. **Row consistency** - all columns must have entries at the same row indices
3. **Minimal performance impact** - avoid full table scans where possible
4. **Incremental implementation** - can be rolled out in phases

## Proposed Solution

### High-Level Approach

1. **Build executor layer** - Bridge QueryPlan → UpdateJob
2. **All-column coordination** - Operations affect ALL columns at same row index
3. **New operations** - `SortedInsert`, `Delete`, `Move`
4. **Entry comparator** - Compare entries based on sort key columns
5. **Row materialization** - Read full rows when needed for repositioning

### Architecture Overview

```
SQL INSERT/UPDATE/DELETE
    ↓
Planner → QueryPlan (with write_columns)
    ↓
[NEW] Executor:
  - Resolve ALL columns from table metadata
  - Apply WHERE filters to get affected rows
  - For each affected row:
    * Read current values (for UPDATE/DELETE)
    * Compute new values (for UPDATE/INSERT)
    * Determine operation type (SortedInsert, Move, Delete)
    * Generate UpdateOp for ALL columns
    ↓
Writer → UpdateJob (all columns)
    ↓
Worker:
  - For each column, stage operations
  - Find insertion points using comparator (for SortedInsert)
  - Apply operations to pages
  - Update prefix_entry_counts
    ↓
Commit to metadata + disk
```

## Detailed Design

### Phase 1: Foundation - Executor Layer & Entry Comparator

#### 1.1 Entry Comparator

**New file: `src/entry/comparator.rs`**

```rust
use super::Entry;
use std::cmp::Ordering;

pub struct EntryComparator {
    sort_key_ordinals: Vec<usize>,
}

impl EntryComparator {
    pub fn new(sort_key_ordinals: Vec<usize>) -> Self {
        Self { sort_key_ordinals }
    }

    /// Compare two complete rows based on sort key columns
    /// row1 and row2 are arrays where index = column ordinal
    pub fn compare_rows(&self, row1: &[Entry], row2: &[Entry]) -> Ordering {
        for &ordinal in &self.sort_key_ordinals {
            match self.compare_entries(&row1[ordinal], &row2[ordinal]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }

    fn compare_entries(&self, e1: &Entry, e2: &Entry) -> Ordering {
        let d1 = e1.get_data();
        let d2 = e2.get_data();

        // Try numeric comparison first
        if let (Ok(n1), Ok(n2)) = (d1.parse::<f64>(), d2.parse::<f64>()) {
            return n1.partial_cmp(&n2).unwrap_or(Ordering::Equal);
        }

        // Fallback to lexicographic
        d1.cmp(d2)
    }
}
```

**Why pass entire rows?** Because sort key might have multiple columns (e.g., `ORDER BY age, name`).

#### 1.2 SQL Executor Layer

**New file: `src/sql/executor.rs`**

```rust
use crate::metadata_store::TableMetaStore;
use crate::writer::{Writer, UpdateJob, ColumnUpdate, UpdateOp};
use crate::entry::Entry;
use crate::sql::planner::QueryPlan;

pub struct SqlExecutor {
    metadata: Arc<TableMetaStore>,
    writer: Arc<Writer>,
}

impl SqlExecutor {
    pub fn new(metadata: Arc<TableMetaStore>, writer: Arc<Writer>) -> Self {
        Self { metadata, writer }
    }

    /// Execute an INSERT statement
    pub fn execute_insert(
        &self,
        table_name: &str,
        plan: &QueryPlan,
        values: Vec<Vec<String>>,  // VALUES clause data
    ) -> Result<(), ExecutorError> {
        let table = self.metadata.get_table(table_name)?;
        let all_columns = table.columns();
        let specified_columns = self.extract_write_columns(plan)?;

        // For each row in VALUES
        for value_row in values {
            let mut column_updates = Vec::new();

            // Generate UpdateOp for EVERY column
            for (ordinal, col) in all_columns.iter().enumerate() {
                let entry = if let Some(value) = self.get_value_for_column(
                    col.name(),
                    &specified_columns,
                    &value_row,
                ) {
                    Entry::new(value)
                } else {
                    Entry::new("")  // NULL placeholder
                };

                let op = if table.has_sort_key() {
                    UpdateOp::SortedInsert { entry }
                } else {
                    UpdateOp::Append { entry }
                };

                column_updates.push(ColumnUpdate::new(
                    col.name().to_string(),
                    vec![op],
                ));
            }

            self.writer.submit(UpdateJob::new(table_name, column_updates))?;
        }

        Ok(())
    }

    fn extract_write_columns(&self, plan: &QueryPlan) -> Result<HashMap<String, usize>, ExecutorError> {
        // Map column name → position in VALUES
        // ...implementation...
    }

    fn get_value_for_column(
        &self,
        column_name: &str,
        specified: &HashMap<String, usize>,
        values: &[String],
    ) -> Option<String> {
        specified.get(column_name).map(|&idx| values[idx].clone())
    }
}
```

**Key points:**
- Resolves ALL columns from table metadata
- Uses `SortedInsert` if table has `ORDER BY`, else `Append`
- Generates operations for all columns, even unspecified ones (NULL)

### Phase 2: Sorted Insert Operation

#### 2.1 Extend UpdateOp

**Update `src/writer/update_job.rs`:**

```rust
pub enum UpdateOp {
    Append { entry: Entry },
    Overwrite { row: u64, entry: Entry },

    // NEW: Insert at sorted position
    SortedInsert { entry: Entry },
}
```

#### 2.2 Implement SortedInsert in Writer

**Update `src/writer/executor.rs`:**

```rust
fn stage_column(&self, table: &str, update: ColumnUpdate) -> Option<StagedColumn> {
    // Get table metadata for sort key
    let table_catalog = self.metadata.get_table(table)?;
    let sort_key_ordinals = if table_catalog.has_sort_key() {
        Some(table_catalog.sort_key_ordinals())
    } else {
        None
    };

    // ... existing code to load base page ...

    apply_operations(
        &mut prepared,
        &update.operations,
        sort_key_ordinals,
        &update.column,
        table,
        &self.metadata,
    );

    // ... rest of staging logic ...
}

fn apply_operations(
    page: &mut PageCacheEntryUncompressed,
    operations: &[UpdateOp],
    sort_key_ordinals: Option<&[usize]>,
    current_column: &str,
    table_name: &str,
    metadata: &TableMetaStore,
) {
    for op in operations {
        match op {
            UpdateOp::Append { entry } => {
                page.page.entries.push(entry.clone());
            }
            UpdateOp::Overwrite { row, entry } => {
                let row_idx = *row as usize;
                ensure_capacity(&mut page.page, row_idx);
                page.page.entries[row_idx] = entry.clone();
            }
            UpdateOp::SortedInsert { entry } => {
                if let Some(sort_key) = sort_key_ordinals {
                    let pos = find_insertion_position(
                        &page.page.entries,
                        entry,
                        sort_key,
                        current_column,
                        table_name,
                        metadata,
                    );
                    page.page.entries.insert(pos, entry.clone());
                } else {
                    // No sort key, just append
                    page.page.entries.push(entry.clone());
                }
            }
        }
    }
}
```

#### 2.3 Finding Insertion Position

**Critical challenge:** We're processing one column at a time, but need to compare based on multiple sort key columns.

**Solution:** During `SortedInsert`, we need access to the full row data to compare.

**Two approaches:**

**Approach A: Pass row data with SortedInsert**
```rust
pub enum UpdateOp {
    SortedInsert {
        entry: Entry,
        full_row: Vec<Entry>,  // Complete row for comparison
    },
}
```

Executor builds full row before submitting job.

**Approach B: Read other columns during staging**
```rust
fn find_insertion_position(
    page_entries: &[Entry],
    new_entry: &Entry,
    sort_key_ordinals: &[usize],
    current_column_name: &str,
    table_name: &str,
    metadata: &TableMetaStore,
) -> usize {
    // Get current column ordinal
    let table = metadata.get_table(table_name).unwrap();
    let current_col_ordinal = table.column_ordinal(current_column_name);

    // If current column IS a sort key column, we can compare directly
    if sort_key_ordinals.contains(&current_col_ordinal) && sort_key_ordinals.len() == 1 {
        // Simple case: single sort key column, binary search
        return binary_search_position(page_entries, new_entry);
    }

    // Complex case: need to read other columns
    // For now, return page_entries.len() (append to end)
    // TODO: Implement cross-column comparison
    page_entries.len()
}

fn binary_search_position(entries: &[Entry], new_entry: &Entry) -> usize {
    entries.binary_search_by(|probe| {
        let d1 = probe.get_data();
        let d2 = new_entry.get_data();

        // Try numeric
        if let (Ok(n1), Ok(n2)) = (d1.parse::<f64>(), d2.parse::<f64>()) {
            return n1.partial_cmp(&n2).unwrap_or(Ordering::Equal);
        }

        // Fallback to string
        d1.cmp(d2)
    }).unwrap_or_else(|pos| pos)
}
```

**Recommendation:** Start with **Approach A** - pass full row with operation. It's simpler and keeps Writer stateless.

#### 2.4 Update prefix_entry_counts

**In `src/writer/executor.rs`, after staging:**

```rust
fn handle_job(&self, job: UpdateJob) {
    // ... stage all columns ...

    // Commit metadata
    let mut updates = Vec::new();
    for staged in &staged_columns {
        updates.push((
            staged.column.clone(),
            staged.descriptor.clone(),
            staged.new_prefix_count,  // NEW: Include updated entry count
        ));
    }

    self.metadata.register_batch(&job.table, updates);
}

fn stage_column(&self, table: &str, update: ColumnUpdate) -> Option<StagedColumn> {
    // ... apply operations to page ...

    let new_entry_count = prepared.page.entries.len() as u64;

    Some(StagedColumn {
        column: update.column.clone(),
        serialized: serialized_page,
        descriptor: new_descriptor,
        new_prefix_count: base_prefix_count + new_entry_count,  // NEW
    })
}
```

**In `src/metadata_store/mod.rs`:**

```rust
pub fn register_batch(&mut self, table: &str, updates: Vec<(String, PageDescriptor, u64)>) {
    for (column_name, descriptor, new_count) in updates {
        let chain = self.get_or_create_chain(table, &column_name);

        if descriptor.replace_last && !chain.pages.is_empty() {
            // Replace last page
            chain.pages.pop();
            chain.prefix_entry_counts.pop();
        }

        chain.pages.push(descriptor);
        chain.prefix_entry_counts.push(new_count);
    }
}
```

### Phase 3: DELETE Operation

#### 3.1 Add Delete Operation

**Update `src/writer/update_job.rs`:**

```rust
pub enum UpdateOp {
    Append { entry: Entry },
    Overwrite { row: u64, entry: Entry },
    SortedInsert { entry: Entry, full_row: Vec<Entry> },

    // NEW: Delete row
    Delete { row: u64 },
}
```

#### 3.2 Implement Delete in Worker

**Update `src/writer/executor.rs`:**

```rust
fn apply_operations(
    page: &mut PageCacheEntryUncompressed,
    operations: &[UpdateOp],
    // ... other params ...
) {
    for op in operations {
        match op {
            // ... existing cases ...

            UpdateOp::Delete { row } => {
                let row_idx = *row as usize;
                if row_idx < page.page.entries.len() {
                    page.page.entries.remove(row_idx);
                }
            }
        }
    }
}
```

#### 3.3 SQL Executor for DELETE

**In `src/sql/executor.rs`:**

```rust
impl SqlExecutor {
    pub fn execute_delete(
        &self,
        table_name: &str,
        plan: &QueryPlan,  // Contains WHERE filters
    ) -> Result<(), ExecutorError> {
        // 1. Apply filters to get affected row indices
        let affected_rows = self.execute_filters(table_name, plan)?;

        // 2. For each affected row, generate Delete op for ALL columns
        let table = self.metadata.get_table(table_name)?;
        let mut column_updates = Vec::new();

        for col in table.columns() {
            let mut ops = Vec::new();
            for &row_idx in &affected_rows {
                ops.push(UpdateOp::Delete { row: row_idx });
            }
            column_updates.push(ColumnUpdate::new(col.name().to_string(), ops));
        }

        self.writer.submit(UpdateJob::new(table_name, column_updates))?;
        Ok(())
    }

    fn execute_filters(
        &self,
        table_name: &str,
        plan: &QueryPlan,
    ) -> Result<Vec<u64>, ExecutorError> {
        // Apply WHERE clause to get row indices
        // This requires reading data - connect to existing pipeline/filtering
        // For now, placeholder
        todo!("Implement filter execution")
    }
}
```

### Phase 4: UPDATE with Sort Key Changes

#### 4.1 Strategy: Decompose UPDATE → DELETE + INSERT

**In `src/sql/executor.rs`:**

```rust
impl SqlExecutor {
    pub fn execute_update(
        &self,
        table_name: &str,
        plan: &QueryPlan,
        set_clauses: HashMap<String, String>,  // column → new_value
    ) -> Result<(), ExecutorError> {
        let table = self.metadata.get_table(table_name)?;
        let sort_key_columns: HashSet<_> = table.sort_key()
            .iter()
            .map(|col| col.name())
            .collect();

        // Check if UPDATE affects sort key columns
        let affects_sort_key = set_clauses.keys()
            .any(|col| sort_key_columns.contains(col.as_str()));

        if affects_sort_key {
            // Use DELETE + INSERT strategy
            self.execute_update_with_repositioning(table_name, plan, set_clauses)
        } else {
            // Simple in-place update
            self.execute_update_in_place(table_name, plan, set_clauses)
        }
    }

    fn execute_update_in_place(
        &self,
        table_name: &str,
        plan: &QueryPlan,
        set_clauses: HashMap<String, String>,
    ) -> Result<(), ExecutorError> {
        // 1. Get affected rows
        let affected_rows = self.execute_filters(table_name, plan)?;

        // 2. Generate Overwrite ops only for modified columns
        let mut column_updates = Vec::new();

        for (column_name, new_value) in set_clauses {
            let mut ops = Vec::new();
            for &row_idx in &affected_rows {
                ops.push(UpdateOp::Overwrite {
                    row: row_idx,
                    entry: Entry::new(new_value.clone()),
                });
            }
            column_updates.push(ColumnUpdate::new(column_name, ops));
        }

        self.writer.submit(UpdateJob::new(table_name, column_updates))?;
        Ok(())
    }

    fn execute_update_with_repositioning(
        &self,
        table_name: &str,
        plan: &QueryPlan,
        set_clauses: HashMap<String, String>,
    ) -> Result<(), ExecutorError> {
        let table = self.metadata.get_table(table_name)?;
        let affected_rows = self.execute_filters(table_name, plan)?;

        // For each affected row:
        for row_idx in affected_rows {
            // 1. Read current values from ALL columns
            let mut current_row = self.read_full_row(table_name, row_idx)?;

            // 2. Apply updates to get new values
            for (column_name, new_value) in &set_clauses {
                let col_ordinal = table.column_ordinal(column_name);
                current_row[col_ordinal] = Entry::new(new_value.clone());
            }

            // 3. Delete old row (from ALL columns)
            let mut delete_updates = Vec::new();
            for col in table.columns() {
                delete_updates.push(ColumnUpdate::new(
                    col.name().to_string(),
                    vec![UpdateOp::Delete { row: row_idx }],
                ));
            }
            self.writer.submit(UpdateJob::new(table_name, delete_updates))?;

            // 4. Insert new row with SortedInsert (to ALL columns)
            let mut insert_updates = Vec::new();
            for (ordinal, col) in table.columns().iter().enumerate() {
                insert_updates.push(ColumnUpdate::new(
                    col.name().to_string(),
                    vec![UpdateOp::SortedInsert {
                        entry: current_row[ordinal].clone(),
                        full_row: current_row.clone(),
                    }],
                ));
            }
            self.writer.submit(UpdateJob::new(table_name, insert_updates))?;
        }

        Ok(())
    }

    fn read_full_row(
        &self,
        table_name: &str,
        row_idx: u64,
    ) -> Result<Vec<Entry>, ExecutorError> {
        let table = self.metadata.get_table(table_name)?;
        let mut row = Vec::new();

        for col in table.columns() {
            // Read entry at row_idx from this column
            let entry = self.read_entry(table_name, col.name(), row_idx)?;
            row.push(entry);
        }

        Ok(row)
    }

    fn read_entry(
        &self,
        table_name: &str,
        column_name: &str,
        row_idx: u64,
    ) -> Result<Entry, ExecutorError> {
        // Get column chain
        let chain = self.metadata.get_column_chain(table_name, column_name)?;

        // Find which page contains this row
        let (page_idx, offset_in_page) = chain.find_page_for_row(row_idx)?;
        let page_descriptor = &chain.pages[page_idx];

        // Load page and extract entry
        let page = self.page_handler.get_page(page_descriptor.clone())?;
        let entry = page.page.entries.get(offset_in_page)
            .ok_or(ExecutorError::RowNotFound)?;

        Ok(entry.clone())
    }
}
```

**Helper in `src/metadata_store/mod.rs`:**

```rust
impl ColumnChain {
    pub fn find_page_for_row(&self, row_idx: u64) -> Result<(usize, usize), Error> {
        for (page_idx, &prefix_count) in self.prefix_entry_counts.iter().enumerate() {
            if row_idx < prefix_count {
                let prev_count = if page_idx > 0 {
                    self.prefix_entry_counts[page_idx - 1]
                } else {
                    0
                };
                let offset_in_page = (row_idx - prev_count) as usize;
                return Ok((page_idx, offset_in_page));
            }
        }
        Err(Error::RowNotFound)
    }
}
```

## Performance Considerations

### 1. SortedInsert Cost

**Single sort key column:**
- Binary search: `O(log n)` where n = entries in page
- Vec insert: `O(n)` worst case (if inserting at beginning)
- **Total: `O(n)` per page**

**Multiple sort key columns:**
- Need full row for comparison
- Pass `full_row` with operation
- Binary search still `O(log n)`
- **Total: `O(n)` per page**

**Mitigation:**
- Pages are typically 1000-2000 entries (not millions)
- Most inserts likely near end (if inserting recent data)
- Consider append-only mode for bulk loads

### 2. UPDATE with Repositioning

**Cost:**
- Read full row: `O(columns * log pages)` to find pages + loads
- Delete: `O(n)` per page (Vec remove)
- Insert: `O(n)` per page (Vec insert)
- **Total: `O(columns * n)` per affected row**

**Mitigation:**
- Batch updates to same row
- Only use repositioning if sort key actually changes
- For non-sort-key updates, use simple `Overwrite`

### 3. DELETE Cost

**Cost:**
- `O(n)` per page per column (Vec remove)
- Update metadata: `O(1)`
- **Total: `O(columns * n)` per affected row**

**Optimization:**
- Batch deletes in single UpdateJob
- Consider tombstone markers instead of immediate removal (trade-off: read-time filtering overhead)

### 4. Read Impact

**No change to read path** - sorted data improves reads:
- Binary search for point queries
- Range scans are sequential
- Filtering is faster

## Edge Cases & Considerations

### 1. Concurrent Writes

**Current architecture:** Single writer thread serializes all writes.
- ✅ No concurrency issues within writer
- ⚠️ Still need transaction semantics at SQL layer

### 2. Large Pages

If pages grow too large (>10K entries):
- Insert/delete becomes expensive
- **Solution:** Implement page splitting (future work)
  - When page exceeds threshold, split into two pages
  - Update `prefix_entry_counts`
  - Requires coordinating across all columns

### 3. NULL Handling

**For unspecified columns in INSERT:**
- Use empty string as NULL placeholder
- **Future:** Add proper NULL support with Entry variant

### 4. Multi-Page Insertion

**Current design:** SortedInsert only works within a page.

**Problem:** What if sorted position is in page 0, but current write is to page 5?

**Solutions:**
1. **Always insert into appropriate existing page** (requires loading/rewriting any page)
2. **Append to end + periodic compaction** (simpler, async)
3. **Force new page to be sorted relative to existing pages**

**Recommendation for Phase 1:** Insert into last page if sorted position would be elsewhere. Mark table for compaction.

### 5. Cross-Page Comparison

**Challenge:** Determining if new entry belongs in page 0 vs page 1.

**Solution:** Store min/max values per page in metadata:

```rust
pub struct PageDescriptor {
    // ... existing fields ...
    pub min_sort_key: Option<Vec<String>>,  // Min values of sort key columns
    pub max_sort_key: Option<Vec<String>>,  // Max values of sort key columns
}
```

Use for binary search across pages.

## Implementation Phases

### Phase 1: Foundation (Week 1)
**Goal:** Enable basic sorted INSERT

- [ ] Create `src/sql/executor.rs` with SqlExecutor
- [ ] Create `src/entry/comparator.rs` with EntryComparator
- [ ] Update `UpdateOp` to include `SortedInsert { entry, full_row }`
- [ ] Implement `execute_insert()` that generates ops for ALL columns
- [ ] Implement simple `binary_search_position()` for single sort key
- [ ] Update `apply_operations()` to handle `SortedInsert`
- [ ] Update metadata to track `prefix_entry_counts` correctly
- [ ] Tests: INSERT into table with single-column ORDER BY

**Acceptance:**
```sql
CREATE TABLE test (id, value) ORDER BY value;
INSERT INTO test (id, value) VALUES (1, 'c');
INSERT INTO test (id, value) VALUES (2, 'a');
INSERT INTO test (id, value) VALUES (3, 'b');
-- Result: Rows sorted as (2, 'a'), (3, 'b'), (1, 'c')
```

### Phase 2: DELETE Support (Week 2)
**Goal:** Enable row deletion

- [ ] Add `UpdateOp::Delete { row }`
- [ ] Implement `execute_delete()` in SqlExecutor
- [ ] Implement filter execution (connect to existing pipeline)
- [ ] Handle `Delete` in `apply_operations()`
- [ ] Update `prefix_entry_counts` after deletion
- [ ] Tests: DELETE with WHERE clause

**Acceptance:**
```sql
DELETE FROM test WHERE id = 2;
-- Row with id=2 removed from ALL columns
```

### Phase 3: UPDATE Support (Week 3)
**Goal:** Enable updates with repositioning

- [ ] Implement `read_full_row()` to materialize rows
- [ ] Implement `ColumnChain::find_page_for_row()`
- [ ] Implement `execute_update_in_place()` for non-sort-key updates
- [ ] Implement `execute_update_with_repositioning()` for sort-key updates
- [ ] Tests: UPDATE both types

**Acceptance:**
```sql
UPDATE test SET id = 99 WHERE id = 1;        -- In-place (id not sort key)
UPDATE test SET value = 'z' WHERE id = 3;    -- Repositioning (value is sort key)
```

### Phase 4: Multi-Column Sort Keys (Week 4)
**Goal:** Support composite ORDER BY

- [ ] Extend `EntryComparator` for multi-column comparison
- [ ] Update `SortedInsert` logic to use full comparator
- [ ] Tests: ORDER BY col1, col2

**Acceptance:**
```sql
CREATE TABLE test (id, age, name) ORDER BY age, name;
-- Inserts sorted first by age, then by name
```

### Phase 5: Optimization (Week 5+)
**Future improvements:**

- [ ] Page splitting when pages grow too large
- [ ] Cross-page insertion (find correct page, not just last page)
- [ ] Min/max metadata for page-level filtering
- [ ] Tombstone markers for lazy deletion
- [ ] Bulk insert optimization (sort batch, then insert)
- [ ] Compaction process for fragmented tables

## Testing Strategy

### Unit Tests

```rust
// src/entry/comparator.rs
#[test]
fn test_compare_numeric() {
    let e1 = Entry::new("10");
    let e2 = Entry::new("100");
    assert!(comparator.compare_entries(&e1, &e2) == Ordering::Less);
}

#[test]
fn test_compare_multi_column() {
    let row1 = vec![Entry::new("30"), Entry::new("Alice")];
    let row2 = vec![Entry::new("30"), Entry::new("Bob")];
    let comp = EntryComparator::new(vec![0, 1]); // ORDER BY col0, col1
    assert!(comp.compare_rows(&row1, &row2) == Ordering::Less);
}
```

### Integration Tests

```rust
// tests/sorted_operations.rs
#[test]
fn test_sorted_insert() {
    let db = setup_db();
    db.execute("CREATE TABLE test (id, val) ORDER BY val");
    db.execute("INSERT INTO test VALUES (1, 'z')");
    db.execute("INSERT INTO test VALUES (2, 'a')");
    db.execute("INSERT INTO test VALUES (3, 'm')");

    let rows = db.query("SELECT * FROM test");
    assert_eq!(rows, vec![
        (2, "a"),
        (3, "m"),
        (1, "z"),
    ]);
}

#[test]
fn test_update_repositioning() {
    let db = setup_db();
    db.execute("CREATE TABLE test (id, val) ORDER BY val");
    db.execute("INSERT INTO test VALUES (1, 'b')");
    db.execute("INSERT INTO test VALUES (2, 'd')");

    db.execute("UPDATE test SET val = 'c' WHERE id = 2");

    let rows = db.query("SELECT * FROM test");
    assert_eq!(rows, vec![
        (1, "b"),
        (2, "c"),  // Repositioned between b and d
    ]);
}
```

### Performance Tests

```rust
#[test]
fn bench_sorted_insert_10k() {
    let db = setup_db();
    db.execute("CREATE TABLE test (id, val) ORDER BY val");

    let start = Instant::now();
    for i in 0..10_000 {
        db.execute(&format!("INSERT INTO test VALUES ({}, {})", i, rand()));
    }
    let duration = start.elapsed();

    println!("10K sorted inserts: {:?}", duration);
    assert!(duration < Duration::from_secs(10)); // Reasonable threshold
}
```

## Migration Path

**For existing databases:**

1. **Detection:** Check if table has `sort_key` in metadata
2. **Warning:** On first INSERT/UPDATE/DELETE to sorted table, log warning if data isn't sorted
3. **Compaction:** Provide tool to reorganize existing unsorted tables:
   ```rust
   pub fn compact_table(table_name: &str) -> Result<()> {
       // Read all data, sort, rewrite
   }
   ```

## Open Questions

1. **Page size limits:** Should we enforce max page size? If so, what threshold?
2. **Tombstones vs immediate delete:** Which is better for read performance?
3. **Bulk insert:** Should we detect bulk inserts and use different strategy?
4. **Transaction semantics:** How do DELETE+INSERT compose in UPDATE? Atomic?
5. **Compaction trigger:** When should we trigger automatic compaction?

## Appendix: Alternative Designs Considered

### Alternative 1: Row-Oriented Storage for Sorted Tables
**Idea:** Store sorted tables in row format instead of columnar.
**Rejected:** Defeats purpose of columnar database, bad for analytics.

### Alternative 2: Separate Sorted Index
**Idea:** Keep data unsorted, maintain separate B-tree index on sort key.
**Rejected:** Adds complexity, double storage, doesn't help with range scans on other columns.

### Alternative 3: Immutable Pages with Copy-on-Write
**Idea:** Never modify pages, always create new versions.
**Current approach!** This is what we do - write to new offsets, update metadata.

### Alternative 4: Lazy Sorting
**Idea:** Append everything, sort at read time.
**Rejected:** Too slow for reads, defeats purpose of ORDER BY.

## Conclusion

This design provides a pragmatic path to support sorted operations in a columnar database:

1. **Incremental:** Can be implemented in phases
2. **Performance-conscious:** Optimizes for common cases (append, single-column sort)
3. **Architecturally consistent:** Uses existing CoW page model
4. **Extensible:** Foundation supports future optimizations

The key insight is that **variable-size pages** make sorted operations tractable - we can modify individual pages without cascading rewrites.

Next step: Implement Phase 1 and validate with benchmarks.
