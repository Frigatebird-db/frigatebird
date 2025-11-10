---
title: "Core Data Types"
layout: default
nav_order: 5
---

# Core Data Types

## Entry (`src/entry/mod.rs`)

```rust
pub struct Entry {
    prefix_meta: String,  // reserved
    data: String,         // user payload
    suffix_meta: String,  // reserved
}
```

```
╔══════════════════════════════════════╗
║            Entry                     ║
║  ┌────────────────────────────────┐  ║
║  │ prefix_meta : String           │  ║
║  ├────────────────────────────────┤  ║
║  │ data : String                  │  ║
║  ├────────────────────────────────┤  ║
║  │ suffix_meta : String           │  ║
║  └────────────────────────────────┘  ║
╚══════════════════════════════════════╝
```

`current_epoch_millis()` provides timestamps.

---

## Page (`src/page/mod.rs`)

```rust
pub struct Page {
    page_metadata: String,
    entries: Vec<Entry>,
}
```

`add_entry()` appends to Vec. Serde-compatible, bincode → LZ4 via Compressor.

```
╔═══════════════════════════════════════╗
║  Uncompressed Page Cache (UPC)       ║
╚═══════════╤═══════════════════════════╝
            │ eviction
            ▼
     ┌──────────────┐
     │ Compressor   │  bincode + lz4
     └──────┬───────┘
            │
            ▼
╔═══════════════════════════════════════╗
║  Compressed Page Cache (CPC)          ║
╚═══════════╤═══════════════════════════╝
            │ eviction
            ▼
     ┌──────────────┐
     │  PageIO      │  64B metadata + blob
     └──────┬───────┘
            │
            ▼
╔═══════════════════════════════════════╗
║          Disk Storage                 ║
╚═══════════════════════════════════════╝
```

---

## FilterExpr (`src/sql/models.rs`)

```rust
pub enum FilterExpr {
    Leaf(Expr),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}
```

Recursive filter tree supporting AND/OR composition.

```
And([Leaf(a), Leaf(b)]) + And([Leaf(c)])
    → And([Leaf(a), Leaf(b), Leaf(c)])
```

---

## TableAccess (`src/sql/models.rs`)

```rust
pub struct TableAccess {
    pub table_name: String,
    pub read_columns: BTreeSet<String>,
    pub write_columns: BTreeSet<String>,
    pub filters: Option<FilterExpr>,
}
```

`merge_from(&other)` combines read/write sets and filters. `add_filter(filter)` conjuncts via `FilterExpr::and`.

---

## QueryPlan (`src/sql/models.rs`)

```rust
pub struct QueryPlan {
    pub tables: Vec<TableAccess>,
}
```

---

## CreateTablePlan (`src/sql/models.rs`)

```rust
pub struct CreateTablePlan {
    pub table_name: String,
    pub columns: Vec<ColumnSpec>,
    pub order_by: Vec<String>,
    pub if_not_exists: bool,
}

pub struct ColumnSpec {
    pub name: String,
    pub data_type: String,
}
```

---

## UpdateOp (`src/writer/update_job.rs`)

```rust
pub enum UpdateOp {
    Overwrite { row: u64, entry: Entry },
    Append { entry: Entry },
}
```

**Overwrite** — Replace row N. Auto-extends page with empty entries if `row >= current_length`.

**Append** — Push entry to end.

Auto-extension example:
```
Before:  [e0, e1, e2]           (len = 3)
Op:      Overwrite { row: 5, entry: "X" }
After:   [e0, e1, e2, "", "", "X"]  (len = 6)
```

---

## ColumnUpdate (`src/writer/update_job.rs`)

```rust
pub struct ColumnUpdate {
    pub column: String,
    pub operations: Vec<UpdateOp>,
}
```

---

## UpdateJob (`src/writer/update_job.rs`)

```rust
pub struct UpdateJob {
    pub table: String,
    pub columns: Vec<ColumnUpdate>,
}
```

```
┌────────────────────────────────────┐
│          UpdateJob                 │
│  table: "users"                    │
├────────────────────────────────────┤
│ columns: Vec<ColumnUpdate>         │
│   ┌──────────────────────────────┐ │
│   │ ColumnUpdate                 │ │
│   │  column: "email"             │ │
│   │  operations: [               │ │
│   │    Append { Entry("a@x") }   │ │
│   │    Append { Entry("b@x") }   │ │
│   │  ]                           │ │
│   └──────────────────────────────┘ │
│   ┌──────────────────────────────┐ │
│   │ ColumnUpdate                 │ │
│   │  column: "age"               │ │
│   │  operations: [               │ │
│   │    Overwrite { row:0, "30" } │ │
│   │  ]                           │ │
│   └──────────────────────────────┘ │
└────────────────────────────────────┘
         │
         │ submit()
         ▼
┌─────────────────────────────────────┐
│          Writer                     │
│  Crossbeam Channel (Unbounded)      │
└─────────────────────────────────────┘
```
