# Components

## SQL Engine (`sql/`)

### Query Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│   "SELECT name FROM users WHERE age > 25 AND city = 'NYC'"                              │
│                                                                                         │
│       │                                                                                 │
│       │  sql/parser.rs (sqlparser crate)                                                │
│       ▼                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                              AST                                                 │  │
│   │                                                                                  │  │
│   │   Select {                                                                       │  │
│   │     projection: [Column("name")],                                                │  │
│   │     from: Table("users"),                                                        │  │
│   │     where: BinaryOp(                                                             │  │
│   │       And,                                                                       │  │
│   │       BinaryOp(Gt, Column("age"), Literal(25)),                                  │  │
│   │       BinaryOp(Eq, Column("city"), Literal("NYC"))                               │  │
│   │     )                                                                            │  │
│   │   }                                                                              │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│       │                                                                                 │
│       │  sql/planner.rs                                                                 │
│       ▼                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                           QueryPlan                                              │  │
│   │                                                                                  │  │
│   │   QueryPlan {                                                                    │  │
│   │     table: "users",                                                              │  │
│   │     columns: [                                                                   │  │
│   │       ColumnSpec { name: "age",  ordinal: 2, filters: [Gt(25)]     },            │  │
│   │       ColumnSpec { name: "city", ordinal: 3, filters: [Eq("NYC")] },             │  │
│   │       ColumnSpec { name: "name", ordinal: 1, filters: []          },             │  │
│   │     ],                                                                           │  │
│   │     projections: ["name"],                                                       │  │
│   │   }                                                                              │  │
│   │                                                                                  │  │
│   │   Note: Columns with filters are processed first (predicate pushdown)            │  │
│   │                                                                                  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│       │                                                                                 │
│       │  pipeline/builder.rs                                                            │
│       ▼                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                              Job                                                 │  │
│   │                                                                                  │  │
│   │   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐                     │  │
│   │   │  Step 0     │      │  Step 1     │      │  Step 2     │                     │  │
│   │   │  col: age   │─────▶│  col: city  │─────▶│  col: name  │─────▶ output        │  │
│   │   │  filter: >25│      │  filter: =  │      │  filter: -  │                     │  │
│   │   │  is_root    │      │             │      │             │                     │  │
│   │   └─────────────┘      └─────────────┘      └─────────────┘                     │  │
│   │                                                                                  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Filter Expression Tree

Predicates form a tree that gets evaluated per-batch:

```
    WHERE age > 25 AND (city = 'NYC' OR city = 'LA')

                    ┌─────────┐
                    │   AND   │
                    └────┬────┘
                         │
              ┌──────────┴──────────┐
              │                     │
         ┌────┴────┐          ┌─────┴────┐
         │  Leaf   │          │    OR    │
         │ age > 25│          └────┬─────┘
         └─────────┘               │
                        ┌──────────┴──────────┐
                        │                     │
                   ┌────┴────┐          ┌─────┴────┐
                   │  Leaf   │          │   Leaf   │
                   │city=NYC │          │ city=LA  │
                   └─────────┘          └──────────┘

    FilterExpr::And(
        FilterExpr::Leaf { col: "age", op: Gt, val: 25 },
        FilterExpr::Or(
            FilterExpr::Leaf { col: "city", op: Eq, val: "NYC" },
            FilterExpr::Leaf { col: "city", op: Eq, val: "LA" },
        )
    )
```

---

## Pipeline Executor (`pipeline/`, `executor.rs`)

### PipelineStep Internals

Each step is a self-contained operator:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                                   PipelineStep                                        │
│                                                                                       │
│   ┌───────────────────────────────────────────────────────────────────────────────┐  │
│   │  Configuration                                                                 │  │
│   │                                                                                │  │
│   │    table: String           "users"                                             │  │
│   │    column: String          "age"                                               │  │
│   │    column_ordinal: usize   2                                                   │  │
│   │    filters: Vec<FilterExpr> [Gt(25)]                                           │  │
│   │    is_root: bool           true (first step) / false (subsequent)              │  │
│   │                                                                                │  │
│   └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                       │
│   ┌───────────────────────────────────────────────────────────────────────────────┐  │
│   │  Communication                                                                 │  │
│   │                                                                                │  │
│   │    previous_receiver: Receiver<ColumnarBatch>   ◀── from upstream step         │  │
│   │    current_producer:  Sender<ColumnarBatch>     ──▶ to downstream step         │  │
│   │                                                                                │  │
│   └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                       │
│   ┌───────────────────────────────────────────────────────────────────────────────┐  │
│   │  Resources                                                                     │  │
│   │                                                                                │  │
│   │    page_handler: Arc<PageHandler>    shared access to cache/disk               │  │
│   │                                                                                │  │
│   └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Root Step Execution

The root step scans pages and initiates the data flow:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                             execute_root()                                            │
│                                                                                       │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  1. Get all pages for this column                                            │    │
│   │                                                                              │    │
│   │     descriptors = page_handler.list_pages("users", "age")                    │    │
│   │                                                                              │    │
│   │     ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                             │    │
│   │     │Page 0  │ │Page 1  │ │Page 2  │ │Page 3  │  ...                        │    │
│   │     └────────┘ └────────┘ └────────┘ └────────┘                             │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  2. For each page (morsel):                                                  │    │
│   │                                                                              │    │
│   │     page = page_handler.get_page(descriptor)     // cache or disk            │    │
│   │                                                                              │    │
│   │     ┌──────────────────────────────────────────────────────┐               │    │
│   │     │                    ColumnarPage                       │               │    │
│   │     │                                                       │               │    │
│   │     │   data: ColumnData::Int64([23, 45, 19, 67, 31, ...])  │               │    │
│   │     │   null_bitmap: Bitmap { bits: [...], len: 50000 }     │               │    │
│   │     │   num_rows: 50000                                     │               │    │
│   │     │                                                       │               │    │
│   │     └──────────────────────────────────────────────────────┘               │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  3. Create batch and evaluate filters                                        │    │
│   │                                                                              │    │
│   │     batch = ColumnarBatch {                                                  │    │
│   │       columns: { 2: page },      // ordinal → data                           │    │
│   │       row_ids: [0, 1, 2, ...],   // global row identifiers                   │    │
│   │       num_rows: 50000,                                                       │    │
│   │     }                                                                        │    │
│   │                                                                              │    │
│   │     bitmap = evaluate_filters([age > 25], batch)                             │    │
│   │                                                                              │    │
│   │     ┌──────────────────────────────────────────────────────────────────┐    │    │
│   │     │  Bitmap evaluation (vectorized):                                  │    │    │
│   │     │                                                                   │    │    │
│   │     │  int_data:  [23, 45, 19, 67, 31, 28, 15, 52, ...]                 │    │    │
│   │     │              ×   ✓   ×   ✓   ✓   ✓   ×   ✓                        │    │    │
│   │     │                                                                   │    │    │
│   │     │  bitmap:    [ 0,  1,  0,  1,  1,  1,  0,  1, ...]                 │    │    │
│   │     │                                                                   │    │    │
│   │     │  Packed as u64 words: [0b01011101..., ...]                        │    │    │
│   │     └──────────────────────────────────────────────────────────────────┘    │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  4. Filter batch and PUSH downstream                                         │    │
│   │                                                                              │    │
│   │     filtered = batch.filter_by_bitmap(bitmap)                                │    │
│   │                                                                              │    │
│   │     // Gather only matching rows (compact representation)                    │    │
│   │     filtered.num_rows: 31247 (was 50000)                                     │    │
│   │     filtered.row_ids: [1, 3, 4, 5, 7, ...]  // original positions            │    │
│   │                                                                              │    │
│   │                           │                                                  │    │
│   │     current_producer.send(filtered)                                          │    │
│   │                           │                                                  │    │
│   │                           ▼                                                  │    │
│   │                    ══════════════                                            │    │
│   │                       Channel                                                │    │
│   │                    ══════════════                                            │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  5. Send termination signal                                                  │    │
│   │                                                                              │    │
│   │     current_producer.send(ColumnarBatch { num_rows: 0 })  // empty = done    │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Non-Root Step Execution

Subsequent steps receive batches, enrich with their column, filter, and push:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                            execute_non_root()                                         │
│                                                                                       │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  while let Ok(batch) = previous_receiver.recv()                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  1. Check for termination                                                    │    │
│   │                                                                              │    │
│   │     if batch.num_rows == 0 {                                                 │    │
│   │       current_producer.send(empty_batch);  // propagate termination          │    │
│   │       break;                                                                 │    │
│   │     }                                                                        │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  2. Materialize this step's column (lazy loading)                            │    │
│   │                                                                              │    │
│   │     Incoming batch has: { columns: {2: age_data}, row_ids: [1,3,4,5,7,...] } │    │
│   │                                                                              │    │
│   │     This step needs column 3 (city). Load only the rows we need:             │    │
│   │                                                                              │    │
│   │     ┌──────────────────────────────────────────────────────────────────┐    │    │
│   │     │  For each row_id in batch.row_ids:                                │    │    │
│   │     │    page_group = row_id / rows_per_page_group                      │    │    │
│   │     │    page = page_handler.get_page("users.city.{page_group}")        │    │    │
│   │     │    offset = row_id % rows_per_page_group                          │    │    │
│   │     │    value = page.data.get_string(offset)                           │    │    │
│   │     └──────────────────────────────────────────────────────────────────┘    │    │
│   │                                                                              │    │
│   │     batch.columns.insert(3, city_data)                                       │    │
│   │                                                                              │    │
│   │     Batch now has: { columns: {2: age, 3: city}, row_ids: [...] }            │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  3. Evaluate filters for this column                                         │    │
│   │                                                                              │    │
│   │     bitmap = evaluate_filters([city = 'NYC'], batch)                         │    │
│   │     filtered = batch.filter_by_bitmap(bitmap)                                │    │
│   │                                                                              │    │
│   │     // Further reduces row count                                             │    │
│   │     filtered.num_rows: 8234 (was 31247)                                      │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                          │                                                            │
│                          ▼                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  4. PUSH to next step                                                        │    │
│   │                                                                              │    │
│   │     current_producer.send(filtered)                                          │    │
│   │                                                                              │    │
│   └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Termination Protocol

Empty batch signals end-of-data, propagates through entire pipeline:

```
    Root Step              Step 1                 Step 2               Output
        │                     │                      │                    │
        │  [batch 0]          │                      │                    │
        │─────────────────────▶                      │                    │
        │                     │  [batch 0]           │                    │
        │                     │──────────────────────▶                    │
        │                     │                      │  [batch 0]         │
        │                     │                      │────────────────────▶
        │  [batch 1]          │                      │                    │
        │─────────────────────▶                      │                    │
        │                     │  [batch 1]           │                    │
        │                     │──────────────────────▶                    │
        │                     │                      │  [batch 1]         │
        │                     │                      │────────────────────▶
        │                     │                      │                    │
        │  [EMPTY]  ◀─────── termination signal ───────────────────────  │
        │─────────────────────▶                      │                    │
        │                     │  [EMPTY]             │                    │
        │                     │──────────────────────▶                    │
        │                     │                      │  [EMPTY]           │
        │                     │                      │────────────────────▶
        │                     │                      │                    │
       done                  done                   done               all done
```

---

## ColumnarBatch & ColumnarPage (`sql/runtime/batch.rs`)

### ColumnarBatch Structure

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                               ColumnarBatch                                          │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  columns: HashMap<usize, ColumnarPage>                                         │ │
│   │                                                                                │ │
│   │     ordinal 2 (age)  ──▶  ColumnarPage { data: Int64([...]), ... }            │ │
│   │     ordinal 3 (city) ──▶  ColumnarPage { data: Text({...}), ... }             │ │
│   │     ordinal 1 (name) ──▶  ColumnarPage { data: Text({...}), ... }             │ │
│   │                                                                                │ │
│   │  Sparse: only columns that have been loaded are present                        │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  row_ids: Vec<u64>                                                             │ │
│   │                                                                                │ │
│   │     [1, 3, 4, 5, 7, 12, 15, ...]                                               │ │
│   │                                                                                │ │
│   │  Global row identifiers - tracks which original rows are in this batch         │ │
│   │  Used for: joining columns, tracking lineage, late materialization             │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  num_rows: usize                                                               │ │
│   │                                                                                │ │
│   │     8234                                                                       │ │
│   │                                                                                │ │
│   │  Current row count (decreases as filters are applied)                          │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### ColumnarPage Structure

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              ColumnarPage                                            │
│                                                                                      │
│   struct ColumnarPage {                                                              │
│       page_metadata: String,      // page identifier                                 │
│       data: ColumnData,           // type-specific data (see enum below)             │
│       null_bitmap: Bitmap,        // which rows are null                             │
│       num_rows: usize,            // row count in this page                          │
│   }                                                                                  │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  ColumnData Enum - One Variant Active Per Page                                 │ │
│   │                                                                                │ │
│   │    enum ColumnData {                                                           │ │
│   │        Boolean(Vec<bool>),                                                     │ │
│   │        Int64(Vec<i64>),                                                        │ │
│   │        Float64(Vec<f64>),                                                      │ │
│   │        Timestamp(Vec<i64>),    // epoch millis                                 │ │
│   │        Text(BytesColumn),      // Arrow-style variable-length                  │ │
│   │        Dictionary(DictionaryColumn),  // dictionary-encoded strings            │ │
│   │    }                                                                           │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Example: Int64 Column                                                         │ │
│   │                                                                                │ │
│   │    data: ColumnData::Int64(vec![23, 45, 19, 67, 31, 28, 15, 52, ...])          │ │
│   │    ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐                          │ │
│   │    │  23 │  45 │  19 │  67 │  31 │  28 │  15 │  52 │ ...                      │ │
│   │    └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘                          │ │
│   │                                                                                │ │
│   │    Direct array access: O(1) per element                                       │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Example: Text Column (Arrow-style BytesColumn)                                │ │
│   │                                                                                │ │
│   │    data: ColumnData::Text(BytesColumn { offsets, data })                       │ │
│   │                                                                                │ │
│   │    offsets: Vec<u32>                                                           │ │
│   │    ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐                                │ │
│   │    │  0  │  5  │  8  │ 16  │ 23  │ 28  │ 35  │ ...                            │ │
│   │    └─────┴─────┴─────┴─────┴─────┴─────┴─────┘                                │ │
│   │                                                                                │ │
│   │    data: Vec<u8>                                                               │ │
│   │    ┌─────────────────────────────────────────────────────────────────────┐    │ │
│   │    │ A l i c e B o b C h a r l o t t e D a v i d ...                     │    │ │
│   │    └─────────────────────────────────────────────────────────────────────┘    │ │
│   │      ├─────┤├───┤├───────────────┤├───────┤                                   │ │
│   │        [0]   [1]        [2]          [3]                                       │ │
│   │                                                                                │ │
│   │    Row i = data[offsets[i]..offsets[i+1]]                                      │ │
│   │    Length of row i = offsets[i+1] - offsets[i] (no strlen needed)              │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Null Bitmap                                                                   │ │
│   │                                                                                │ │
│   │    null_bitmap: Bitmap { bits: Vec<u64>, len: usize }                          │ │
│   │                                                                                │ │
│   │    ┌────────────────────────────────────────────────────────────────────┐     │ │
│   │    │ 1111111011111110111111111111111111111111111111111111111111111110... │     │ │
│   │    └────────────────────────────────────────────────────────────────────┘     │ │
│   │      │                                                                        │ │
│   │      └── 0 = NULL, 1 = present                                                │ │
│   │                                                                                │ │
│   │    Packed: 64 rows per u64 word                                                │ │
│   │    Memory: 50,000 rows = 782 u64s = 6.25 KB                                    │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Bitmap Operations

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Bitmap Operations                                       │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Predicate: age > 25                                                           │ │
│   │                                                                                │ │
│   │    ages:    [23, 45, 19, 67, 31, 28, 15, 52]                                   │ │
│   │    result:  [ 0,  1,  0,  1,  1,  1,  0,  1]                                   │ │
│   │                                                                                │ │
│   │    Packed: 0b11101101 (little-endian bit order)                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Combining predicates: bitmap.and(&other)                                      │ │
│   │                                                                                │ │
│   │    age > 25:     [0, 1, 0, 1, 1, 1, 0, 1]                                      │ │
│   │    city = NYC:   [1, 1, 0, 0, 1, 0, 1, 1]                                      │ │
│   │                  ─────────────────────────                                     │ │
│   │    AND result:   [0, 1, 0, 0, 1, 0, 0, 1]                                      │ │
│   │                                                                                │ │
│   │    Single CPU instruction per 64 rows: word1 &= word2                          │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Filtering: batch.filter_by_bitmap(bitmap)                                     │ │
│   │                                                                                │ │
│   │    Before:                                                                     │ │
│   │      row_ids: [0, 1, 2, 3, 4, 5, 6, 7]                                         │ │
│   │      ages:    [23, 45, 19, 67, 31, 28, 15, 52]                                 │ │
│   │      bitmap:  [ 0,  1,  0,  0,  1,  0,  0,  1]                                 │ │
│   │                                                                                │ │
│   │    After (gather selected rows):                                               │ │
│   │      row_ids: [1, 4, 7]                                                        │ │
│   │      ages:    [45, 31, 52]                                                     │ │
│   │      num_rows: 3                                                               │ │
│   │                                                                                │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Metadata Store (`metadata_store/`)

### Table Catalog

```
TableCatalog {
    name: "users",
    columns: [
        ColumnDef { name: "id",    ordinal: 0, data_type: Int64,  nullable: false },
        ColumnDef { name: "name",  ordinal: 1, data_type: String, nullable: false },
        ColumnDef { name: "age",   ordinal: 2, data_type: Int64,  nullable: true  },
        ColumnDef { name: "city",  ordinal: 3, data_type: String, nullable: true  },
        ColumnDef { name: "email", ordinal: 4, data_type: String, nullable: true  },
    ],
    sort_keys: ["id"],           // data sorted by this column
    rows_per_page_group: 50000,  // rows per physical page
}
```

### PageDescriptor

```
PageDescriptor {
    id: "users.age.00042",           // unique identifier
    disk_path: "data/users/age.dat", // file location
    offset: 2097152,                 // byte offset in file (page 42 * ~50KB)
    alloc_len: 65536,                // space allocated
    actual_len: 48234,               // actual compressed size
    entry_count: 50000,              // rows in this page
    data_type: Int64,                // column type
    stats: Some(ColumnStats {
        min: 18,
        max: 95,
        null_count: 127,
    }),
}
```

---

## Writer (`writer/`)

### Sharded Architecture

The writer uses **sharded parallelism** - each table maps to exactly one shard via FNV-1a hash, ensuring writes to the same table are serialized while different tables can be written in parallel.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                               SHARDED WRITER                                         │
│                                                                                      │
│   writer.submit(UpdateJob { table: "users", ... })                                   │
│       │                                                                              │
│       │  shard_index = fnv_hash("users") % num_shards                                │
│       ▼                                                                              │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │    Shard 0              Shard 1              Shard 2              Shard N     │ │
│   │   ┌────────┐           ┌────────┐           ┌────────┐           ┌────────┐  │ │
│   │   │ Worker │           │ Worker │           │ Worker │           │ Worker │  │ │
│   │   │ Thread │           │ Thread │           │ Thread │           │ Thread │  │ │
│   │   └───┬────┘           └───┬────┘           └───┬────┘           └───┬────┘  │ │
│   │       │                    │                    │                    │       │ │
│   │       ▼                    ▼                    ▼                    ▼       │ │
│   │   ╔════════╗           ╔════════╗           ╔════════╗           ╔════════╗  │ │
│   │   ║Channel ║           ║Channel ║           ║Channel ║           ║Channel ║  │ │
│   │   ╚════════╝           ╚════════╝           ╚════════╝           ╚════════╝  │ │
│   │                                                                               │ │
│   │   Tables routed by hash:                                                      │ │
│   │     "users"   → Shard 2                                                       │ │
│   │     "orders"  → Shard 0                                                       │ │
│   │     "products"→ Shard 1                                                       │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Benefits:                                                                          │
│   • Same-table writes serialized (no races)                                          │
│   • Different tables write in parallel                                               │
│   • Work distributed evenly via hash                                                 │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Three-Phase Commit

Every write operation follows a strict three-phase protocol to ensure durability and consistency:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                            THREE-PHASE COMMIT                                        │
│                                                                                      │
│   UpdateJob arrives at worker shard                                                  │
│       │                                                                              │
│       ▼                                                                              │
│   ╔═══════════════════════════════════════════════════════════════════════════════╗ │
│   ║  PHASE 1: PERSIST DATA TO DISK                                                 ║ │
│   ╠═══════════════════════════════════════════════════════════════════════════════╣ │
│   ║                                                                                ║ │
│   ║  for each column in staged_columns:                                            ║ │
│   ║      allocation = allocator.allocate(serialized.len())                         ║ │
│   ║      page_io.write_to_path(allocation.path, allocation.offset, serialized)     ║ │
│   ║      fsync()  ◀── Data is durable on disk                                      ║ │
│   ║                                                                                ║ │
│   ║  If ANY persist fails → abort entire commit (no partial writes)                ║ │
│   ║                                                                                ║ │
│   ╚═══════════════════════════════════════════════════════════════════════════════╝ │
│       │                                                                              │
│       │ All data safely on disk                                                      │
│       ▼                                                                              │
│   ╔═══════════════════════════════════════════════════════════════════════════════╗ │
│   ║  PHASE 2: COMMIT METADATA (Atomic Switch)                                      ║ │
│   ╠═══════════════════════════════════════════════════════════════════════════════╣ │
│   ║                                                                                ║ │
│   ║  metadata_client.commit(table, updates)                                        ║ │
│   ║      │                                                                         ║ │
│   ║      ├── Reserve descriptor IDs                                                ║ │
│   ║      ├── Append to MetaJournal (durability)                                    ║ │
│   ║      └── Register in PageDirectory (visibility)                                ║ │
│   ║                                                                                ║ │
│   ║  After this point: new pages are visible to readers                            ║ │
│   ║                                                                                ║ │
│   ╚═══════════════════════════════════════════════════════════════════════════════╝ │
│       │                                                                              │
│       │ Metadata committed                                                           │
│       ▼                                                                              │
│   ╔═══════════════════════════════════════════════════════════════════════════════╗ │
│   ║  PHASE 3: UPDATE IN-MEMORY CACHE                                               ║ │
│   ╠═══════════════════════════════════════════════════════════════════════════════╣ │
│   ║                                                                                ║ │
│   ║  for each (page, descriptor) in staged:                                        ║ │
│   ║      page.page_metadata = descriptor.id                                        ║ │
│   ║      page_handler.write_back_uncompressed(descriptor.id, page)                 ║ │
│   ║                                                                                ║ │
│   ║  Hot cache now has latest data (avoids re-reading from disk)                   ║ │
│   ║                                                                                ║ │
│   ╚═══════════════════════════════════════════════════════════════════════════════╝ │
│       │                                                                              │
│       ▼                                                                              │
│   ACK WAL entry (mark as processed)                                                  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Row Buffering and Page Group Flushing

For bulk inserts, rows are buffered until a full page group (50k rows) accumulates:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           ROW BUFFERING FLOW                                         │
│                                                                                      │
│   BufferRow operations accumulate in worker:                                         │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  buffered_rows: HashMap<String, Vec<Vec<String>>>                             │ │
│   │                                                                               │ │
│   │    "users" → [                                                                │ │
│   │      ["1", "Alice", "30"],                                                    │ │
│   │      ["2", "Bob", "25"],                                                      │ │
│   │      ["3", "Carol", "35"],                                                    │ │
│   │      ...                                                                      │ │
│   │    ]                                                                          │ │
│   │                                                                               │ │
│   │    When len >= 50,000 (ROWS_PER_PAGE_GROUP):                                  │ │
│   │        flush_page_group(table, rows)                                          │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                          │                                                           │
│                          ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  flush_page_group():                                                          │ │
│   │                                                                               │ │
│   │  1. SORT BY SORT KEY                                                          │ │
│   │     ┌─────────────────────────────────────────────────────────────────────┐  │ │
│   │     │  rows.sort_by(|a, b| compare_by_sort_key_ordinals(a, b))            │  │ │
│   │     │                                                                     │  │ │
│   │     │  Before: [("3","Carol"), ("1","Alice"), ("2","Bob")]                │  │ │
│   │     │  After:  [("1","Alice"), ("2","Bob"), ("3","Carol")]                │  │ │
│   │     └─────────────────────────────────────────────────────────────────────┘  │ │
│   │                                                                               │ │
│   │  2. EXTEND PARTIAL TAIL (if last page not full)                               │ │
│   │     ┌─────────────────────────────────────────────────────────────────────┐  │ │
│   │     │  Last page has 45,000 rows, can fit 5,000 more                      │  │ │
│   │     │  Take first 5,000 sorted rows → append to existing page             │  │ │
│   │     │  Replace page with updated version                                  │  │ │
│   │     └─────────────────────────────────────────────────────────────────────┘  │ │
│   │                                                                               │ │
│   │  3. CREATE NEW PAGE GROUPS (for remaining full chunks)                        │ │
│   │     ┌─────────────────────────────────────────────────────────────────────┐  │ │
│   │     │  while rows.len() >= 50,000:                                        │  │ │
│   │     │      chunk = rows.drain(..50,000)                                   │  │ │
│   │     │      stage_rows_as_new_group(table, columns, chunk)                 │  │ │
│   │     └─────────────────────────────────────────────────────────────────────┘  │ │
│   │                                                                               │ │
│   │  4. HANDLE REMAINDER (partial page group)                                     │ │
│   │     ┌─────────────────────────────────────────────────────────────────────┐  │ │
│   │     │  if rows.len() > 0:                                                 │  │ │
│   │     │      stage_rows_as_new_group(table, columns, rows)                  │  │ │
│   │     │      // Will become partial tail for next flush                     │  │ │
│   │     └─────────────────────────────────────────────────────────────────────┘  │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### WAL Integration and Recovery

The writer integrates with WAL for crash recovery:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           WAL INTEGRATION                                            │
│                                                                                      │
│   WRITE PATH:                                                                        │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   writer.submit(job)                                                          │ │
│   │       │                                                                       │ │
│   │       ├── 1. Serialize job with rkyv                                          │ │
│   │       │      bytes = rkyv::to_bytes::<_, 512>(job)                            │ │
│   │       │                                                                       │ │
│   │       ├── 2. Append to WAL (per-table topic)                                  │ │
│   │       │      wal.append_for_topic(&job.table, &bytes)                         │ │
│   │       │                                                                       │ │
│   │       └── 3. Send to shard worker                                             │ │
│   │              shards[shard_index].tx.send(Job(job))                            │ │
│   │                                                                               │ │
│   │   After successful commit:                                                    │ │
│   │       wal.read_next(table, true)  // ACK and advance                          │ │
│   │       wal.mark_topic_clean(table) // Mark for cleanup                         │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   RECOVERY PATH (on startup):                                                        │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   Writer::new() calls replay_pending_jobs()                                   │ │
│   │       │                                                                       │ │
│   │       ├── for each table in metadata.table_names():                           │ │
│   │       │       if !wal.topic_is_clean(table):                                  │ │
│   │       │           loop:                                                       │ │
│   │       │               entry = wal.read_next(table, true)                      │ │
│   │       │               job = rkyv::deserialize(entry.data)                     │ │
│   │       │               worker_context.handle_job(job)  // Re-apply             │ │
│   │       │           flush_pending(table)                                        │ │
│   │       │           wal.mark_topic_clean(table)                                 │ │
│   │       │                                                                       │ │
│   │   Guarantees:                                                                 │ │
│   │   • Jobs in WAL but not committed → will be replayed                          │ │
│   │   • Jobs committed but not ACKed → safe to replay (idempotent)                │ │
│   │   • Clean topics → no replay needed                                           │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Update Operations

The writer supports multiple operation types:

```rust
enum UpdateOp {
    Overwrite { row: u64, entry: Entry },  // Replace existing row
    Append { entry: Entry },                // Add to end
    InsertAt { row: u64, entry: Entry },    // Insert at position
    BufferRow { row: Vec<String> },         // Buffer for batch flush
}
```

---

## Storage I/O (`page_handler/page_io.rs`)

### io_uring and O_DIRECT (Linux)

On Linux, all disk I/O uses **io_uring** with **O_DIRECT** for maximum throughput:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        IO_URING BATCHED READS                                        │
│                                                                                      │
│   read_batch_from_path(path, offsets: [0, 65536, 131072])                            │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  PHASE 1: Read First 4KB Block (contains metadata + start of data)            │ │
│   │                                                                               │ │
│   │   ┌─────────────────────────────────────────────────────────────────────────┐│ │
│   │   │                         io_uring Ring                                   ││ │
│   │   │                                                                         ││ │
│   │   │   Submission Queue:                                                     ││ │
│   │   │   ┌─────────┬─────────┬─────────┐                                      ││ │
│   │   │   │ Read @0 │Read @64K│Read @128K│  ◀── 3 reads submitted in parallel  ││ │
│   │   │   └─────────┴─────────┴─────────┘                                      ││ │
│   │   │                                                                         ││ │
│   │   │   ring.submit_and_wait(3)  ◀── Single syscall for all 3 reads          ││ │
│   │   │                                                                         ││ │
│   │   │   Completion Queue:                                                     ││ │
│   │   │   ┌─────────┬─────────┬─────────┐                                      ││ │
│   │   │   │ Done: 0 │ Done: 1 │ Done: 2 │                                      ││ │
│   │   │   └─────────┴─────────┴─────────┘                                      ││ │
│   │   │                                                                         ││ │
│   │   └─────────────────────────────────────────────────────────────────────────┘│ │
│   │                                                                               │ │
│   │   Each 4KB block: [64B metadata][data...]                                     │ │
│   │   Extract read_size from metadata → know if more data needed                  │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                          │                                                           │
│                          ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  PHASE 2: Read Remaining Data (if page > 4KB)                                 │ │
│   │                                                                               │ │
│   │   Page 0: total_len = 64 + 3500 = 3564  → fits in first 4KB (no extra read)   │ │
│   │   Page 1: total_len = 64 + 8000 = 8064  → need 4KB more at offset 65536+4096  │ │
│   │   Page 2: total_len = 64 + 12000 = 12064 → need 8KB more at offset 131072+4096│ │
│   │                                                                               │ │
│   │   Submit only the extra reads needed (2 in this example)                      │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                          │                                                           │
│                          ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Assemble Results                                                             │ │
│   │                                                                               │ │
│   │   For each page:                                                              │ │
│   │       data = first_block[64..] + extra_buffer[..]                             │ │
│   │       results.push(PageCacheEntryCompressed { page: data })                   │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### O_DIRECT Requirements

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          O_DIRECT ALIGNMENT                                          │
│                                                                                      │
│   O_DIRECT bypasses OS page cache, reading directly to user buffers.                 │
│   Requirements:                                                                      │
│                                                                                      │
│   • Buffer address must be 4KB aligned                                               │
│   • Read/write offset must be 4KB aligned                                            │
│   • Read/write length must be 4KB aligned                                            │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  AlignedBuffer Implementation:                                                │ │
│   │                                                                               │ │
│   │    fn new(capacity: usize) -> Self {                                          │ │
│   │        let aligned_capacity = capacity.div_ceil(4096) * 4096;                 │ │
│   │        let layout = Layout::from_size_align(aligned_capacity, 4096);          │ │
│   │        let ptr = alloc(layout);  // 4KB-aligned allocation                    │ │
│   │        ptr::write_bytes(ptr, 0, aligned_capacity);  // Zero-init              │ │
│   │    }                                                                          │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Benefits:                                                                          │
│   • No double-buffering (OS cache → user buffer)                                     │
│   • Predictable memory usage (cache is ours to manage)                               │
│   • No page cache pollution from scan workloads                                      │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Block Allocator (`writer/allocator.rs`)

### Direct Block Allocation Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        DIRECT BLOCK ALLOCATOR                                        │
│                                                                                      │
│   Constants:                                                                         │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   ALIGN_4K     = 4096           // O_DIRECT alignment requirement             │ │
│   │   BLOCK_SIZE   = 256 * 1024     // 256KB allocation unit                      │ │
│   │   FILE_MAX     = 4 * 1024^3     // 4GB per data file                          │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Allocation Strategy:                                                               │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   compute_alloc_len(actual_len):                                              │ │
│   │                                                                               │ │
│   │     if actual_len <= 256KB:                                                   │ │
│   │         return round_up_4k(actual_len)    // Small pages: 4KB aligned         │ │
│   │                                                                               │ │
│   │     else:                                                                     │ │
│   │         full_blocks = actual_len / 256KB                                      │ │
│   │         tail = actual_len % 256KB                                             │ │
│   │         return (full_blocks * 256KB) + round_up_4k(tail)                      │ │
│   │                                                                               │ │
│   │   Examples:                                                                   │ │
│   │     actual_len = 1000     → alloc_len = 4096        (4KB)                     │ │
│   │     actual_len = 50000    → alloc_len = 53248       (13 × 4KB)                │ │
│   │     actual_len = 300000   → alloc_len = 262144+49152 (256KB + 12×4KB)         │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   File Layout:                                                                       │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   storage/                                                                    │ │
│   │   ├── data.00000    ◀── First 4GB of allocations                              │ │
│   │   ├── data.00001    ◀── Next 4GB (auto-rotated)                               │ │
│   │   ├── data.00002                                                              │ │
│   │   └── ...                                                                     │ │
│   │                                                                               │ │
│   │   Within each file:                                                           │ │
│   │   ┌────────────────────────────────────────────────────────────────────────┐ │ │
│   │   │ offset 0      │ offset 4096   │ offset 8192   │ ...                    │ │ │
│   │   │ [Page A: 4KB] │ [Page B: 8KB ─────────────────│ [Page C: 4KB] │ ...    │ │ │
│   │   └────────────────────────────────────────────────────────────────────────┘ │ │
│   │                                                                               │ │
│   │   Allocator tracks: { file_id, current_offset }                               │ │
│   │   When current_offset + alloc_len > 4GB: rotate to next file                  │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Prefetch Mechanism (`page_handler/`)

### Background Prefetch Thread

The PageHandler spawns a background thread for speculative page loading:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          PREFETCH ARCHITECTURE                                       │
│                                                                                      │
│   PageHandler::new() spawns prefetch thread:                                         │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   Main Thread                           Prefetch Thread                       │ │
│   │   ────────────                          ───────────────                       │ │
│   │                                                                               │ │
│   │   get_pages_with_prefetch(                                                    │ │
│   │       page_ids: ["p0","p1","p2","p3","p4"],                                   │ │
│   │       k: 2  // fetch first 2 immediately                                      │ │
│   │   )                                                                           │ │
│   │       │                                                                       │ │
│   │       ├── prefetch_tx.send(["p2","p3","p4"])  ────────────▶ prefetch_rx       │ │
│   │       │   (non-blocking)                                         │            │ │
│   │       │                                                          ▼            │ │
│   │       └── get_pages(["p0","p1"])                          process_prefetch()  │ │
│   │           (blocking, returns immediately)                        │            │ │
│   │                                                                  │            │ │
│   │                                                      ┌───────────┴──────────┐ │ │
│   │                                                      │ 1. Dedupe IDs        │ │ │
│   │                                                      │ 2. Filter cached     │ │ │
│   │                                                      │ 3. Batch fetch disk  │ │ │
│   │                                                      │ 4. Decompress all    │ │ │
│   │                                                      │ 5. Store in UPC      │ │ │
│   │                                                      └──────────────────────┘ │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Prefetch Loop:                                                                     │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   loop {                                                                      │ │
│   │       match rx.recv_timeout(1ms) {                                            │ │
│   │           Ok(batch) => {                                                      │ │
│   │               // Drain channel to batch pending requests                      │ │
│   │               while let Ok(more) = rx.try_recv() {                            │ │
│   │                   batch.extend(more);                                         │ │
│   │               }                                                               │ │
│   │               process_prefetch_batch(batch);                                  │ │
│   │           }                                                                   │ │
│   │           Err(Timeout) => continue,                                           │ │
│   │           Err(Disconnected) => break,                                         │ │
│   │       }                                                                       │ │
│   │   }                                                                           │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Benefits:                                                                          │
│   • Query gets first K pages immediately                                             │
│   • Remaining pages loaded in background                                             │ │
│   • By the time query needs page K+1, it's likely cached                             │
│   • Batched I/O reduces syscall overhead                                             │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Dictionary Encoding (`sql/runtime/batch.rs`)

### DictionaryColumn for Low-Cardinality Strings

For columns with few unique values (e.g., country codes, status flags), dictionary encoding dramatically reduces memory:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          DICTIONARY ENCODING                                         │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  struct DictionaryColumn {                                                    │ │
│   │      keys: Vec<u16>,        // Integer references (2 bytes each)              │ │
│   │      values: BytesColumn,   // Unique string dictionary                       │ │
│   │  }                                                                            │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Example: 1 million rows with values ["US", "EU", "AP"]                             │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  WITHOUT Dictionary (BytesColumn):                                            │ │
│   │                                                                               │ │
│   │    offsets: [0, 2, 4, 6, 8, ...]     (4 bytes × 1M = 4MB)                     │ │
│   │    data:    "USEUAPUSEUUS..."        (2 bytes × 1M = 2MB)                     │ │
│   │                                                                               │ │
│   │    Total: ~6MB + overhead                                                     │ │
│   │    Filtering: strcmp() per row                                                │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  WITH Dictionary:                                                             │ │
│   │                                                                               │ │
│   │    values (dictionary):                                                       │ │
│   │      offsets: [0, 2, 4, 6]           (16 bytes)                               │ │
│   │      data:    "USEUAP"               (6 bytes)                                │ │
│   │                                                                               │ │
│   │    keys: [0, 1, 2, 0, 1, 0, ...]     (2 bytes × 1M = 2MB)                     │ │
│   │           │  │  │                                                             │ │
│   │           │  │  └── "AP"                                                      │ │
│   │           │  └───── "EU"                                                      │ │
│   │           └──────── "US"                                                      │ │
│   │                                                                               │ │
│   │    Total: ~2MB                                                                │ │
│   │    Filtering: integer comparison (much faster)                                │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Memory Savings: ~67% reduction                                                     │
│   Filter Speedup: Integer compare vs string compare                                  │
│                                                                                      │
│   Auto-Detection:                                                                    │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │  During page creation from disk:                                              │ │
│   │                                                                               │ │
│   │    let unique_ratio = unique_values.len() / total_rows;                       │ │
│   │                                                                               │ │
│   │    if unique_ratio < 0.5 && unique_values.len() <= u16::MAX {                 │ │
│   │        ColumnData::Dictionary(...)  // Use dictionary                         │ │
│   │    } else {                                                                   │ │
│   │        ColumnData::Text(...)        // Use BytesColumn                        │ │
│   │    }                                                                          │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Column Statistics (`metadata_store/`)

### Per-Page Statistics

Each PageDescriptor stores statistics for query optimization:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          COLUMN STATISTICS                                           │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  struct ColumnStats {                                                         │ │
│   │      min_value: Option<String>,   // Minimum value in page                    │ │
│   │      max_value: Option<String>,   // Maximum value in page                    │ │
│   │      null_count: u64,             // Number of NULL values                    │ │
│   │      kind: ColumnStatsKind,       // Int64 | Float64 | Text                   │ │
│   │  }                                                                            │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Stats Derivation (during write):                                                   │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │  derive_column_stats_from_page(page):                                         │ │
│   │                                                                               │ │
│   │    For Int64:                                                                 │ │
│   │      min_val = values.iter().filter(not_null).min()                           │ │
│   │      max_val = values.iter().filter(not_null).max()                           │ │
│   │                                                                               │ │
│   │    For Text/Dictionary:                                                       │ │
│   │      min_val = values.iter().filter(not_null).min_by(bytes_compare)           │ │
│   │      max_val = values.iter().filter(not_null).max_by(bytes_compare)           │ │
│   │                                                                               │ │
│   │    null_count = null_bitmap.count_ones()                                      │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Potential Use (predicate pruning):                                                 │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │  Query: SELECT * FROM users WHERE age > 50                                    │ │
│   │                                                                               │ │
│   │    Page 0: stats.max = 45  → SKIP (max < 50, no rows can match)               │ │
│   │    Page 1: stats.min = 30, max = 60 → SCAN (might have matches)               │ │
│   │    Page 2: stats.min = 55  → SCAN (all rows match)                            │ │
│   │                                                                               │ │
│   │  Result: Skip entire pages without reading data                               │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Cache System (`cache/`)

### LRU Cache with Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           LRU Cache with Lifecycle                                   │
│                                                                                      │
│   trait CacheLifecycle<T> {                                                          │
│       fn on_evict(&self, id: &str, data: Arc<T>);                                    │
│   }                                                                                  │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  UncompressedCache                                                             │ │
│   │    on_evict: compress → store in CompressedCache                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                          │                                                           │
│                          ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  CompressedCache                                                               │ │
│   │    on_evict: write to disk (if dirty)                                          │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                          │                                                           │
│                          ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │  Disk                                                                          │ │
│   │    Final resting place                                                         │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   This creates automatic data flow: hot data stays uncompressed,                     │
│   warm data gets compressed, cold data goes to disk.                                 │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Operations Handler (`ops_handler/`)

External API for database operations:

```rust
// DDL
create_table_from_plan(plan: &QueryPlan) -> Result<()>

// Point operations
read_row(table: &str, row_id: u64) -> Result<Row>
insert_sorted_row(table: &str, row: Row) -> Result<()>
delete_row(table: &str, row_id: u64) -> Result<()>

// Column operations
upsert_data_into_column(table: &str, col: &str, data: Vec<Value>) -> Result<()>
update_column_entry(table: &str, col: &str, row_id: u64, val: Value) -> Result<()>

// Range operations
range_scan_table_column_entry(
    table: &str,
    column: &str,
    start: Bound<Value>,
    end: Bound<Value>,
) -> Result<Vec<Entry>>
```
