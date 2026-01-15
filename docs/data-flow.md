# Data Flow

This document traces data through the system in detail, showing exactly how components interact.

## Complete Query Execution Example

Let's trace `SELECT name, age FROM users WHERE age > 25 AND city = 'NYC'` through every layer.

### Pipeline at a Glance

Before diving into details, here's the complete pipeline for this query:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  SELECT name, age FROM users WHERE age > 25 AND city = 'NYC'                               │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  PIPELINE STRUCTURE (3 steps, filter columns first, then projection columns)               │
│                                                                                            │
│  ┌─────────────────┐        ┌─────────────────┐        ┌─────────────────┐                │
│  │     STEP 0      │        │     STEP 1      │        │     STEP 2      │                │
│  │    (ROOT)       │        │                 │        │                 │                │
│  ├─────────────────┤        ├─────────────────┤        ├─────────────────┤                │
│  │ Column: age     │        │ Column: city    │        │ Column: name    │                │
│  │ Filter: > 25    │        │ Filter: = 'NYC' │        │ Filter: (none)  │                │
│  │                 │        │                 │        │                 │                │
│  │ Scans pages,    │        │ Loads city for  │        │ Loads name for  │                │
│  │ creates batches │        │ surviving rows  │        │ surviving rows  │                │
│  └────────┬────────┘        └────────┬────────┘        └────────┬────────┘                │
│           │                          │                          │                         │
│           │    ┌──────────────┐      │    ┌──────────────┐      │                         │
│           └───▶│   Channel    │──────┴───▶│   Channel    │──────┴───▶  Output             │
│                └──────────────┘           └──────────────┘                                │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  DATA VOLUME AT EACH STAGE                                                                 │
│                                                                                            │
│       187,500 rows          31,247 rows           8,234 rows          8,234 rows           │
│      ┌──────────┐          ┌──────────┐          ┌──────────┐        ┌──────────┐         │
│      │          │          │          │          │          │        │          │         │
│      │  Source  │  ──────▶ │ age > 25 │  ──────▶ │city=NYC  │ ─────▶ │  Output  │         │
│      │          │  filter  │          │  filter  │          │  add   │          │         │
│      └──────────┘   83%    └──────────┘   74%    └──────────┘  name  └──────────┘         │
│                   filtered            filtered                                             │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  LATE MATERIALIZATION BENEFIT                                                              │
│                                                                                            │
│     Without late materialization:  Load ALL 4 columns × 187,500 rows = 750k column values  │
│     With late materialization:     age: 187k + city: 31k + name: 8k  = 226k column values  │
│                                                                                            │
│     Savings: 70% less data loaded from storage                                             │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Parallel Morsel Execution

The pipeline processes data in **morsels** (page groups of ~50k rows). Multiple morsels flow through the pipeline simultaneously across worker threads:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  TABLE: users (187,500 rows = 4 morsels)                                                   │
│                                                                                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                          │
│  │  Morsel 0   │ │  Morsel 1   │ │  Morsel 2   │ │  Morsel 3   │                          │
│  │  rows 0-50k │ │ rows 50k-100k│ │rows 100k-150k│ │rows 150k-187k│                         │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘                          │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  EXECUTION TIMELINE (4 workers)                                                            │
│                                                                                            │
│  Time ───────────────────────────────────────────────────────────────────────────────▶     │
│                                                                                            │
│  Worker 0  ║▓▓▓▓▓▓▓▓▓▓▓║           ║▓▓▓▓▓▓▓▓║        ║▓▓▓▓▓▓║                             │
│            ║  M0:S0   ║           ║ M0:S1  ║        ║M0:S2 ║                              │
│            ║ (age)    ║           ║ (city) ║        ║(name)║                              │
│                                                                                            │
│  Worker 1  ║▓▓▓▓▓▓▓▓▓▓▓║       ║▓▓▓▓▓▓▓▓║    ║▓▓▓▓▓▓║                                    │
│            ║  M1:S0   ║       ║ M1:S1  ║    ║M1:S2 ║                                     │
│            ║ (age)    ║       ║ (city) ║    ║(name)║                                     │
│                                                                                            │
│  Worker 2      ║▓▓▓▓▓▓▓▓▓▓▓║       ║▓▓▓▓▓▓▓▓║        ║▓▓▓▓▓▓║                             │
│                ║  M2:S0   ║       ║ M2:S1  ║        ║M2:S2 ║                              │
│                ║ (age)    ║       ║ (city) ║        ║(name)║                              │
│                                                                                            │
│  Worker 3          ║▓▓▓▓▓▓▓▓║     ║▓▓▓▓▓▓║      ║▓▓▓▓║                                    │
│                    ║ M3:S0 ║     ║M3:S1 ║      ║M3:S2║  (smaller morsel)                  │
│                    ║ (age) ║     ║(city)║      ║(name)                                    │
│                                                                                            │
│  Legend: M0:S0 = Morsel 0, Step 0                                                          │
│                                                                                            │
│  Key observations:                                                                         │
│  • Multiple morsels execute concurrently (M0, M1, M2, M3 all in flight)                    │
│  • Same morsel's steps execute sequentially (M0:S0 → M0:S1 → M0:S2)                        │
│  • Workers grab whatever step is ready (work-stealing)                                     │
│  • Channels buffer batches between steps                                                   │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Work-Stealing Execution Model

Workers grab morsels using lock-free atomic compare-and-swap:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  JOB STATE                                                                                 │
│  ─────────                                                                                 │
│  next_free_slot: AtomicUsize                                                               │
│                                                                                            │
│  Page descriptors (morsels):                                                               │
│  ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐               │
│  │   0    │   1    │   2    │   3    │   4    │   5    │   6    │   7    │               │
│  │ 50k    │ 50k    │ 50k    │ 37.5k  │  ...   │  ...   │  ...   │  ...   │               │
│  └────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘               │
│      ▲                                                                                     │
│      │                                                                                     │
│      └── next_free_slot = 0                                                                │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  WORK-STEALING SEQUENCE                                                                    │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │  T=0:  next_free_slot = 0                                                           │  │
│  │                                                                                     │  │
│  │        Worker 0: CAS(0 → 1) ✓  →  takes morsel 0                                    │  │
│  │        Worker 1: CAS(0 → 1) ✗  →  retry                                             │  │
│  │        Worker 2: CAS(0 → 1) ✗  →  retry                                             │  │
│  │        Worker 3: CAS(0 → 1) ✗  →  retry                                             │  │
│  │                                                                                     │  │
│  │  T=1:  next_free_slot = 1                                                           │  │
│  │                                                                                     │  │
│  │        Worker 1: CAS(1 → 2) ✓  →  takes morsel 1                                    │  │
│  │        Worker 2: CAS(1 → 2) ✗  →  retry                                             │  │
│  │        Worker 3: CAS(1 → 2) ✗  →  retry                                             │  │
│  │                                                                                     │  │
│  │  T=2:  next_free_slot = 2                                                           │  │
│  │                                                                                     │  │
│  │        Worker 2: CAS(2 → 3) ✓  →  takes morsel 2                                    │  │
│  │        Worker 3: CAS(2 → 3) ✗  →  CAS(3 → 4) ✓  →  takes morsel 3                   │  │
│  │                                                                                     │  │
│  │  T=3:  next_free_slot = 4                                                           │  │
│  │                                                                                     │  │
│  │        All workers now processing their morsels...                                  │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │  fn claim_next_slot(&self) -> Option<usize> {                                       │  │
│  │      loop {                                                                         │  │
│  │          let current = self.next_free_slot.load(Ordering::Acquire);                 │  │
│  │          if current >= self.total_morsels {                                         │  │
│  │              return None;  // All work claimed                                      │  │
│  │          }                                                                          │  │
│  │          if self.next_free_slot                                                     │  │
│  │              .compare_exchange(current, current + 1, ...)                           │  │
│  │              .is_ok()                                                               │  │
│  │          {                                                                          │  │
│  │              return Some(current);  // Successfully claimed                         │  │
│  │          }                                                                          │  │
│  │          // CAS failed, another worker got it, retry                                │  │
│  │      }                                                                              │  │
│  │  }                                                                                  │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                            │
│  Benefits:                                                                                 │
│  • Zero lock contention (atomic operations only)                                           │
│  • Fast workers automatically get more work                                                │
│  • No coordinator bottleneck                                                               │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Channel Flow and Buffering

Channels decouple steps and buffer batches when producers outpace consumers:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  CHANNEL BUFFERING BETWEEN STEPS                                                           │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │   Step 0 (fast)              Channel (unbounded)              Step 1 (slow)         │  │
│  │   ─────────────              ──────────────────              ─────────────          │  │
│  │                                                                                     │  │
│  │   Worker produces       ┌────┬────┬────┬────┬────┐         Worker consumes         │  │
│  │   batch every 2ms       │ B4 │ B3 │ B2 │ B1 │ B0 │──────▶  batch every 5ms         │  │
│  │        │                └────┴────┴────┴────┴────┘               │                 │  │
│  │        │                     buffered batches                    │                 │  │
│  │        ▼                                                         ▼                 │  │
│  │   ┌─────────┐                                              ┌─────────┐             │  │
│  │   │ Morsel 0│                                              │ Process │             │  │
│  │   │ age>25  │ ═══════════════════════════════════════════▶ │city=NYC │             │  │
│  │   │ filter  │           send() never blocks                │ filter  │             │  │
│  │   └─────────┘           (unbounded channel)                └─────────┘             │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  MULTI-PRODUCER TO SINGLE STEP                                                             │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │   Multiple workers push to same channel:                                            │  │
│  │                                                                                     │  │
│  │   Worker 0 (M0) ───┐                                                                │  │
│  │                    │                                                                │  │
│  │   Worker 1 (M1) ───┼────▶  Channel 0  ────▶  Any available worker runs Step 1       │  │
│  │                    │       [B0][B1][B2]                                             │  │
│  │   Worker 2 (M2) ───┤                                                                │  │
│  │                    │                                                                │  │
│  │   Worker 3 (M3) ───┘                                                                │  │
│  │                                                                                     │  │
│  │   Channel guarantees:                                                               │  │
│  │   • FIFO order per producer (M0's batches stay in order)                            │  │
│  │   • No ordering between producers (M1's batch may arrive before M0's)               │  │
│  │   • Thread-safe (crossbeam mpmc channels)                                           │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  TERMINATION PROTOCOL                                                                      │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │   How does Step 1 know all morsels from Step 0 are done?                            │  │
│  │                                                                                     │  │
│  │   Step 0 (root):                                                                    │  │
│  │       for each morsel:                                                              │  │
│  │           batch = process(morsel)                                                   │  │
│  │           channel.send(batch)                                                       │  │
│  │                                                                                     │  │
│  │       // After ALL morsels processed:                                               │  │
│  │       channel.send(ColumnarBatch { num_rows: 0 })  ◀── TERMINATION SIGNAL           │  │
│  │                                                                                     │  │
│  │   Step 1 (non-root):                                                                │  │
│  │       loop {                                                                        │  │
│  │           batch = channel.recv()                                                    │  │
│  │           if batch.num_rows == 0 {                                                  │  │
│  │               // Propagate termination downstream                                   │  │
│  │               next_channel.send(ColumnarBatch { num_rows: 0 })                      │  │
│  │               break                                                                 │  │
│  │           }                                                                         │  │
│  │           process(batch)                                                            │  │
│  │       }                                                                             │  │
│  │                                                                                     │  │
│  │   Empty batch (num_rows: 0) cascades through pipeline to signal completion          │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Phase 1: SQL to Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  INPUT: "SELECT name, age FROM users WHERE age > 25 AND city = 'NYC'"                   │
│                                                                                         │
│  ════════════════════════════════════════════════════════════════════════════════════   │
│                                                                                         │
│  STEP 1: Parser (sql/parser.rs)                                                         │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│     sqlparser::parse_sql(sql_string)                                                    │
│           │                                                                             │
│           ▼                                                                             │
│     Statement::Query(Query {                                                            │
│       body: Select {                                                                    │
│         projection: [                                                                   │
│           SelectItem::UnnamedExpr(Expr::Identifier("name")),                            │
│           SelectItem::UnnamedExpr(Expr::Identifier("age")),                             │
│         ],                                                                              │
│         from: [TableWithJoins {                                                         │
│           relation: TableFactor::Table { name: "users" }                                │
│         }],                                                                             │
│         selection: Some(Expr::BinaryOp {                                                │
│           left: Expr::BinaryOp {                                                        │
│             left: Expr::Identifier("age"),                                              │
│             op: Gt,                                                                     │
│             right: Expr::Value(Number("25"))                                            │
│           },                                                                            │
│           op: And,                                                                      │
│           right: Expr::BinaryOp {                                                       │
│             left: Expr::Identifier("city"),                                             │
│             op: Eq,                                                                     │
│             right: Expr::Value(SingleQuotedString("NYC"))                               │
│           }                                                                             │
│         })                                                                              │
│       }                                                                                 │
│     })                                                                                  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  STEP 2: Planner (sql/planner.rs)                                                       │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│     The planner does several key things:                                                │
│                                                                                         │
│     a) SCHEMA LOOKUP                                                                    │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │ page_directory.get_table_catalog("users")                                   │ │
│        │                                                                             │ │
│        │ Returns: TableCatalog {                                                     │ │
│        │   columns: [                                                                │ │
│        │     { name: "id",   ordinal: 0, type: Int64  },                            │ │
│        │     { name: "name", ordinal: 1, type: String },                            │ │
│        │     { name: "age",  ordinal: 2, type: Int64  },                            │ │
│        │     { name: "city", ordinal: 3, type: String },                            │ │
│        │   ]                                                                         │ │
│        │ }                                                                           │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│     b) PREDICATE ANALYSIS                                                               │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │ Extract which columns have filters:                                         │ │
│        │                                                                             │ │
│        │   age  → has filter (> 25)                                                  │ │
│        │   city → has filter (= 'NYC')                                               │ │
│        │   name → no filter (projection only)                                        │ │
│        │                                                                             │ │
│        │ Order columns: filters first (selectivity), then projections                │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│     c) BUILD QUERY PLAN                                                                 │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │ QueryPlan {                                                                 │ │
│        │   plan_type: Select,                                                        │ │
│        │   table: "users",                                                           │ │
│        │   columns: [                                                                │ │
│        │     ColumnSpec {                                                            │ │
│        │       name: "age",                                                          │ │
│        │       ordinal: 2,                                                           │ │
│        │       filters: [FilterExpr::Leaf {                                          │ │
│        │         column_ordinal: 2,                                                  │ │
│        │         op: GreaterThan,                                                    │ │
│        │         value: Value::Int64(25)                                             │ │
│        │       }],                                                                   │ │
│        │     },                                                                      │ │
│        │     ColumnSpec {                                                            │ │
│        │       name: "city",                                                         │ │
│        │       ordinal: 3,                                                           │ │
│        │       filters: [FilterExpr::Leaf {                                          │ │
│        │         column_ordinal: 3,                                                  │ │
│        │         op: Equal,                                                          │ │
│        │         value: Value::String("NYC")                                         │ │
│        │       }],                                                                   │ │
│        │     },                                                                      │ │
│        │     ColumnSpec {                                                            │ │
│        │       name: "name",                                                         │ │
│        │       ordinal: 1,                                                           │ │
│        │       filters: [],  // no filter, just projection                           │ │
│        │     },                                                                      │ │
│        │   ],                                                                        │ │
│        │   projections: ["name", "age"],                                             │ │
│        │ }                                                                           │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  STEP 3: Pipeline Builder (pipeline/builder.rs)                                         │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│     build_pipeline(query_plan, page_handler) → Job                                      │
│                                                                                         │
│     a) CREATE CHANNELS                                                                  │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │                                                                             │ │
│        │  (entry_tx, entry_rx) = unbounded::<ColumnarBatch>()   // input to root     │ │
│        │  (ch0_tx, ch0_rx) = unbounded::<ColumnarBatch>()       // step 0 → step 1   │ │
│        │  (ch1_tx, ch1_rx) = unbounded::<ColumnarBatch>()       // step 1 → step 2   │ │
│        │  (ch2_tx, ch2_rx) = unbounded::<ColumnarBatch>()       // step 2 → output   │ │
│        │                                                                             │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│     b) WIRE STEPS                                                                       │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │                                                                             │ │
│        │  Step 0 (age, root):                                                        │ │
│        │    previous_receiver: entry_rx (unused for root)                            │ │
│        │    current_producer: ch0_tx                                                 │ │
│        │    is_root: true                                                            │ │
│        │    filters: [age > 25]                                                      │ │
│        │                                                                             │ │
│        │  Step 1 (city):                                                             │ │
│        │    previous_receiver: ch0_rx  ◀── receives from step 0                      │ │
│        │    current_producer: ch1_tx                                                 │ │
│        │    is_root: false                                                           │ │
│        │    filters: [city = 'NYC']                                                  │ │
│        │                                                                             │ │
│        │  Step 2 (name):                                                             │ │
│        │    previous_receiver: ch1_rx  ◀── receives from step 1                      │ │
│        │    current_producer: ch2_tx                                                 │ │
│        │    is_root: false                                                           │ │
│        │    filters: []  (none)                                                      │ │
│        │                                                                             │ │
│        │  Output receiver: ch2_rx                                                    │ │
│        │                                                                             │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│     c) RESULTING JOB STRUCTURE                                                          │
│                                                                                         │
│        ┌─────────────────────────────────────────────────────────────────────────────┐ │
│        │                                                                             │ │
│        │                              Job                                            │ │
│        │  ┌─────────────────────────────────────────────────────────────────────┐   │ │
│        │  │                                                                     │   │ │
│        │  │   steps: [Step 0, Step 1, Step 2]                                   │   │ │
│        │  │   next_free_slot: AtomicUsize(0)   // for work-stealing             │   │ │
│        │  │   cost: 3                          // number of steps               │   │ │
│        │  │   output_receiver: ch2_rx                                           │   │ │
│        │  │                                                                     │   │ │
│        │  └─────────────────────────────────────────────────────────────────────┘   │ │
│        │                                                                             │ │
│        │        ┌───────────┐       ┌───────────┐       ┌───────────┐               │ │
│        │        │  Step 0   │       │  Step 1   │       │  Step 2   │               │ │
│        │        │   (age)   │──────▶│  (city)   │──────▶│  (name)   │──────▶ output │ │
│        │        │  is_root  │ ch0   │           │ ch1   │           │ ch2           │ │
│        │        └───────────┘       └───────────┘       └───────────┘               │ │
│        │                                                                             │ │
│        └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase 2: Pipeline Execution

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  EXECUTOR SUBMITS JOB                                                                   │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│     executor.submit(job)                                                                │
│           │                                                                             │
│           ├── Add job to job_board (SkipSet, ordered by cost)                           │
│           │                                                                             │
│           └── Send job reference to main worker channel                                 │
│                                                                                         │
│     Workers compete to execute steps:                                                   │
│                                                                                         │
│     ┌─────────────────────────────────────────────────────────────────────────────────┐│
│     │                                                                                 ││
│     │  Main Worker 1              Main Worker 2              Main Worker 3            ││
│     │       │                          │                          │                   ││
│     │       │ recv(job)                │                          │                   ││
│     │       │                          │                          │                   ││
│     │       ▼                          │                          │                   ││
│     │  job.get_next()                  │                          │                   ││
│     │       │                          │                          │                   ││
│     │       │ CAS(0→1) ✓               │                          │                   ││
│     │       │ got slot 0               │                          │                   ││
│     │       │                          │                          │                   ││
│     │       ▼                          ▼                          │                   ││
│     │  steps[0].execute()         job.get_next()                  │                   ││
│     │  (running...)                    │                          │                   ││
│     │       │                          │ CAS(1→2) ✓               │                   ││
│     │       │                          │ got slot 1               │                   ││
│     │       │                          │                          │                   ││
│     │       │                          ▼                          ▼                   ││
│     │       │                     steps[1].execute()         job.get_next()           ││
│     │       │                     (blocked on ch0...)             │                   ││
│     │       │                          │                          │ CAS(2→3) ✓        ││
│     │       │                          │                          │ got slot 2        ││
│     │       │                          │                          │                   ││
│     │       │                          │                          ▼                   ││
│     │       │                          │                     steps[2].execute()       ││
│     │       │                          │                     (blocked on ch1...)      ││
│     │       │                          │                          │                   ││
│     │                                                                                 ││
│     │  All 3 steps executing in parallel on different workers!                        ││
│     │                                                                                 ││
│     └─────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase 3: Step 0 (Root) Execution Detail

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  STEP 0: execute_root() - Scans "age" column                                            │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  1. GET PAGE CHAIN FROM METADATA                                                   │ │
│  │                                                                                    │ │
│  │     page_handler.page_directory.get_column_chain("users", "age")                   │ │
│  │                                                                                    │ │
│  │     Returns: ColumnChain {                                                         │ │
│  │       pages: [                                                                     │ │
│  │         PageDescriptor { id: "users.age.0", offset: 0,      entry_count: 50000 },  │ │
│  │         PageDescriptor { id: "users.age.1", offset: 65536,  entry_count: 50000 },  │ │
│  │         PageDescriptor { id: "users.age.2", offset: 131072, entry_count: 50000 },  │ │
│  │         PageDescriptor { id: "users.age.3", offset: 196608, entry_count: 37500 },  │ │
│  │       ]                                                                            │ │
│  │     }                                                                              │ │
│  │                                                                                    │ │
│  │     Total rows: 187,500                                                            │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  2. PROCESS EACH PAGE (MORSEL)                                                     │ │
│  │                                                                                    │ │
│  │     for descriptor in column_chain.pages {                                         │ │
│  │                                                                                    │ │
│  │     ════════════════════════════════════════════════════════════════════════════   │ │
│  │     MORSEL 0: PageDescriptor { id: "users.age.0", ... }                            │ │
│  │     ════════════════════════════════════════════════════════════════════════════   │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  2a. FETCH PAGE VIA PAGE HANDLER                                        │   │ │
│  │     │                                                                         │   │ │
│  │     │      page = page_handler.get_page("users.age.0")                        │   │ │
│  │     │                                                                         │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │  PageHandler.get_page(page_id):                               │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │    // Check uncompressed cache first                          │  │   │ │
│  │     │      │    if let Some(page) = uncompressed_cache.get(page_id) {      │  │   │ │
│  │     │      │      return page;  // HOT PATH - already in memory            │  │   │ │
│  │     │      │    }                                                          │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │    // Check compressed cache                                  │  │   │ │
│  │     │      │    if let Some(compressed) = compressed_cache.get(page_id) {  │  │   │ │
│  │     │      │      let page = compressor.decompress(compressed);            │  │   │ │
│  │     │      │      uncompressed_cache.put(page_id, page.clone());           │  │   │ │
│  │     │      │      return page;  // WARM PATH - decompress from memory      │  │   │ │
│  │     │      │    }                                                          │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │    // Fetch from disk                                         │  │   │ │
│  │     │      │    let descriptor = page_locator.locate(page_id);             │  │   │ │
│  │     │      │    let bytes = page_io.read(                                  │  │   │ │
│  │     │      │      descriptor.disk_path,                                    │  │   │ │
│  │     │      │      descriptor.offset,                                       │  │   │ │
│  │     │      │      descriptor.actual_len                                    │  │   │ │
│  │     │      │    );                                                         │  │   │ │
│  │     │      │    compressed_cache.put(page_id, bytes.clone());              │  │   │ │
│  │     │      │    let page = compressor.decompress(bytes);                   │  │   │ │
│  │     │      │    uncompressed_cache.put(page_id, page.clone());             │  │   │ │
│  │     │      │    return page;  // COLD PATH - read from disk                │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     │      Returns: ColumnarPage {                                            │   │ │
│  │     │        data: ColumnData::Int64(vec![23, 45, 19, 67, 31, 28, ...]),      │   │ │
│  │     │        null_bitmap: Bitmap { bits: [0xFFFFFFFF...], len: 50000 },       │   │ │
│  │     │        num_rows: 50000,                                                 │   │ │
│  │     │      }                                                                  │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  2b. CREATE COLUMNAR BATCH                                              │   │ │
│  │     │                                                                         │   │ │
│  │     │      let mut batch = ColumnarBatch::new();                              │   │ │
│  │     │      batch.num_rows = 50000;                                            │   │ │
│  │     │      batch.columns.insert(2, page);  // ordinal 2 = "age"               │   │ │
│  │     │      batch.row_ids = (0..50000).collect();  // [0, 1, 2, ..., 49999]    │   │ │
│  │     │                                                                         │   │ │
│  │     │      Batch state:                                                       │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │  columns: { 2: ColumnarPage(age data) }                       │  │   │ │
│  │     │      │  row_ids: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...]                 │  │   │ │
│  │     │      │  num_rows: 50000                                              │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  2c. EVALUATE FILTER: age > 25                                          │   │ │
│  │     │                                                                         │   │ │
│  │     │      bitmap = evaluate_filter_expr_with_batch(&filter, &batch)          │   │ │
│  │     │                                                                         │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │  Physical evaluation (physical_evaluator.rs):                 │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  let age_col = batch.columns.get(&2).unwrap();                │  │   │ │
│  │     │      │  let int_data = age_col.data.as_int64();                      │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  let mut bitmap = Bitmap::new(batch.num_rows);                │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  for (idx, &value) in int_data.iter().enumerate() {           │  │   │ │
│  │     │      │    if value > 25 {                                            │  │   │ │
│  │     │      │      bitmap.set(idx, true);                                   │  │   │ │
│  │     │      │    }                                                          │  │   │ │
│  │     │      │  }                                                            │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  // Vectorized: actually processes 64 values per loop         │  │   │ │
│  │     │      │  // with bitwise operations                                   │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     │      Result bitmap (50000 bits packed into 782 u64s):                   │   │ │
│  │     │                                                                         │   │ │
│  │     │      ages:   [23, 45, 19, 67, 31, 28, 15, 52, 42, 18, ...]              │   │ │
│  │     │      bitmap: [ 0,  1,  0,  1,  1,  1,  0,  1,  1,  0, ...]              │   │ │
│  │     │                ↑   ↑   ↑   ↑   ↑   ↑   ↑   ↑   ↑   ↑                   │   │ │
│  │     │               23  45  19  67  31  28  15  52  42  18                    │   │ │
│  │     │              ≤25 >25 ≤25 >25 >25 >25 ≤25 >25 >25 ≤25                   │   │ │
│  │     │                                                                         │   │ │
│  │     │      bitmap.count_ones() = 31,247 (out of 50,000)                       │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  2d. FILTER BATCH BY BITMAP                                             │   │ │
│  │     │                                                                         │   │ │
│  │     │      filtered = batch.filter_by_bitmap(&bitmap)                         │   │ │
│  │     │                                                                         │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │  Gather operation:                                            │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  let mut new_batch = ColumnarBatch::new();                    │  │   │ │
│  │     │      │  let mut new_row_ids = Vec::new();                            │  │   │ │
│  │     │      │  let mut new_int_data = Vec::new();                           │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  for idx in bitmap.iter_ones() {                              │  │   │ │
│  │     │      │    new_row_ids.push(batch.row_ids[idx]);                      │  │   │ │
│  │     │      │    new_int_data.push(batch.columns[&2].int_data[idx]);        │  │   │ │
│  │     │      │  }                                                            │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  new_batch.row_ids = new_row_ids;                             │  │   │ │
│  │     │      │  new_batch.columns.insert(2, ColumnarPage::from(new_int_data));│  │   │ │
│  │     │      │  new_batch.num_rows = bitmap.count_ones();                    │  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     │      Filtered batch state:                                              │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │  columns: { 2: ColumnarPage([45, 67, 31, 28, 52, 42, ...]) }  │  │   │ │
│  │     │      │  row_ids: [1, 3, 4, 5, 7, 8, ...]  // original positions     │  │   │ │
│  │     │      │  num_rows: 31247                                              │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  2e. PUSH TO DOWNSTREAM CHANNEL                                         │   │ │
│  │     │                                                                         │   │ │
│  │     │      current_producer.send(filtered)                                    │   │ │
│  │     │                                                                         │   │ │
│  │     │      ┌───────────────────────────────────────────────────────────────┐  │   │ │
│  │     │      │                                                               │  │   │ │
│  │     │      │  Step 0                           Channel 0                   │  │   │ │
│  │     │      │     │                                 │                       │  │   │ │
│  │     │      │     │  ColumnarBatch                  │                       │  │   │ │
│  │     │      │     │  {                              │                       │  │   │ │
│  │     │      │     │    columns: {2: [45,67,31,...]} │                       │  │   │ │
│  │     │      │     │    row_ids: [1,3,4,5,7,8,...]   │                       │  │   │ │
│  │     │      │     │    num_rows: 31247              │                       │  │   │ │
│  │     │      │     │  }                              │                       │  │   │ │
│  │     │      │     │                                 │                       │  │   │ │
│  │     │      │     └────────────────────────────────▶│                       │  │   │ │
│  │     │      │                                       │                       │  │   │ │
│  │     │      │                                       │      Step 1 waiting   │  │   │ │
│  │     │      │                                       │◀─────on recv()        │  │   │ │
│  │     │      │                                       │                       │  │   │ │
│  │     │      └───────────────────────────────────────────────────────────────┘  │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     [Repeat for morsels 1, 2, 3...]                                                │ │
│  │                                                                                    │ │
│  │     ════════════════════════════════════════════════════════════════════════════   │ │
│  │     AFTER ALL MORSELS: Send termination signal                                     │ │
│  │     ════════════════════════════════════════════════════════════════════════════   │ │
│  │                                                                                    │ │
│  │     current_producer.send(ColumnarBatch { num_rows: 0, ... })                      │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase 4: Step 1 Execution Detail

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  STEP 1: execute_non_root() - Filters on "city" column                                  │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│  while let Ok(batch) = previous_receiver.recv() {                                       │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  RECEIVED BATCH FROM STEP 0:                                                       │ │
│  │                                                                                    │ │
│  │    columns: { 2: ColumnarPage(age data) }     // only "age" column present         │ │
│  │    row_ids: [1, 3, 4, 5, 7, 8, 12, 15, ...]   // survivors from age filter         │ │
│  │    num_rows: 31247                                                                 │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  1. MATERIALIZE "city" COLUMN FOR THESE ROWS                                       │ │
│  │                                                                                    │ │
│  │     This step needs column 3 (city), but batch only has column 2 (age).            │ │
│  │     Must load city data for ONLY the row_ids in this batch.                        │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  Late materialization process:                                          │   │ │
│  │     │                                                                         │   │ │
│  │     │  // Group row_ids by page group for efficient I/O                       │   │ │
│  │     │  let rows_per_page = 50000;                                             │   │ │
│  │     │                                                                         │   │ │
│  │     │  row_ids: [1, 3, 4, 5, 7, 8, 12, 15, ...]                               │   │ │
│  │     │            └──────────────────────────┘                                 │   │ │
│  │     │                 all in page group 0                                     │   │ │
│  │     │                 (row_id / 50000 = 0)                                    │   │ │
│  │     │                                                                         │   │ │
│  │     │  // Fetch page for city column                                          │   │ │
│  │     │  city_page = page_handler.get_page("users.city.0")                      │   │ │
│  │     │                                                                         │   │ │
│  │     │  // Extract only the needed values                                      │   │ │
│  │     │  let mut city_data = BytesColumn::new();                                │   │ │
│  │     │  for &row_id in &batch.row_ids {                                        │   │ │
│  │     │    let offset = row_id % rows_per_page;                                 │   │ │
│  │     │    let city = city_page.data.get_string(offset);                         │   │ │
│  │     │    city_data.push(city);                                                │   │ │
│  │     │  }                                                                      │   │ │
│  │     │                                                                         │   │ │
│  │     │  batch.columns.insert(3, ColumnarPage::from_strings(city_data));        │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  │     Batch state after materialization:                                             │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  columns: {                                                             │   │ │
│  │     │    2: ColumnarPage(ages:  [45, 67, 31, 28, 52, 42, ...]),               │   │ │
│  │     │    3: ColumnarPage(cities: ["NYC", "LA", "NYC", "CHI", "NYC", ...]),    │   │ │
│  │     │  }                                                                      │   │ │
│  │     │  row_ids: [1, 3, 4, 5, 7, 8, ...]                                       │   │ │
│  │     │  num_rows: 31247                                                        │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  2. EVALUATE FILTER: city = 'NYC'                                                  │ │
│  │                                                                                    │ │
│  │     bitmap = evaluate_filter_expr_with_batch(&filter, &batch)                      │ │
│  │                                                                                    │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  String comparison:                                                     │   │ │
│  │     │                                                                         │   │ │
│  │     │  cities: ["NYC", "LA", "NYC", "CHI", "NYC", "SF", ...]                  │   │ │
│  │     │  bitmap: [  1,    0,    1,     0,     1,    0,  ...]                    │   │ │
│  │     │                                                                         │   │ │
│  │     │  bitmap.count_ones() = 8,234 (out of 31,247)                            │   │ │
│  │     │                                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  3. FILTER BATCH AND PUSH DOWNSTREAM                                               │ │
│  │                                                                                    │ │
│  │     filtered = batch.filter_by_bitmap(&bitmap)                                     │ │
│  │     current_producer.send(filtered)                                                │ │
│  │                                                                                    │ │
│  │     Pushed to Step 2:                                                              │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  columns: {                                                             │   │ │
│  │     │    2: ColumnarPage(ages:   [45, 31, 52, ...]),  // only NYC rows        │   │ │
│  │     │    3: ColumnarPage(cities: ["NYC", "NYC", "NYC", ...]),                 │   │ │
│  │     │  }                                                                      │   │ │
│  │     │  row_ids: [1, 4, 7, ...]  // original row positions                     │   │ │
│  │     │  num_rows: 8234                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase 5: Step 2 Execution (Final Projection)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  STEP 2: execute_non_root() - Materializes "name" column (final projection)             │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  1. RECEIVE BATCH FROM STEP 1                                                      │ │
│  │                                                                                    │ │
│  │     columns: { 2: ages, 3: cities }   // has age and city                          │ │
│  │     row_ids: [1, 4, 7, 23, 45, ...]   // survivors of both filters                 │ │
│  │     num_rows: 8234                                                                 │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  2. MATERIALIZE "name" COLUMN (ordinal 1)                                          │ │
│  │                                                                                    │ │
│  │     // No filter on this step, just add the column                                 │ │
│  │     name_page = page_handler.get_page("users.name.0")                              │ │
│  │                                                                                    │ │
│  │     for &row_id in &batch.row_ids {                                                │ │
│  │       let offset = row_id % rows_per_page;                                         │ │
│  │       let name = name_page.data.get_string(offset);                                 │ │
│  │       name_data.push(name);                                                        │ │
│  │     }                                                                              │ │
│  │                                                                                    │ │
│  │     batch.columns.insert(1, ColumnarPage::from_strings(name_data));                │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                                │
│                                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │  3. NO FILTER - PASS THROUGH TO OUTPUT                                             │ │
│  │                                                                                    │ │
│  │     // Step 2 has no filters, so bitmap is all 1s                                  │ │
│  │     // Just push the enriched batch to output                                      │ │
│  │                                                                                    │ │
│  │     current_producer.send(batch)                                                   │ │
│  │                                                                                    │ │
│  │     Final batch:                                                                   │ │
│  │     ┌─────────────────────────────────────────────────────────────────────────┐   │ │
│  │     │  columns: {                                                             │   │ │
│  │     │    1: ColumnarPage(names:  ["Alice", "Bob", "Carol", ...]),             │   │ │
│  │     │    2: ColumnarPage(ages:   [45, 31, 52, ...]),                          │   │ │
│  │     │    3: ColumnarPage(cities: ["NYC", "NYC", "NYC", ...]),                 │   │ │
│  │     │  }                                                                      │   │ │
│  │     │  row_ids: [1, 4, 7, ...]                                                │   │ │
│  │     │  num_rows: 8234                                                         │   │ │
│  │     └─────────────────────────────────────────────────────────────────────────┘   │ │
│  │                                                                                    │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase 6: Result Collection

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  OUTPUT RECEIVER COLLECTS RESULTS                                                       │
│  ─────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                         │
│     let mut results = Vec::new();                                                       │
│                                                                                         │
│     while let Ok(batch) = output_receiver.recv() {                                      │
│       if batch.num_rows == 0 {                                                          │
│         break;  // termination signal                                                   │
│       }                                                                                 │
│       results.push(batch);                                                              │
│     }                                                                                   │
│                                                                                         │
│     // Apply final projection: SELECT name, age                                         │
│     let projected = results.iter().map(|batch| {                                        │
│       ProjectedBatch {                                                                  │
│         name: batch.columns[&1],  // ordinal 1                                          │
│         age:  batch.columns[&2],  // ordinal 2                                          │
│       }                                                                                 │
│     });                                                                                 │
│                                                                                         │
│     FINAL RESULT:                                                                       │
│     ┌────────────────────────────────────────┐                                          │
│     │  name     │  age                       │                                          │
│     ├────────────────────────────────────────┤                                          │
│     │  Alice    │  45                        │                                          │
│     │  Bob      │  31                        │                                          │
│     │  Carol    │  52                        │                                          │
│     │  Dave     │  38                        │                                          │
│     │  ...      │  ...                       │                                          │
│     │  (8234 total rows)                     │                                          │
│     └────────────────────────────────────────┘                                          │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Summary: Data Flow Through System

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  SQL Query                                                                              │
│      │                                                                                  │
│      ▼                                                                                  │
│  ┌─────────┐     ┌──────────┐     ┌───────────────┐                                    │
│  │ Parser  │────▶│ Planner  │────▶│Pipeline Builder│                                    │
│  └─────────┘     └──────────┘     └───────┬───────┘                                    │
│                                           │                                             │
│                                           ▼                                             │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              PIPELINE EXECUTION                                   │  │
│  │                                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐    │  │
│  │   │                           Worker Threads                                 │    │  │
│  │   │                                                                          │    │  │
│  │   │   Worker 1          Worker 2          Worker 3                           │    │  │
│  │   │      │                  │                  │                             │    │  │
│  │   │      ▼                  ▼                  ▼                             │    │  │
│  │   │   Step 0            Step 1            Step 2                             │    │  │
│  │   │   (root)                                                                 │    │  │
│  │   │      │                  │                  │                             │    │  │
│  │   │      │ ColumnarBatch    │ ColumnarBatch    │ ColumnarBatch               │    │  │
│  │   │      │ (morsel 0)       │ (morsel 0)       │ (morsel 0)                  │    │  │
│  │   │      ▼                  ▼                  ▼                             │    │  │
│  │   │   ═══════════       ═══════════       ═══════════                        │    │  │
│  │   │   Channel 0         Channel 1         Channel 2                          │    │  │
│  │   │   ═══════════       ═══════════       ═══════════                        │    │  │
│  │   │                                           │                              │    │  │
│  │   │                                           ▼                              │    │  │
│  │   │                                      Output Receiver                     │    │  │
│  │   │                                                                          │    │  │
│  │   └──────────────────────────────────────────────────────────────────────────┘    │  │
│  │                                                                                   │  │
│  │   ┌──────────────────────────────────────────────────────────────────────────┐   │  │
│  │   │                        PAGE HANDLER                                       │   │  │
│  │   │                                                                           │   │  │
│  │   │   get_page(id) ──▶ [Uncompressed Cache] ──▶ [Compressed Cache] ──▶ [Disk] │   │  │
│  │   │                            │                       │                 │    │   │  │
│  │   │                            │                       │                 │    │   │  │
│  │   │                         HIT: O(1)            HIT: decompress     MISS: I/O│   │  │
│  │   │                                                                           │   │  │
│  │   └──────────────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                                   │  │
│  └──────────────────────────────────────────────────────────────────────────────────┘  │
│                                           │                                             │
│                                           ▼                                             │
│                                      Query Results                                      │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Characteristics

| Property | How It Works |
|----------|--------------|
| **Push-based** | Each step pushes batches downstream via `channel.send()`, no pulling |
| **Morsel-driven** | Each page group (50k rows) becomes one morsel/batch flowing through |
| **Late materialization** | Columns loaded only when needed by a step (not all columns upfront) |
| **Parallel steps** | Workers execute different steps simultaneously via atomic CAS |
| **Vectorized filtering** | Bitmap operations process 64 rows per CPU instruction |
| **Pipeline parallelism** | Multiple morsels in-flight across steps at the same time |
| **Termination** | Empty batch (num_rows=0) propagates end-of-data through all steps |
