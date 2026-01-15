# Satori Architecture Documentation

Satori is a **columnar SQL database** implementing a **push-based Volcano execution model** with **morsel-driven parallelism**.

---

## How SQL Executes in Satori

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│                              SQL EXECUTION: THE BIG PICTURE                                │
│                                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                     │  │
│  │   "SELECT name, age FROM users WHERE age > 25 AND city = 'NYC'"                     │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                          │                                                 │
│                                          ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │  1. PARSE                                                                            │ │
│  │     SQL string → Abstract Syntax Tree (AST)                                          │ │
│  │     Validates syntax, extracts structure                                             │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                                 │
│                                          ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │  2. PLAN                                                                             │ │
│  │     AST → QueryPlan                                                                  │ │
│  │     • Resolves table/column names against catalog                                    │ │
│  │     • Extracts filter predicates                                                     │ │
│  │     • Determines column access patterns                                              │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                                 │
│                                          ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │  3. BUILD PIPELINE                                                                   │ │
│  │     QueryPlan → Job (executable pipeline)                                            │ │
│  │     • Creates one Step per filter column                                             │ │
│  │     • Wires Steps together with channels                                             │ │
│  │     • First filter column becomes "root" step                                        │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                                 │
│                                          ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │  4. EXECUTE                                                                          │ │
│  │     Workers execute pipeline in parallel                                             │ │
│  │                                                                                      │ │
│  │     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐                       │ │
│  │     │   STEP 0    │      │   STEP 1    │      │   STEP 2    │                       │ │
│  │     │   (root)    │      │             │      │             │                       │ │
│  │     │             │      │             │      │             │                       │ │
│  │     │  Scan age   │ ───▶ │ Filter city │ ───▶ │  Add name   │ ───▶ Results          │ │
│  │     │  Filter>25  │      │ Filter=NYC  │      │ (project)   │                       │ │
│  │     │             │      │             │      │             │                       │ │
│  │     │  187k→31k   │      │  31k→8k     │      │    8k       │                       │ │
│  │     └─────────────┘      └─────────────┘      └─────────────┘                       │ │
│  │           │                    │                    │                               │ │
│  │           └────────────────────┴────────────────────┘                               │ │
│  │                    Data flows via channels                                          │ │
│  │                    Multiple morsels in parallel                                     │ │
│  │                                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                          │                                                 │
│                                          ▼                                                 │
│                                      8,234 rows                                            │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### SQL Concepts → Satori Implementation

| SQL Concept | Satori Implementation | Notes |
|-------------|----------------------|-------|
| `FROM table` | Root step scans column pages | Columnar: reads one column at a time |
| `WHERE col = X` | Pipeline step with bitmap filter | Each filter column = one step |
| `AND` / `OR` | Filter tree evaluated per step | Vectorized bitmap operations |
| `SELECT cols` | Final steps materialize columns | Late materialization: only for surviving rows |
| Row groups | Morsels (~50k rows) | Unit of parallelism |

### What Makes Satori Different

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│   TRADITIONAL ROW STORE                      SATORI (COLUMNAR)                             │
│   ─────────────────────                      ─────────────────                             │
│                                                                                            │
│   Read all columns for each row              Read only needed columns                      │
│                                                                                            │
│   ┌─────────────────────────┐                ┌──────┐ ┌──────┐ ┌──────┐                   │
│   │ id │ name │ age │ city  │                │  id  │ │ name │ │ age  │ │ city │          │
│   ├────┼──────┼─────┼───────┤                ├──────┤ ├──────┤ ├──────┤ ├──────┤          │
│   │ 1  │ Alice│ 30  │ NYC   │                │  1   │ │Alice │ │  30  │ │ NYC  │          │
│   │ 2  │ Bob  │ 25  │ LA    │                │  2   │ │ Bob  │ │  25  │ │  LA  │          │
│   │ 3  │ Carol│ 35  │ NYC   │                │  3   │ │Carol │ │  35  │ │ NYC  │          │
│   └─────────────────────────┘                └──────┘ └──────┘ └──────┘ └──────┘          │
│                                                                                            │
│   WHERE age > 25:                            WHERE age > 25:                               │
│   • Read entire rows                         • Read only "age" column                      │
│   • Check age for each                       • Bitmap filter (vectorized)                  │
│   • Discard non-matching                     • Then load other columns                     │
│                                                only for matching rows                      │
│                                                                                            │
│   SELECT name: still read all cols           SELECT name: load "name" only                 │
│                                              for the 8k surviving rows                     │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                      SATORI                                              │
│                                                                                          │
│     SQL Query ──▶ Parser ──▶ Planner ──▶ Pipeline Builder ──▶ Pipeline Executor         │
│                                                                      │                   │
│                                                                      ▼                   │
│     ┌──────────────────────────────────────────────────────────────────────────────┐    │
│     │                            EXECUTION ENGINE                                   │    │
│     │                                                                               │    │
│     │    ┌─────────┐         ┌─────────┐         ┌─────────┐                       │    │
│     │    │  Step   │  PUSH   │  Step   │  PUSH   │  Step   │  ──▶  Results         │    │
│     │    │ (root)  │ ═══════▶│         │ ═══════▶│         │                       │    │
│     │    └─────────┘ Channel └─────────┘ Channel └─────────┘                       │    │
│     │         │                   │                   │                             │    │
│     │         │                   │                   │                             │    │
│     │         └───────────────────┴───────────────────┘                             │    │
│     │                    Workers execute steps in parallel                          │    │
│     │                                                                               │    │
│     └──────────────────────────────────────────────────────────────────────────────┘    │
│                                          │                                               │
│     ┌──────────────────────────────────────────────────────────────────────────────┐    │
│     │                          THREE-TIER PAGE CACHE                                │    │
│     │                                                                               │    │
│     │    Uncompressed (Hot) ◀──▶ Compressed (Warm) ◀──▶ Disk (Cold)                │    │
│     │                                                                               │    │
│     └──────────────────────────────────────────────────────────────────────────────┘    │
│                                          │                                               │
│     ┌──────────────────────────────────────────────────────────────────────────────┐    │
│     │                           METADATA STORE                                      │    │
│     │                                                                               │    │
│     │         Tables ──▶ Column Chains ──▶ Page Descriptors                         │    │
│     │                                                                               │    │
│     └──────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Documentation

| Document | What You'll Learn |
|----------|-------------------|
| **[Architecture](architecture.md)** | Push-based Volcano model, pipeline structure, parallel execution, cache tiers, metadata organization |
| **[Components](components.md)** | SQL engine internals, PipelineStep execution, ColumnarBatch structure, bitmap operations, writer flow |
| **[Data Flow](data-flow.md)** | Complete query trace from SQL to results, step-by-step execution with detailed diagrams |

## Quick Start

Start with **[Architecture](architecture.md)** for the big picture, then **[Data Flow](data-flow.md)** to see how a query actually executes through the system.

## Key Concepts

### Push-Based Volcano
Unlike traditional pull-based Volcano where consumers request data, Satori uses **push-based** execution:
- Producers push batches downstream via channels
- Steps execute in parallel on worker threads
- Natural pipelining of multiple morsels

### Morsel-Driven Parallelism
Data is processed in **morsels** (page groups of ~50k rows):
- Each morsel becomes one `ColumnarBatch`
- Multiple morsels flow through pipeline simultaneously
- Work-stealing via atomic CAS for load balancing

### Late Materialization
Columns are loaded **on-demand**:
- Root step scans only the first filter column
- Subsequent steps materialize their columns for surviving rows only
- Avoids loading data that gets filtered out

### Vectorized Filtering
Predicates evaluated using **bitmaps**:
- 64 rows per CPU instruction (bitwise AND/OR)
- Compact representation (50k rows = 6KB)
- Fast gather operations for surviving rows

## Module Map

```
src/
├── sql/                 # Parser, planner, runtime types
│   ├── parser.rs        # SQL string → AST
│   ├── planner.rs       # AST → QueryPlan
│   └── runtime/
│       ├── batch.rs     # ColumnarBatch, ColumnarPage, Bitmap
│       └── ...
├── pipeline/            # Execution pipeline
│   ├── builder.rs       # QueryPlan → Job with connected steps
│   └── types.rs         # PipelineStep, Job
├── executor.rs          # Thread pools, work-stealing
├── cache/               # LRU caches with lifecycle
├── page_handler/        # Cache orchestration (locate, fetch, materialize)
├── metadata_store/      # Table catalog, column chains, page descriptors
├── writer/              # Insert, update, delete handling
├── ops_handler/         # External API
├── helpers/             # Compression (lz4)
└── pool/                # Thread pool primitives
```
