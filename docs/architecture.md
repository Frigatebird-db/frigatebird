# System Architecture

## Overview

Frigatebird is a columnar SQL database implementing a **push-based Volcano execution model** with **morsel-driven parallelism**. Data flows through pipelines where operators push batches downstream via channels, enabling parallel execution across multiple workers.

```
                                    ┌─────────────────────────────────────┐
                                    │           FRIGATEBIRD                    │
                                    │    Columnar Analytical Database     │
                                    └─────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────────────────────────────────┐
    │                                                                                      │
    │   SQL Query                                                                          │
    │       │                                                                              │
    │       ▼                                                                              │
    │   ┌────────┐      ┌──────────┐      ┌───────────────────────────────────────────┐   │
    │   │ Parser │─────▶│ Planner  │─────▶│            Pipeline Builder               │   │
    │   └────────┘      └──────────┘      └───────────────────────────────────────────┘   │
    │                                                         │                            │
    │                                                         ▼                            │
    │                        ┌────────────────────────────────────────────────────────┐   │
    │                        │                  PIPELINE EXECUTOR                     │   │
    │                        │                                                        │   │
    │                        │    ┌─────────┐    ┌─────────┐    ┌─────────┐          │   │
    │                        │    │  Step   │───▶│  Step   │───▶│  Step   │──▶ Out   │   │
    │                        │    │ (root)  │    │         │    │         │          │   │
    │                        │    └─────────┘    └─────────┘    └─────────┘          │   │
    │                        │         │              │              │               │   │
    │                        │         └──────────────┴──────────────┘               │   │
    │                        │              Workers execute steps                     │   │
    │                        └────────────────────────────────────────────────────────┘   │
    │                                                         │                            │
    │                                                         ▼                            │
    │   ┌──────────────────────────────────────────────────────────────────────────────┐  │
    │   │                              PAGE HANDLER                                     │  │
    │   │                                                                               │  │
    │   │    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐   │  │
    │   │    │   Uncompressed  │     │   Compressed    │     │       Disk          │   │  │
    │   │    │   Cache (Hot)   │◀───▶│   Cache (Warm)  │◀───▶│     (Cold)          │   │  │
    │   │    └─────────────────┘     └─────────────────┘     └─────────────────────┘   │  │
    │   └──────────────────────────────────────────────────────────────────────────────┘  │
    │                                                                                      │
    │   ┌──────────────────────────────────────────────────────────────────────────────┐  │
    │   │                            METADATA STORE                                     │  │
    │   │              Tables • Column Chains • Page Descriptors                        │  │
    │   └──────────────────────────────────────────────────────────────────────────────┘  │
    │                                                                                      │
    └──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## SQL Query Execution Model

### How SQL Maps to Pipeline Steps

Frigatebird compiles SQL queries into pipelines where **each filter column becomes a pipeline step**:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  SQL QUERY                                                                                 │
│  ─────────                                                                                 │
│  SELECT name, age                                                                          │
│  FROM users                                                                                │
│  WHERE age > 25 AND city = 'NYC'                                                           │
│        ────────     ────────────                                                           │
│         filter 1       filter 2                                                            │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  PIPELINE GENERATION                                                                       │
│  ───────────────────                                                                       │
│                                                                                            │
│  Planner extracts:                                                                         │
│    • Table: "users"                                                                        │
│    • Filter columns: [age, city]                                                           │
│    • Projection columns: [name, age]                                                       │
│                                                                                            │
│  Pipeline builder creates:                                                                 │
│                                                                                            │
│    Step 0 (ROOT)          Step 1                 Step 2                                    │
│    ┌─────────────┐        ┌─────────────┐        ┌─────────────┐                          │
│    │ column: age │        │ column: city│        │ column: name│                          │
│    │ filter: >25 │   ───▶ │ filter: =NYC│   ───▶ │ filter: none│   ───▶  Output           │
│    │ root: true  │        │ root: false │        │ root: false │                          │
│    └─────────────┘        └─────────────┘        └─────────────┘                          │
│                                                                                            │
│    Scans all pages        Loads city for         Loads name for                           │
│    for age column,        rows that passed       rows that passed                          │
│    creates batches        age filter             both filters                              │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Query Execution Stages

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│                         QUERY EXECUTION STAGES                                             │
│                                                                                            │
│   ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐      │
│   │           │    │           │    │           │    │           │    │           │      │
│   │   PARSE   │───▶│   PLAN    │───▶│   BUILD   │───▶│  EXECUTE  │───▶│  COLLECT  │      │
│   │           │    │           │    │           │    │           │    │           │      │
│   └───────────┘    └───────────┘    └───────────┘    └───────────┘    └───────────┘      │
│        │                │                │                │                │              │
│        ▼                ▼                ▼                ▼                ▼              │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐    │
│   │                                                                                 │    │
│   │  SQL text      QueryPlan         Job              ColumnarBatch    Result rows  │    │
│   │     │          {filters,         {steps,          {columns,                     │    │
│   │     ▼           columns}          channels}        row_ids}                     │    │
│   │  AST                                                                            │    │
│   │                                                                                 │    │
│   └─────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                            │
│   PARSE:   sqlparser crate converts SQL string to AST                                      │
│   PLAN:    Extract table access patterns, filters, projections                             │
│   BUILD:   Create pipeline steps, wire channels                                            │
│   EXECUTE: Workers process morsels through steps in parallel                               │
│   COLLECT: Gather results from output channel                                              │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Supported Query Types

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│                           CURRENTLY SUPPORTED                                              │
│                                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                                      │ │
│  │  SELECT queries with:                                                                │ │
│  │    • Column projections          SELECT col1, col2, ...                              │ │
│  │    • WHERE filters               WHERE col > value                                   │ │
│  │    • Multiple predicates         AND, OR combinations                                │ │
│  │    • Comparison operators        =, !=, <, >, <=, >=                                 │ │
│  │    • Pattern matching            LIKE 'pattern%'                                     │ │
│  │    • List membership             IN (val1, val2, ...)                                │ │
│  │    • NULL checks                 IS NULL, IS NOT NULL                                │ │
│  │                                                                                      │ │
│  │  INSERT, UPDATE, DELETE          Basic row operations                                │ │
│  │                                                                                      │ │
│  │  CREATE TABLE                    With column types and sort keys                     │ │
│  │                                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                            │
│                              PIPELINE FOCUS                                                │
│                                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                                      │ │
│  │  The push-based pipeline is optimized for:                                           │ │
│  │                                                                                      │ │
│  │    • SCAN + FILTER workloads     Columnar scan with predicate pushdown               │ │
│  │    • High selectivity filters    Late materialization shines when filters reduce     │ │
│  │    • Parallel scanning           Morsel-driven parallelism across pages              │ │
│  │    • Large tables                Streaming execution, bounded memory                 │ │
│  │                                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Step-by-Step: What Happens When You Run a Query

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│  SELECT name FROM users WHERE age > 25                                                     │
│                                                                                            │
│  ══════════════════════════════════════════════════════════════════════════════════════    │
│                                                                                            │
│  STEP 1: ROOT STEP SCANS                                                                   │
│                                                                                            │
│    Table "users" has 200k rows across 4 page groups (morsels)                              │
│                                                                                            │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐    │
│    │                                                                                 │    │
│    │    Page Group 0      Page Group 1      Page Group 2      Page Group 3           │    │
│    │    ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐          │    │
│    │    │ 50k rows │      │ 50k rows │      │ 50k rows │      │ 50k rows │          │    │
│    │    │          │      │          │      │          │      │          │          │    │
│    │    │ age data │      │ age data │      │ age data │      │ age data │          │    │
│    │    │ only     │      │ only     │      │ only     │      │ only     │          │    │
│    │    └──────────┘      └──────────┘      └──────────┘      └──────────┘          │    │
│    │         │                 │                 │                 │                │    │
│    │         ▼                 ▼                 ▼                 ▼                │    │
│    │    ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐          │    │
│    │    │ Bitmap   │      │ Bitmap   │      │ Bitmap   │      │ Bitmap   │          │    │
│    │    │ filter   │      │ filter   │      │ filter   │      │ filter   │          │    │
│    │    │ age > 25 │      │ age > 25 │      │ age > 25 │      │ age > 25 │          │    │
│    │    └──────────┘      └──────────┘      └──────────┘      └──────────┘          │    │
│    │         │                 │                 │                 │                │    │
│    │         ▼                 ▼                 ▼                 ▼                │    │
│    │      12k pass          14k pass          11k pass          13k pass            │    │
│    │                                                                                 │    │
│    └─────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                            │
│  STEP 2: PROJECTION STEP MATERIALIZES                                                      │
│                                                                                            │
│    Now load "name" column, but ONLY for the 50k rows that passed (not all 200k)            │
│                                                                                            │
│    ┌─────────────────────────────────────────────────────────────────────────────────┐    │
│    │                                                                                 │    │
│    │    For each surviving row_id:                                                   │    │
│    │                                                                                 │    │
│    │      1. Determine which page group: page_group = row_id / 50000                 │    │
│    │      2. Load that name page (if not cached)                                     │    │
│    │      3. Extract: name_page.data[row_id % 50000]                                 │    │
│    │                                                                                 │    │
│    │    Only 50k name values loaded instead of 200k                                  │    │
│    │    ═══════════════════════════════════════════                                  │    │
│    │    75% I/O savings from late materialization                                    │    │
│    │                                                                                 │    │
│    └─────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                            │
│  RESULT: 50k rows with (name) returned                                                     │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Push-Based Volcano Model

Traditional Volcano uses **pull-based** iteration where the root operator pulls tuples from children. Frigatebird inverts this with a **push-based** model:

```
    ┌─────────────────────────────────────────────────────────────────────────────────────┐
    │                                                                                     │
    │   PULL-BASED (Traditional Volcano)         PUSH-BASED (Frigatebird)                      │
    │                                                                                     │
    │        Output                                    Root Step                          │
    │           │                                          │                              │
    │       ◀───┤ next()                              PUSH │                              │
    │           │                                          ▼                              │
    │       Operator                               ════════════════                       │
    │           │                                      Channel                            │
    │       ◀───┤ next()                           ════════════════                       │
    │           │                                          │                              │
    │       Operator                                  PUSH │                              │
    │           │                                          ▼                              │
    │       ◀───┤ next()                                Step 1                            │
    │           │                                          │                              │
    │        Source                                   PUSH │                              │
    │                                                      ▼                              │
    │   "Give me the next tuple"               ════════════════                           │
    │   Control flows UP                           Channel                                │
    │   Data flows DOWN                        ════════════════                           │
    │                                                      │                              │
    │                                                 PUSH │                              │
    │                                                      ▼                              │
    │                                                   Output                            │
    │                                                                                     │
    │                                          "Here's the next batch"                    │
    │                                          Control flows DOWN                         │
    │                                          Data flows DOWN                            │
    │                                                                                     │
    └─────────────────────────────────────────────────────────────────────────────────────┘
```

### Why Push-Based?

| Aspect | Pull-Based | Push-Based (Frigatebird) |
|--------|-----------|---------------------|
| Control flow | Consumer requests data | Producer sends data |
| Blocking | Consumer blocks waiting | Producer works continuously |
| Parallelism | Hard (who pulls?) | Natural (each step independent) |
| Batching | Awkward | Natural (push full batches) |

---

## Pipeline Architecture

A query compiles into a **Job** containing multiple **PipelineSteps** connected by channels:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                          JOB                                             │
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐│
│  │                                                                                     ││
│  │   ┌───────────────┐         ┌───────────────┐         ┌───────────────┐            ││
│  │   │               │         │               │         │               │            ││
│  │   │   ROOT STEP   │         │    STEP 1     │         │    STEP 2     │            ││
│  │   │               │         │               │         │               │            ││
│  │   │  ┌─────────┐  │         │  ┌─────────┐  │         │  ┌─────────┐  │            ││
│  │   │  │  Scan   │  │         │  │  Load   │  │         │  │  Load   │  │            ││
│  │   │  │  Pages  │  │         │  │ Column  │  │         │  │ Column  │  │            ││
│  │   │  └────┬────┘  │         │  └────┬────┘  │         │  └────┬────┘  │            ││
│  │   │       │       │         │       │       │         │       │       │            ││
│  │   │       ▼       │         │       ▼       │         │       ▼       │            ││
│  │   │  ┌─────────┐  │         │  ┌─────────┐  │         │  ┌─────────┐  │            ││
│  │   │  │ Filter  │  │         │  │ Filter  │  │         │  │ Filter  │  │            ││
│  │   │  └────┬────┘  │         │  └────┬────┘  │         │  └────┬────┘  │            ││
│  │   │       │       │         │       │       │         │       │       │            ││
│  │   └───────┼───────┘         └───────┼───────┘         └───────┼───────┘            ││
│  │           │                         │                         │                    ││
│  │           │    ┌───────────────┐    │    ┌───────────────┐    │                    ││
│  │           └───▶│    Channel    │────┴───▶│    Channel    │────┴───▶  Output        ││
│  │                │  (unbounded)  │         │  (unbounded)  │         Receiver        ││
│  │                └───────────────┘         └───────────────┘                         ││
│  │                                                                                     ││
│  │   ColumnarBatch    ═══════▶     ColumnarBatch    ═══════▶     ColumnarBatch        ││
│  │   {age: [...]}                  {age, name}                   {age, name, id}      ││
│  │                                                                                     ││
│  └─────────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                          │
│   cost: 3 (number of steps)                                                              │
│   next_free_slot: AtomicUsize (for work-stealing)                                        │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Channel Wiring

Each step's output channel becomes the next step's input:

```
    attach_channels() builds this chain:

    Step 0                    Step 1                    Step 2
    ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
    │                  │      │                  │      │                  │
    │  previous_rx: ─  │      │  previous_rx: ◀──│──────│─ current_tx      │
    │                  │      │                  │      │                  │
    │  current_tx: ────│──────│─▶ previous_rx    │      │  current_tx: ────│───▶ output
    │                  │      │                  │      │                  │
    │  is_root: true   │      │  is_root: false  │      │  is_root: false  │
    │                  │      │                  │      │                  │
    └──────────────────┘      └──────────────────┘      └──────────────────┘

    Data flow:  Step 0  ══▶  Channel  ══▶  Step 1  ══▶  Channel  ══▶  Step 2  ══▶  Output
```

---

## Parallel Execution Model

Workers execute steps using **lock-free work stealing**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              PIPELINE EXECUTOR                                          │
│                                                                                         │
│   ┌──────────────────────────────────────────┐  ┌────────────────────────────────────┐ │
│   │          MAIN POOL (85%)                 │  │       RESERVE POOL (15%)           │ │
│   │                                          │  │                                    │ │
│   │   ┌────────┐ ┌────────┐ ┌────────┐      │  │   ┌────────┐                       │ │
│   │   │Worker 1│ │Worker 2│ │Worker 3│ ...  │  │   │Worker R│ ...                   │ │
│   │   └───┬────┘ └───┬────┘ └───┬────┘      │  │   └───┬────┘                       │ │
│   │       │          │          │            │  │       │                            │ │
│   └───────┼──────────┼──────────┼────────────┘  └───────┼────────────────────────────┘ │
│           │          │          │                       │                              │
│           │          │          │     wake signal       │                              │
│           │          │          │ ─────────────────────▶│                              │
│           │          │          │                       │                              │
│           ▼          ▼          ▼                       ▼                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                              JOB BOARD (SkipSet)                                 │  │
│   │                                                                                  │  │
│   │    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐              │  │
│   │    │  Job A (cost 5) │   │  Job B (cost 3) │   │  Job C (cost 2) │              │  │
│   │    └─────────────────┘   └─────────────────┘   └─────────────────┘              │  │
│   │                                                                                  │  │
│   │    Reserve workers pick highest-cost jobs when main pool is busy                │  │
│   │                                                                                  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Work Stealing via Atomic CAS

Multiple workers can execute different steps of the same job simultaneously:

```
    Job.get_next() - Lock-free step assignment:

    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                                                                             │
    │   next_free_slot: AtomicUsize                                               │
    │                                                                             │
    │   Worker 1                    Worker 2                    Worker 3          │
    │      │                           │                           │              │
    │      │  CAS(0 → 1) ✓             │                           │              │
    │      │  Execute Step 0           │  CAS(0 → 1) ✗             │              │
    │      │                           │  CAS(1 → 2) ✓             │              │
    │      │                           │  Execute Step 1           │  CAS(1 → 2) ✗│
    │      │                           │                           │  CAS(2 → 3) ✓│
    │      │                           │                           │  Execute 2   │
    │      ▼                           ▼                           ▼              │
    │                                                                             │
    │   Steps execute in PARALLEL across workers                                  │
    │   Each step processes batches SEQUENTIALLY                                  │
    │                                                                             │
    └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Morsel-Driven Execution

Data is processed in **morsels** - batches aligned to page group boundaries:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│   Column "age" on disk:                                                                 │
│                                                                                         │
│   ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐      │
│   │  Page Group 0   │ │  Page Group 1   │ │  Page Group 2   │ │  Page Group 3   │      │
│   │  rows 0-50k     │ │  rows 50k-100k  │ │  rows 100k-150k │ │  rows 150k-200k │      │
│   └────────┬────────┘ └────────┬────────┘ └────────┬────────┘ └────────┬────────┘      │
│            │                   │                   │                   │               │
│            │                   │                   │                   │               │
│            ▼                   ▼                   ▼                   ▼               │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                              ROOT STEP                                           │  │
│   │                                                                                  │  │
│   │   for each page_group:                                                           │  │
│   │       page = page_handler.get_page(descriptor)                                   │  │
│   │       batch = ColumnarBatch::from(page)        ◀── MORSEL                        │  │
│   │       bitmap = evaluate_filters(batch)                                           │  │
│   │       filtered = batch.filter_by_bitmap(bitmap)                                  │  │
│   │       channel.send(filtered)                   ◀── PUSH downstream               │  │
│   │                                                                                  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
│   Each morsel (page group) becomes one ColumnarBatch                                    │
│   Morsels flow through pipeline independently                                           │
│   Different morsels can be in different steps simultaneously                            │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline in Motion

```
    Time ──────────────────────────────────────────────────────────────────────▶

    Step 0 (root):  [Morsel 0]  [Morsel 1]  [Morsel 2]  [Morsel 3]  [done]
                         │           │           │           │
                         ▼           │           │           │
    Step 1:              ░░░░░░  [Morsel 0]  [Morsel 1]  [Morsel 2]  [Morsel 3]
                                     │           │           │           │
                                     ▼           │           │           │
    Step 2:                          ░░░░░░  [Morsel 0]  [Morsel 1]  [Morsel 2] ...

    ░░░░░░ = waiting on channel

    Multiple morsels in flight enables pipeline parallelism
```

---

## Three-Tier Cache Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   PAGE HANDLER                                           │
│                                                                                          │
│   ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│   │ TIER 1: Uncompressed Page Cache                                                   │ │
│   │                                                                                   │ │
│   │   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │ │
│   │   │ColumnarPage │ │ColumnarPage │ │ColumnarPage │ │     ...     │  LRU 1024     │ │
│   │   │  (ready to  │ │             │ │             │ │             │               │ │
│   │   │   query)    │ │             │ │             │ │             │               │ │
│   │   └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘               │ │
│   │         │                                                                        │ │
│   │         │ on_evict: compress and store in Tier 2                                 │ │
│   └─────────┼────────────────────────────────────────────────────────────────────────┘ │
│             ▼                                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│   │ TIER 2: Compressed Page Cache                                                     │ │
│   │                                                                                   │ │
│   │   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │ │
│   │   │  Vec<u8>    │ │  Vec<u8>    │ │  Vec<u8>    │ │     ...     │  LRU 1024     │ │
│   │   │  (lz4)      │ │  (lz4)      │ │  (lz4)      │ │             │               │ │
│   │   └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘               │ │
│   │         │                                                                        │ │
│   │         │ on_evict: write to disk (if dirty)                                     │ │
│   └─────────┼────────────────────────────────────────────────────────────────────────┘ │
│             ▼                                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│   │ TIER 3: Disk Storage                                                              │ │
│   │                                                                                   │ │
│   │   data/                                                                           │ │
│   │   └── users/                                                                      │ │
│   │       ├── age.dat    ────▶  [Page 0][Page 1][Page 2][...]                        │ │
│   │       ├── name.dat   ────▶  [Page 0][Page 1][Page 2][...]                        │ │
│   │       └── email.dat  ────▶  [Page 0][Page 1][Page 2][...]                        │ │
│   │                                                                                   │ │
│   └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Cache Lookup Flow

```
    get_page(page_id)
           │
           ▼
    ┌──────────────────┐
    │ Check Tier 1     │──── HIT ────▶ Return ColumnarPage
    │ (Uncompressed)   │
    └────────┬─────────┘
             │ MISS
             ▼
    ┌──────────────────┐
    │ Check Tier 2     │──── HIT ────▶ Decompress ──▶ Store Tier 1 ──▶ Return
    │ (Compressed)     │
    └────────┬─────────┘
             │ MISS
             ▼
    ┌──────────────────┐
    │ Read from Disk   │──────────────▶ Store Tier 2 ──▶ Decompress ──▶ Store Tier 1 ──▶ Return
    │ (Tier 3)         │
    └──────────────────┘
```

---

## Metadata Organization

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                               TABLE META STORE                                           │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │  tables: HashMap<String, TableCatalog>                                           │   │
│   │                                                                                  │   │
│   │     "users" ──▶ TableCatalog {                                                   │   │
│   │                   columns: [                                                     │   │
│   │                     { name: "id",    ordinal: 0, type: Int64  },                │   │
│   │                     { name: "name",  ordinal: 1, type: String },                │   │
│   │                     { name: "age",   ordinal: 2, type: Int64  },                │   │
│   │                   ],                                                             │   │
│   │                   sort_keys: ["id"],                                             │   │
│   │                   rows_per_page_group: 50000,                                    │   │
│   │                 }                                                                │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │  column_chains: HashMap<String, ColumnChain>                                     │   │
│   │                                                                                  │   │
│   │     "users.age" ──▶ ColumnChain {                                                │   │
│   │                       pages: [                                                   │   │
│   │                         PageDescriptor { id: "users.age.0", offset: 0,    ... },│   │
│   │                         PageDescriptor { id: "users.age.1", offset: 8192, ... },│   │
│   │                         PageDescriptor { id: "users.age.2", offset: 16384,... },│   │
│   │                       ]                                                          │   │
│   │                     }                                                            │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │  page_index: HashMap<String, PageDescriptor>                                     │   │
│   │                                                                                  │   │
│   │     "users.age.0" ──▶ PageDescriptor {                                           │   │
│   │                         id: "users.age.0",                                       │   │
│   │                         disk_path: "data/users/age.dat",                         │   │
│   │                         offset: 0,                                               │   │
│   │                         actual_len: 4096,     // compressed size                 │   │
│   │                         entry_count: 50000,                                      │   │
│   │                         data_type: Int64,                                        │   │
│   │                         stats: { min: 18, max: 95, null_count: 42 },             │   │
│   │                       }                                                          │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Disk I/O Architecture

### io_uring + O_DIRECT (Linux)

On Linux, all disk I/O bypasses the OS page cache using **O_DIRECT** and batches operations via **io_uring**:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              DISK I/O STACK                                          │
│                                                                                      │
│   Traditional I/O                         Frigatebird I/O (Linux)                         │
│   ──────────────                          ──────────────────                         │
│                                                                                      │
│   Application                              Application                               │
│        │                                        │                                    │
│        │ read()                                 │ io_uring submit                    │
│        ▼                                        ▼                                    │
│   ┌─────────────┐                         ┌─────────────┐                           │
│   │  OS Page    │                         │  io_uring   │                           │
│   │   Cache     │  ◀── Extra copy         │    Ring     │  ◀── Batched async        │
│   └──────┬──────┘                         └──────┬──────┘                           │
│          │                                       │                                   │
│          │ page fault                            │ O_DIRECT                          │
│          ▼                                       ▼                                   │
│   ┌─────────────┐                         ┌─────────────┐                           │
│   │    Disk     │                         │    Disk     │                           │
│   └─────────────┘                         └─────────────┘                           │
│                                                                                      │
│   Problems:                                Benefits:                                 │
│   • Double buffering                       • Zero-copy to user buffer                │
│   • Cache pollution                        • Batched syscalls                        │
│   • Unpredictable eviction                 • Predictable memory                      │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Batched Read Pattern

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           BATCHED PAGE READS                                         │
│                                                                                      │
│   Query needs pages: [P0, P1, P2, P3, P4]                                            │
│                                                                                      │
│   ┌───────────────────────────────────────────────────────────────────────────────┐ │
│   │                                                                               │ │
│   │   Step 1: Submit all reads to io_uring                                        │ │
│   │                                                                               │ │
│   │   Submission Queue:                                                           │ │
│   │   ┌────┬────┬────┬────┬────┐                                                 │ │
│   │   │ P0 │ P1 │ P2 │ P3 │ P4 │  ◀── 5 reads, 1 syscall                         │ │
│   │   └────┴────┴────┴────┴────┘                                                 │ │
│   │                                                                               │ │
│   │   ring.submit_and_wait(5)                                                     │ │
│   │                                                                               │ │
│   │   Step 2: Process completions as they arrive                                  │ │
│   │                                                                               │ │
│   │   Completion Queue:                                                           │ │
│   │   ┌────┬────┬────┬────┬────┐                                                 │ │
│   │   │ P2 │ P0 │ P4 │ P1 │ P3 │  ◀── Complete in any order                      │ │
│   │   └────┴────┴────┴────┴────┘                                                 │ │
│   │                                                                               │ │
│   └───────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                      │
│   Latency: max(individual_reads) instead of sum(individual_reads)                    │
│   Syscalls: 1 instead of 5                                                           │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Write Path Architecture

### Sharded Writer with WAL

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              WRITE PATH                                              │
│                                                                                      │
│   INSERT/UPDATE                                                                      │
│        │                                                                             │
│        ▼                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                          WAL (Write-Ahead Log)                               │   │
│   │                                                                              │   │
│   │   Serialize with rkyv → append to per-table topic                            │   │
│   │   (Durability: job is recoverable if crash before commit)                    │   │
│   │                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│        │                                                                             │
│        │ shard_index = fnv_hash(table) % num_shards                                  │
│        ▼                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                         SHARDED WORKERS                                      │   │
│   │                                                                              │   │
│   │   ┌─────────┐   ┌─────────┐   ┌─────────┐                                   │   │
│   │   │ Shard 0 │   │ Shard 1 │   │ Shard 2 │   ...                             │   │
│   │   │ tables: │   │ tables: │   │ tables: │                                   │   │
│   │   │ A, D, G │   │ B, E, H │   │ C, F, I │                                   │   │
│   │   └────┬────┘   └────┬────┘   └────┬────┘                                   │   │
│   │        │             │             │                                         │   │
│   │        └─────────────┴─────────────┘                                         │   │
│   │                      │                                                       │   │
│   └──────────────────────┼───────────────────────────────────────────────────────┘   │
│                          │                                                           │
│                          ▼                                                           │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                     THREE-PHASE COMMIT                                       │   │
│   │                                                                              │   │
│   │   Phase 1: Persist data pages to disk (fsync)                                │   │
│   │   Phase 2: Commit metadata to journal + directory                            │   │
│   │   Phase 3: Update in-memory cache                                            │   │
│   │                                                                              │   │
│   │   ◀── After Phase 2, data is visible to readers                              │   │
│   │                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│        │                                                                             │
│        ▼                                                                             │
│   ACK WAL entry (mark processed)                                                     │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Block Allocation

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           STORAGE LAYOUT                                             │
│                                                                                      │
│   storage/                                                                           │
│   ├── data.00000     ◀── 4GB max per file                                            │
│   ├── data.00001                                                                     │
│   └── ...                                                                            │
│                                                                                      │
│   Within each file:                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                             │   │
│   │   offset 0        4KB       12KB      16KB                    4GB           │   │
│   │   ┌───────────┬──────────┬─────────┬──────────┬─  ─  ─  ─  ─ ─┐            │   │
│   │   │  Page A   │  Page B  │ Page C  │  Page D  │               │            │   │
│   │   │  (4KB)    │  (8KB)   │ (4KB)   │  (256KB) │      ...      │            │   │
│   │   └───────────┴──────────┴─────────┴──────────┴─  ─  ─  ─  ─ ─┘            │   │
│   │                                                                             │   │
│   │   Allocation rules:                                                         │   │
│   │   • All offsets 4KB aligned (O_DIRECT requirement)                          │   │
│   │   • Small pages: round up to 4KB                                            │   │
│   │   • Large pages: 256KB blocks + 4KB-aligned tail                            │   │
│   │   • When file reaches 4GB: rotate to next file                              │   │
│   │                                                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Execution model | Push-based Volcano | Natural parallelism, no pull coordination |
| Data flow | Channels (crossbeam) | Decouples steps, enables pipelining |
| Work distribution | Lock-free CAS | Zero contention, scales with cores |
| Batching unit | Page group (morsel) | Matches storage, good cache locality |
| Filtering | Bitmap-based | Vectorized: 64 rows per CPU instruction |
| Column loading | Lazy (on-demand) | Only materialize what's needed |
| Caching | 3-tier with lifecycle | Balances memory vs I/O |
| Compression | lz4_flex | Fast decompression for hot path |
| Disk I/O | io_uring + O_DIRECT | Batched async, bypass OS cache |
| Write sharding | FNV hash by table | Parallel tables, serialized per-table |
| Durability | WAL + 3-phase commit | Crash recovery, atomic visibility |
| String encoding | Dictionary (auto) | 2x-3x compression for low cardinality |
