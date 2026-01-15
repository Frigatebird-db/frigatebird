# Satori Documentation Index

## Quick Navigation

| Doc | What It Covers | Read Time |
|-----|----------------|-----------|
| **[README](README.md)** | Big picture overview, SQL execution model, system diagram | 5 min |
| **[Architecture](architecture.md)** | Push-based Volcano, pipeline structure, parallelism, caching, I/O | 15 min |
| **[Components](components.md)** | Deep dive into each module: steps, batches, writer, cache, storage | 25 min |
| **[Data Flow](data-flow.md)** | Complete query trace with detailed diagrams at every stage | 20 min |

---

## Reading Paths

### "I want to understand how Satori works" (30 min)
```
README.md ──▶ Architecture.md (first half) ──▶ Data Flow (Pipeline at a Glance)
```

### "I need to understand the execution model deeply" (45 min)
```
Architecture.md (full) ──▶ Data Flow.md (full)
```

### "I need to work on the codebase" (60 min)
```
README.md ──▶ Architecture.md ──▶ Components.md ──▶ Data Flow.md
```

---

## Topic Index

### SQL Execution
| Topic | Location |
|-------|----------|
| SQL → Pipeline mapping | [architecture.md#sql-query-execution-model](architecture.md#sql-query-execution-model) |
| Query execution stages | [architecture.md#query-execution-stages](architecture.md#query-execution-stages) |
| Supported query types | [architecture.md#supported-query-types](architecture.md#supported-query-types) |
| Complete query trace | [data-flow.md#complete-query-execution-example](data-flow.md#complete-query-execution-example) |

### Pipeline & Execution
| Topic | Location |
|-------|----------|
| Push vs Pull Volcano | [architecture.md#push-based-volcano-model](architecture.md#push-based-volcano-model) |
| Pipeline architecture | [architecture.md#pipeline-architecture](architecture.md#pipeline-architecture) |
| Pipeline at a glance | [data-flow.md#pipeline-at-a-glance](data-flow.md#pipeline-at-a-glance) |
| PipelineStep internals | [components.md#pipelinestep-execution](components.md#pipelinestep-execution) |
| Channel wiring | [architecture.md#channel-wiring](architecture.md#channel-wiring) |

### Parallelism & Scheduling
| Topic | Location |
|-------|----------|
| Morsel-driven parallelism | [architecture.md#parallel-execution-model](architecture.md#parallel-execution-model) |
| Parallel morsel execution | [data-flow.md#parallel-morsel-execution](data-flow.md#parallel-morsel-execution) |
| Work-stealing (CAS) | [data-flow.md#work-stealing-execution-model](data-flow.md#work-stealing-execution-model) |
| Executor thread pools | [architecture.md#parallel-execution-model](architecture.md#parallel-execution-model) |

### Data Structures
| Topic | Location |
|-------|----------|
| ColumnarBatch | [components.md#columnarbatch-structure](components.md#columnarbatch-structure) |
| ColumnarPage & ColumnData | [components.md#columnarpage-structure](components.md#columnarpage-structure) |
| Bitmap operations | [components.md#bitmap-operations](components.md#bitmap-operations) |
| DictionaryColumn | [components.md#dictionary-encoding](components.md#dictionary-encoding) |

### Storage & I/O
| Topic | Location |
|-------|----------|
| Three-tier cache | [architecture.md#three-tier-page-cache](architecture.md#three-tier-page-cache) |
| Cache lifecycle | [components.md#cache-system](components.md#cache-system) |
| io_uring + O_DIRECT | [components.md#storage-io](components.md#storage-io) |
| Block allocator | [components.md#block-allocator](components.md#block-allocator) |
| Disk I/O architecture | [architecture.md#disk-io-architecture](architecture.md#disk-io-architecture) |

### Write Path
| Topic | Location |
|-------|----------|
| Writer overview | [architecture.md#write-path-architecture](architecture.md#write-path-architecture) |
| Sharded writer | [components.md#sharded-architecture](components.md#sharded-architecture) |
| Three-phase commit | [components.md#three-phase-commit](components.md#three-phase-commit) |
| WAL integration | [components.md#wal-integration-and-recovery](components.md#wal-integration-and-recovery) |
| Row buffering | [components.md#row-buffering-and-page-group-flushing](components.md#row-buffering-and-page-group-flushing) |

### Metadata
| Topic | Location |
|-------|----------|
| Metadata organization | [architecture.md#metadata-organization](architecture.md#metadata-organization) |
| Column statistics | [components.md#column-statistics](components.md#column-statistics) |
| Page descriptors | [architecture.md#metadata-organization](architecture.md#metadata-organization) |

---

## Key Diagrams

| Diagram | Shows | Location |
|---------|-------|----------|
| System overview | All components and data flow | [README.md](README.md) |
| SQL execution big picture | Parse → Plan → Build → Execute | [README.md](README.md) |
| Push vs Pull comparison | Why push-based is better | [architecture.md](architecture.md) |
| Pipeline structure | Steps, channels, Job | [architecture.md](architecture.md) |
| Parallel timeline | Workers executing morsels | [data-flow.md](data-flow.md) |
| Work-stealing sequence | CAS operations | [data-flow.md](data-flow.md) |
| Three-tier cache | Hot → Warm → Cold flow | [architecture.md](architecture.md) |
| Writer three-phase commit | Persist → Metadata → Cache | [components.md](components.md) |
| io_uring batched reads | Async I/O pattern | [components.md](components.md) |

---

## Module → Documentation Map

```
src/
├── sql/
│   ├── parser.rs        → README (SQL execution), architecture.md (query stages)
│   ├── planner.rs       → architecture.md (SQL-to-pipeline mapping)
│   └── runtime/
│       └── batch.rs     → components.md (ColumnarBatch, ColumnarPage, Bitmap)
├── pipeline/
│   ├── builder.rs       → architecture.md (pipeline architecture)
│   └── types.rs         → components.md (PipelineStep, Job)
├── executor.rs          → architecture.md (parallel execution), data-flow.md (work-stealing)
├── cache/               → architecture.md (three-tier), components.md (lifecycle)
├── page_handler/        → components.md (PageHandler, prefetch, io_uring)
├── metadata_store/      → architecture.md (metadata org), components.md (stats)
└── writer/              → components.md (sharded writer, three-phase commit, WAL)
```

---

## Glossary

| Term | Definition |
|------|------------|
| **Morsel** | A page group (~50k rows), unit of parallel work |
| **Step** | Pipeline operator that processes one column |
| **Root step** | First step that scans pages and creates batches |
| **Late materialization** | Loading columns only for rows that survive filters |
| **Bitmap** | Packed bit vector (64 rows per u64) for filter results |
| **ColumnarBatch** | Container for columns flowing through pipeline |
| **Job** | Executable pipeline with steps and channels |
| **CAS** | Compare-and-swap, atomic operation for work-stealing |
| **Page group** | Storage unit, one morsel of data for one column |
| **Column chain** | Ordered list of page descriptors for a column |
