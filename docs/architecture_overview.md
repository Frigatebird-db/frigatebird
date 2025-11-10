---
title: "Satori Storage Architecture Overview"
layout: default
nav_order: 3
---

# Satori Storage Architecture Overview

This document walks through the current storage engine blueprint implemented in `src/`. It assumes no prior familiarity with the codebase and covers the core data structures, caches, metadata, and execution flows that already exist in the repository.

## High-Level Topology

At runtime, `main.rs` wires together the storage layers into the following pipeline:

```
╔═════════════════════════════════════════════════════════════╗
║                      TableMetaStore                         ║
║  • Column ranges + MVCC versions                            ║
║  • Owns Arc<PageMetadata> entries                           ║
╚══════════════════════╤══════════════════════════════════════╝
                       │ Arc<PageMetadata> (id, disk path, offset)
                       ▼
╔═════════════════════════════════════════════════════════════╗
║                      PageHandler                            ║
║  ╔═══════════════════╦═══════════════════════════════════╗  ║
║  ║ Uncompressed      ║   Compressed Page Cache           ║  ║
║  ║ Page Cache (UPC)  ║   (CPC / cold blobs)              ║  ║
║  ║ (hot Pages)       ║   RwLock<LruCache>                ║  ║
║  ║ RwLock<LruCache>  ║                                   ║  ║
║  ╚═════════╤═════════╩═══════════╤═══════════════════════╝  ║
║            │ UPC hit?             │ CPC hit?                 ║
║            ▼                      ▼                          ║
║      Arc<PageCacheEntry>    Arc<PageCacheEntry>             ║
║            └──────────┬───────────┘                          ║
║                       ▼                                      ║
║                  Compressor                                  ║
║               (lz4_flex + bincode)                           ║
║                       │                                      ║
║                       ▼                                      ║
║                    PageIO                                    ║
║             (64B metadata + blob on disk)                    ║
╚═══════════════════════╤═════════════════════════════════════╝
                        │
                        ▼
              ╔═══════════════════╗
              ║ Persistent Storage║
              ╚═══════════════════╝
```

All public operations (`ops_handler.rs`) talk to the `PageDirectory` façade (which wraps `TableMetaStore`) to map logical column ranges to physical pages, then ask `PageHandler` to materialize the requested pages through the cache strata down to disk.

## Data Model

### Entry & Page

- **Entry** — single row value with placeholder metadata fields
- **Page** — ordered Vec<Entry> + metadata, Serde-serializable
- `current_epoch_millis()` used for LRU and MVCC timestamps

```
Page
 ├─ page_metadata : String (placeholder)
 └─ entries       : Vec<Entry>
        ├─ prefix_meta : String (reserved)
        ├─ data        : String (user payload)
        └─ suffix_meta : String (reserved)
```

## Metadata Catalog (`src/metadata_store/mod.rs`)

`TableMetaStore` is the authoritative mapping from logical column ranges to physical pages and their on-disk location. It has two maps:

```rust
col_data  : HashMap<ColumnName, Arc<RwLock<Vec<TableMetaStoreEntry>>>>
page_data : HashMap<PageId, Arc<PageMetadata>>
```

**Key structures:**
- `PageMetadata` — immutable (id, disk_path, offset)
- `TableMetaStoreEntry` — spans `(start_idx, end_idx)`, holds MVCC history
- `MVCCKeeperEntry` — version info (page_id, commit_time, locked_by)
- `PageDirectory` — façade providing `latest()` and `range()` lookup

**Operations:**
- Add page: atomic counter mints hex ID, append to column's MVCC list (max 8 versions)
- Range lookup: binary search boundaries, pick newest version ≤ timestamp
- Locks held briefly during search, dropped before building descriptors

### Metadata Diagram

```
col_data["users.age"]
       │
       ▼
Arc<RwLock<Vec<TableMetaStoreEntry>>>  (per-column ranges)
       │
       ├─ TableMetaStoreEntry #0
       │    start_idx = 0
       │    end_idx   = 1024
       │    page_metas:
       │       [ MVCCKeeperEntry(page_id="p1", commit=100),
       │         MVCCKeeperEntry(page_id="p2", commit=120) ]
       │
       └─ TableMetaStoreEntry #1
            start_idx = 1024
            end_idx   = 2048
            page_metas:
               [ MVCCKeeperEntry(page_id="p9", commit=180) ]

page_data:
  "p1" ──▶ Arc<PageMetadata { id="p1", disk_path="/data/...", offset=... }>
  "p2" ──▶ Arc<PageMetadata { ... }>
  "p9" ──▶ Arc<PageMetadata { ... }>
```

## Cache Hierarchy (`src/cache/page_cache.rs`)

Two-tier LRU cache (max 10 entries each):

**UPC (Uncompressed Page Cache):**
- Hot `Page` structs for mutations/reads
- Eviction → compress → insert into CPC

**CPC (Compressed Page Cache):**
- Compressed blobs ready for disk
- Eviction → lookup via `PageDirectory` → flush to disk

**Structure:**
- `store: HashMap<PageId, PageCacheEntry<T>>` — O(1) lookup
- `lru_queue: BTreeSet<(used_time, PageId)>` — eviction order
- `lifecycle: Option<Arc<dyn CacheLifecycle<T>>>` — eviction hook

### Cache Flow

```
Request page "p42"
    │
    ├─ UPC (HashMap) hit?
    │      └─▶ YES ──▶ return Arc<PageCacheEntryUncompressed>
    │
    ├─ CPC hit?
    │      ├─▶ YES ──▶ decompress
    │      │          └─▶ insert into UPC
    │      │              └─▶ return Arc<PageCacheEntryUncompressed>
    │
    └─ Fetch from disk
            │
            ├─▶ PageIO::read_from_path(path, offset)
            ├─▶ insert compressed bytes into CPC
            └─▶ decompress + seed UPC
                └─▶ return Arc<PageCacheEntryUncompressed>
```

## Compressor (`src/helpers/compressor.rs`)

- `compress` — Page → bincode → lz4_flex → compressed blob
- `decompress` — compressed blob → lz4_flex → bincode → Page
- Uses `Arc` to avoid cloning

## Persistent Storage Layout (`src/page_handler/page_io.rs`)

Pages on disk are stored as a 64-byte metadata prefix followed by the compressed payload:

```
╔═══════════════════════════════╦═══════════════════════════════╗
║ 64-byte Metadata (rkyv)       ║ LZ4 Compressed Page bytes     ║
║ • read_size : usize           ║ (Compressor::compress output) ║
╚═══════════════════════════════╩═══════════════════════════════╝
```

**Read:** seek → read 64B metadata (rkyv zero-copy) → read compressed page
**Write:** serialize metadata (rkyv) → pad to 64B → write prefix+page → sync

**Note:** `File::create` truncates; appending requires `OpenOptions`.

## PageHandler (`src/page_handler/mod.rs`)

Three services + coordinator + prefetch thread:

- **PageLocator** — metadata lookups (latest/range/id) → `PageDescriptor`
- **PageFetcher** — CPC lookups, batch disk reads (io_uring on Linux), inserts to CPC
- **PageMaterializer** — UPC lookups, batch decompress, inserts to UPC
- **PageHandler** — wires above, exposes unified API, manages prefetch thread
- **Prefetch Thread** — background worker polls channel (1ms interval), batches page requests

### Batch Retrieval Flow (`get_pages`)

```
Input: [PageDescriptor{id="p1"}, PageDescriptor{id="p7"}, ...]

╔═══════════════════════════════════════════════════════════╗
║ Step 1: UPC sweep (read lock)                             ║
║   ▶ Collect hits in request order                         ║
║   ▶ Track missing IDs in `meta_map`                       ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 2: CPC sweep (read lock)                             ║
║   ▶ Collect compressed hits                               ║
║   ▶ Remove from `meta_map`                                ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 3: Decompress CPC hits (outside locks)               ║
║   ▶ materializer.materialize_many(blobs)                  ║
║   ▶ Write each page back into UPC                         ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 4: Remaining misses                                  ║
║   ▶ For each descriptor still in `meta_map`:              ║
║     • fetcher.fetch_and_insert(descriptor)  // CPC + disk ║
║     • materializer.materialize_many(fetched) // UPC       ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 5: Final UPC pass (read lock)                        ║
║   ▶ Gather pages in original order                        ║
║   ▶ For any IDs not emitted in step 1                     ║
╚═══════════════════════════════════════════════════════════╝
```

Locks are held only around direct cache map access; decompression happens outside of locks to prevent blocking other threads.

### Batch Operations & Prefetching

**Batch Read (io_uring on Linux):**
```
fetch_and_insert_batch(descriptors)
    │
    ├─▶ Group by disk_path
    ├─▶ For each path:
    │      ├─▶ Phase 1: Submit all metadata reads (64B each)
    │      ├─▶ Phase 2: Submit all payload reads (parallel)
    │      └─▶ Single fsync
    └─▶ Single write lock to insert all into CPC
```

**Batch Write (io_uring on Linux):**
```
write_batch_to_path(writes)
    │
    ├─▶ Prepare all buffers (metadata + payload)
    ├─▶ Submit all Write opcodes
    ├─▶ Wait for completions
    └─▶ Single Fsync
```

**Prefetching:**
```
get_pages_with_prefetch(page_ids, k)
    │
    ├─▶ Return first k pages immediately (blocking)
    └─▶ Send remaining to prefetch channel (non-blocking)
          │
          ▼
    Prefetch Thread (polls every 1ms)
          │
          ├─▶ Drain channel immediately when work arrives
          ├─▶ Batch overlapping requests automatically
          └─▶ Call ensure_pages_cached() to load into caches
```

See [batch_operations](batch_operations) for complete details on io_uring implementation and performance characteristics.

## Operation Layer (`src/ops_handler.rs`)

External API surface for column operations:

- `upsert_data_into_column` — locate latest page, clone, mutate, write back
- `update_column_entry` — same as upsert but overwrites specific row
- `range_scan_column_entry` — locate range, get pages, concatenate entries (no row slicing)

## Scheduler (`src/pool/scheduler.rs`)

Minimal thread pool. Currently unused.

## Putting It All Together

Current execution path for a write request:

```
Client upsert
      │
      ▼
ops_handler::upsert_data_into_column
      │
      ▼
PageDirectory::latest(column)
      │
      ▼
PageHandler::get_page
      │
      ├─▶ UPC hit? ──▶ return Arc (hot path)
      │
      ├─▶ CPC hit? ──▶ decompress + seed UPC
      │
      └─▶ Disk miss ──▶ PageIO::read + CPC insert + decompress + seed UPC
      │
      ▼
Clone Arc<PageCacheEntryUncompressed>
      │
      ▼ (mutate Page entries)
      │
UPC write lock
      │
      ▼ (add updated page back into cache)
      │
      ▼
Return success
```

Current execution path for a range scan:

```
Client range query
      │
      ▼
ops_handler::range_scan_column_entry
      │
      ▼
PageDirectory::range(column, l, r, ts)
      │
      ▼
PageHandler::get_pages (batch)
      │
      ├─▶ UPC sweep ──▶ immediate hits
      │
      ├─▶ CPC sweep ──▶ decompress + insert UPC
      │
      └─▶ Disk fetch ──▶ CPC insert ──▶ decompress + insert UPC
      │
      ▼
Gather Pages in original order
      │
      ▼
Collect Entry vectors (returns full pages, no row slicing)
```

## SQL Engine (`src/sql/`)

The SQL engine provides complete query execution including SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE operations. It consists of three main subsystems:

### Parser & Planner

- `parser.rs` wraps the `sqlparser` crate (GenericDialect) and exposes `parse_sql(&str)`
- `planner.rs` converts SQL statements into execution plans
- `models.rs` defines shared types: `QueryPlan`, `TableAccess`, `FilterExpr`, `PlannerError`

### SQL Executor (`src/sql/executor/`)

Full query execution engine with **~10,000 lines** across 14 modules:

**Core Modules:**
- `mod.rs` - Main SELECT execution with full feature support (3,445 lines)
- `batch.rs` - Columnar batch processing with type-specific storage (655 lines)
- `expressions.rs` - Expression evaluation engine supporting WHERE, projections, arithmetic (1,631 lines)
- `aggregates.rs` - Complete aggregate functions: COUNT, SUM, AVG, MIN, MAX, VARIANCE, STDDEV, PERCENTILE_CONT (1,089 lines)
- `window_helpers.rs` - Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM OVER, etc. (1,375 lines)
- `ordering.rs` - Advanced sorting with NULLS FIRST/LAST, multi-column keys (424 lines)
- `grouping_helpers.rs` - GROUP BY with multi-column keys and HAVING clauses (170 lines)

**Support Modules:**
- `values.rs` - Type system with null encoding and coercion (174 lines)
- `row_functions.rs` - Scalar functions (ABS, ROUND, CEIL, FLOOR, EXP, LN, LOG, POWER) (408 lines)
- `scalar_functions.rs` - Additional scalar operations (167 lines)
- `projection_helpers.rs` - Result set materialization (223 lines)
- `helpers.rs` - Utilities for name extraction, LIKE/RLIKE patterns (460 lines)
- `scan_helpers.rs` - Scan optimization helpers (110 lines)
- `spill.rs` - Spill-to-disk infrastructure (24 lines, placeholder)

### Columnar Batch Architecture

The executor uses a **columnar batch processing** model for performance:

```
ColumnarBatch
  ├─ columns: HashMap<ordinal, ColumnarPage>
  ├─ num_rows: usize
  └─ aliases: HashMap<name, ordinal>

ColumnarPage
  ├─ data: ColumnData
  │    ├─ Int64(Vec<i64>)
  │    ├─ Float64(Vec<f64>)
  │    └─ Text(Vec<String>)
  ├─ null_bitmap: Bitmap
  └─ num_rows: usize

Bitmap
  ├─ bits: Vec<u64>         (64-bit words for compact storage)
  └─ operations: and, or, invert, count_ones, iter_ones
```

**Benefits:**
- Type-specific storage reduces parse overhead
- Bitmap operations enable efficient filtering
- Columnar layout improves cache locality
- Batch operations reduce per-row overhead

### SELECT Execution Pipeline

```
SQL Query
    │
    ├─▶ Parse & validate (parser.rs)
    │
    ├─▶ Plan query (executor/mod.rs)
    │     ├─▶ Identify required columns
    │     ├─▶ Detect aggregates/window functions
    │     └─▶ Build execution plan
    │
    ├─▶ Load data into ColumnarBatch
    │     ├─▶ Locate pages via PageDirectory
    │     ├─▶ Materialize columns from pages
    │     └─▶ Convert to type-specific arrays
    │
    ├─▶ Apply WHERE filters
    │     ├─▶ Evaluate expressions on batch
    │     ├─▶ Build selection bitmap
    │     └─▶ Gather filtered rows
    │
    ├─▶ Apply GROUP BY (if present)
    │     ├─▶ Evaluate group keys on batch
    │     ├─▶ Hash-based aggregation
    │     └─▶ Evaluate HAVING clause
    │
    ├─▶ Apply window functions (if present)
    │     ├─▶ Partition by keys
    │     ├─▶ Sort within partitions
    │     └─▶ Compute window function outputs
    │
    ├─▶ Apply ORDER BY
    │     ├─▶ Build multi-column sort keys
    │     ├─▶ Sort with NULLS FIRST/LAST
    │     └─▶ Reorder batch
    │
    └─▶ Apply LIMIT/OFFSET → Final results
```

### Supported SQL Features

**SELECT:**
- Projections with expressions: `SELECT price * 1.1 AS discounted`
- WHERE with complex predicates: `age > 18 AND (status = 'active' OR vip = true)`
- Aggregate functions: `COUNT(*), COUNT(col), SUM, AVG, MIN, MAX, VARIANCE, STDDEV, PERCENTILE_CONT`
- GROUP BY with multi-column keys: `GROUP BY region, status`
- HAVING clause: `HAVING COUNT(*) > 10`
- Window functions: `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)`
- ORDER BY with NULLS placement: `ORDER BY age DESC NULLS LAST`
- LIMIT/OFFSET for pagination
- Scalar functions: `ABS, ROUND, CEIL, FLOOR, EXP, LN, LOG, POWER, WIDTH_BUCKET`
- Pattern matching: `LIKE, ILIKE, RLIKE` (regex)
- Operators: arithmetic (`+, -, *, /`), comparison (`=, !=, <, >, <=, >=`), logical (`AND, OR, NOT`)
- Range queries: `BETWEEN, IN`
- Null handling: `IS NULL, IS NOT NULL`

**DML:**
- `INSERT INTO ... VALUES ...` with sorted insertion
- `UPDATE ... SET ... WHERE ...` with row repositioning for sort key changes
- `DELETE FROM ... WHERE ...` with metadata updates

**DDL:**
- `CREATE TABLE ... ORDER BY ...` for sorted tables

See [sql_executor](sql_executor) for complete documentation on the execution engine.

## Writer Architecture (`src/writer/`)

The Writer module provides **transactional write execution** with O_DIRECT I/O support and atomic metadata updates.

### Components

```
┌─────────────────────────────────────────────┐
│              Writer                         │
│  ┌─────────────────────────────────┐        │
│  │   Crossbeam Channel (Unbounded) │        │
│  └────────────┬────────────────────┘        │
│               │                             │
│               ▼                             │
│  ┌─────────────────────────────────┐        │
│  │      WorkerContext              │        │
│  │   (Background Thread)           │        │
│  └────────────┬────────────────────┘        │
│               │                             │
└───────────────┼─────────────────────────────┘
                │
    ┌───────────┴──────────────┐
    │                          │
    ▼                          ▼
PageAllocator            MetadataClient
DirectBlockAllocator     DirectoryMetadataClient
```

**Key Features:**
- **DirectBlockAllocator**: 256 KiB block allocation with 4 KiB tail granularity
- **O_DIRECT support**: 4K-aligned writes on Linux for bypassing page cache
- **File rotation**: Automatic rotation at 4 GiB boundaries (storage/data.00000, data.00001, ...)
- **Atomic commits**: All column updates in a job commit together via `register_batch()`
- **Background execution**: Single worker thread serializes all writes

### Write Execution Flow

```
UpdateJob { table, columns: [ColumnUpdate] }
    │
    ├─▶ Stage Phase (parallel per-column preparation)
    │     ├─▶ Load latest page version from cache
    │     ├─▶ Apply operations (Overwrite, Append, InsertAt, BufferRow)
    │     ├─▶ Serialize with bincode
    │     └─▶ Allocate disk space (4K-aligned)
    │
    ├─▶ Metadata Commit (atomic)
    │     └─▶ PageDirectory::register_batch() with write lock
    │
    ├─▶ Persistence (I/O)
    │     ├─▶ Zero-pad to alloc_len
    │     ├─▶ Write to allocated offsets
    │     └─▶ (O_DIRECT writes on Linux)
    │
    └─▶ Cache Writeback
          └─▶ Insert new page versions into UPC
```

**Update Operations:**
- `Overwrite { row, entry }` - Replace specific row, auto-extend page if needed
- `Append { entry }` - Push to end of page
- `InsertAt { row, entry }` - Insert at position, shifting subsequent rows
- `BufferRow { row }` - Buffer for page-group batching (optimizes sorted inserts)

### Page Group Batching

For efficient sorted table inserts, the Writer buffers rows until reaching `ROWS_PER_PAGE_GROUP` (default: 1000), then writes complete page groups across all columns atomically:

```
INSERT rows 1-999   → Buffered in memory
INSERT row 1000     → Triggers flush:
                        ├─▶ Sort by ORDER BY columns
                        ├─▶ Write page for each column
                        └─▶ Atomic metadata commit
```

See [writer](writer) for detailed architecture documentation.

## Pipeline Executor (`src/executor.rs`)

The pipeline executor orchestrates the execution of query pipelines using a sophisticated dual-pool architecture:

- **PipelineExecutor** — coordinates job submission and worker pools
- **Main workers (85%)** — process incoming jobs sequentially from a queue
- **Reserve workers (15%)** — steal high-cost jobs for parallel execution
- **JobBoard** — lock-free SkipSet maintaining cost-ordered job priorities
- **CAS-based step execution** — atomic compare-and-swap for lock-free step claiming

### Execution Model

```
Job submission
      │
      ▼
Main worker receives Job
      │
      ├─▶ Insert into JobBoard (SkipSet ordered by cost)
      ├─▶ Wake reserve workers
      └─▶ Execute job.get_next() (CAS claim step)
            │
            └─▶ Repeat until all steps complete

Reserve worker (parallel):
      │
      ├─▶ Wait for wake signal
      ├─▶ Pop heaviest job from JobBoard
      └─▶ Execute job.get_next() (CAS claim step)
            │
            └─▶ Work-steal from same job as main worker
```

**Job::get_next() mechanism:**
```
loop:
  slot = next_free_slot.load()
  if slot >= steps.len(): return (done)
  if compare_exchange(slot, slot+1) succeeds:
    execute steps[slot]
    return
  else: retry
```

Exactly one worker executes each PipelineStep. Multiple workers progress through different steps of the same job in parallel.

See [executor](executor) for complete documentation on the dual-pool work-stealing architecture.

## Current Behavior Notes

- Page ID generation uses simple atomic counter
- Lifecycle errors silently ignored (`.unwrap()` or dropped `Result`)
- `get_page_path_and_offset` panics on missing IDs
- No WAL, no background compaction
- `Page::add_entry` doesn't trigger persistence
