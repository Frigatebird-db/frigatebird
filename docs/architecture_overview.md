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

## SQL Parser & Planner (`src/sql/`)

To reason about table access without a full execution engine, the repo now includes a small SQL surface located under `src/sql/`:

- `parser.rs` wraps the `sqlparser` crate (GenericDialect) and exposes `parse_sql(&str)`.
- `planner.rs` turns `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements (single table only, joins unsupported) into table-level plans.
- `models.rs` defines the shared types: `QueryPlan`, `TableAccess`, `FilterExpr`, and `PlannerError`.

Using `plan_sql(sql)` returns a `QueryPlan` with one `TableAccess` entry per referenced table. Each entry records:

- `table_name`
- `read_columns` / `write_columns` (`BTreeSet<String>`; `*` denotes unknown/all columns)
- `filters: Option<FilterExpr>` combining `WHERE`/`HAVING`/`QUALIFY` (or DML predicates) into an AND/OR tree with original `sqlparser::ast::Expr` leaves

Example (`tests/sql_planner_tests.rs`):

```
SELECT id
FROM accounts
WHERE status = 'active'
  AND (region = 'US' OR vip = true)
```

Produces:

```
TableAccess {
  table_name: "accounts",
  read_columns: {"id", "region", "status", "vip"},
  write_columns: {},
  filters: Some(
    And([
      Leaf(status = 'active'),
      Or([Leaf(region = 'US'), Leaf(vip = true)])
    ])
  ),
}
```

Pipeline sketch:

```
+------------------------------+
| sql::plan_sql(\"SELECT ...\") |
+---------------+--------------+
                |
                v
        +-------+-------+
        | parser.rs     |  (calls sqlparser)
        +-------+-------+
                |
                v
        +-------+-------+
        | planner.rs    |  (walks AST, collects tables)
        +-------+-------+
                |
                v
        +-------+--------+
        | models.rs      |
        |  QueryPlan     |
        |   └─ TableAccess{read/write cols, filters}
        |  FilterExpr    |
        +----------------+
                |
                v
        +-------+--------+
        | pipeline::build| (group filters → Job)
        +-------+--------+
                |
                v
        +-------+--------+
        | executor.rs    | (main/reserve pools + JobBoard)
        +----------------+
```

Because plans are pure data, they can be fed into future query-planning layers, validation tooling, or observability hooks without touching the storage engine.

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
