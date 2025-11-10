---
title: "System Overview - Visual Guide"
layout: default
nav_order: 4
---

# System Overview - Visual Guide

Progressive deep-dive from 30,000ft view down to implementation details.

---

## Level 0: Complete System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT APPLICATION                        │
│              SQL queries, DML operations, scans                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌────────────────┐   ┌─────────────────┐
│ SQL Executor  │   │ Direct Ops     │   │ Writer          │
│ (SELECT)      │   │ (legacy scans) │   │ (DML mutations) │
└───────┬───────┘   └────────┬───────┘   └────────┬────────┘
        │                    │                     │
        └────────────────────┼─────────────────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
        ┌────────────┬────────────┬────────────┐
        │ PageDir    │ PageHandler│ Allocator  │
        │ (metadata) │ (caches)   │ (disk)     │
        └────────────┴────────────┴────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Persistent      │
                    │ Storage         │
                    │ (data files)    │
                    └─────────────────┘
```

---

## Level 1: SQL Query Execution Flow

### SELECT Path (Columnar Batch Processing)

```
╔═══════════════════════════════════════════════════════════════╗
║ SQL: "SELECT dept, AVG(salary) FROM employees                ║
║       WHERE age > 25 GROUP BY dept ORDER BY dept"            ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║                    SQL PARSER (sqlparser)                     ║
║  • Tokenize: [SELECT, dept, ',', AVG, '(', salary, ...)      ║
║  • Parse: Statement::Query(...)                              ║
║  • Output: AST (Abstract Syntax Tree)                        ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │ AST
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║              SQL EXECUTOR (executor/mod.rs)                   ║
║                                                               ║
║  Phase 1: Planning                                            ║
║    • Identify required columns: [dept, salary, age]          ║
║    • Detect aggregates: [AVG(salary)]                        ║
║    • Extract WHERE filters: [age > 25]                       ║
║    • Extract GROUP BY keys: [dept]                           ║
║    • Extract ORDER BY: [dept ASC]                            ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║  Phase 2: Data Loading                                        ║
║    ┌──────────────────────────────────────────┐              ║
║    │ PageDirectory::locate_range(table, ...)  │              ║
║    │   → Returns Vec<PageDescriptor>          │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ PageHandler::get_pages(descriptors)      │              ║
║    │   → Returns Vec<Arc<Page>>               │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ Convert to ColumnarBatch                 │              ║
║    │   Page(Vec<Entry>) → ColumnarBatch       │              ║
║    │     columns: {                            │              ║
║    │       0 → ColumnarPage {                  │              ║
║    │             data: Text(["eng","sales"]),  │              ║
║    │             null_bitmap: [0,0]            │              ║
║    │           }                               │              ║
║    │       1 → ColumnarPage {                  │              ║
║    │             data: Int64([25, 30, ...]),   │              ║
║    │             null_bitmap: [0,0]            │              ║
║    │           }                               │              ║
║    │       2 → ColumnarPage {                  │              ║
║    │             data: Int64([80000, 90000])   │              ║
║    │           }                               │              ║
║    │     }                                     │              ║
║    └──────────────────────────────────────────┘              ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │ ColumnarBatch (1000 rows)
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║  Phase 3: WHERE Filtering (batch.rs + expressions.rs)         ║
║    ┌──────────────────────────────────────────┐              ║
║    │ evaluate_selection_on_batch(age > 25)    │              ║
║    │   → Bitmap: [1,1,0,1,0,1,1,0,...]        │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ selected_indices = bitmap.iter_ones()    │              ║
║    │   → [0, 1, 3, 5, 6, ...]                 │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ batch = batch.gather(selected_indices)   │              ║
║    │   → ColumnarBatch with 650 rows          │              ║
║    └──────────────────────────────────────────┘              ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │ Filtered batch (650 rows)
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║  Phase 4: GROUP BY Aggregation (aggregates.rs)                ║
║    ┌──────────────────────────────────────────┐              ║
║    │ Evaluate group keys: [dept]              │              ║
║    │   keys = ["eng", "sales", "eng", ...]    │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ Hash aggregation table:                  │              ║
║    │   HashMap<GroupKey, AggregateState>      │              ║
║    │                                           │              ║
║    │   For each row i:                        │              ║
║    │     key = GroupKey(dept[i])              │              ║
║    │     state = table[key]                   │              ║
║    │     state.sum += salary[i]               │              ║
║    │     state.count += 1                     │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ Materialize results:                     │              ║
║    │   dept      AVG(salary)                  │              ║
║    │   ----      -----------                  │              ║
║    │   eng       95000                        │              ║
║    │   sales     87500                        │              ║
║    │   → ColumnarBatch with 2 rows            │              ║
║    └──────────────────────────────────────────┘              ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │ Aggregated batch (2 rows)
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║  Phase 5: ORDER BY Sorting (ordering.rs)                      ║
║    ┌──────────────────────────────────────────┐              ║
║    │ Build sort keys for each row:            │              ║
║    │   keys = [                               │              ║
║    │     OrderKey([Text("eng")]),             │              ║
║    │     OrderKey([Text("sales")])            │              ║
║    │   ]                                      │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ keys.sort_unstable_by(compare_order_keys)│              ║
║    │   → indices: [0, 1] (eng < sales)        │              ║
║    └───────────────┬──────────────────────────┘              ║
║                    ▼                                          ║
║    ┌──────────────────────────────────────────┐              ║
║    │ batch = batch.gather(sorted_indices)     │              ║
║    └──────────────────────────────────────────┘              ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │ Sorted batch (2 rows)
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║  Phase 6: Projection (projection_helpers.rs)                  ║
║    Convert ColumnarBatch → Vec<Vec<String>>                   ║
║                                                               ║
║    Results:                                                   ║
║    [                                                          ║
║      ["eng", "95000"],                                        ║
║      ["sales", "87500"]                                       ║
║    ]                                                          ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## Level 2: DML Execution (INSERT/UPDATE/DELETE)

### INSERT Path (Page Group Batching)

```
╔═══════════════════════════════════════════════════════════════╗
║ SQL: "INSERT INTO users (id, name) VALUES (1, 'Alice')"      ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║              SQL EXECUTOR (executor/mod.rs)                   ║
║  • Parse INSERT statement                                     ║
║  • Extract values: [("1", "Alice")]                          ║
║  • Resolve table schema and ORDER BY columns                 ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
╔═══════════════════════════════════════════════════════════════╗
║                    WRITER (writer/executor.rs)                ║
║                                                               ║
║  ┌──────────────────────────────────────────┐                ║
║  │ Submit UpdateJob {                       │                ║
║  │   table: "users",                        │                ║
║  │   columns: [                             │                ║
║  │     ColumnUpdate {                       │                ║
║  │       column: "id",                      │                ║
║  │       operations: [                      │                ║
║  │         BufferRow { row: ["1"] }         │                ║
║  │       ]                                  │                ║
║  │     },                                   │                ║
║  │     ColumnUpdate {                       │                ║
║  │       column: "name",                    │                ║
║  │       operations: [                      │                ║
║  │         BufferRow { row: ["Alice"] }     │                ║
║  │       ]                                  │                ║
║  │     }                                    │                ║
║  │   ]                                      │                ║
║  │ }                                        │                ║
║  └───────────────┬──────────────────────────┘                ║
║                  │                                            ║
║                  ▼                                            ║
║  ┌──────────────────────────────────────────┐                ║
║  │ Background Worker Thread                 │                ║
║  │ (polls channel every ~1ms)               │                ║
║  └───────────────┬──────────────────────────┘                ║
║                  │                                            ║
║                  ▼                                            ║
║  ┌──────────────────────────────────────────┐                ║
║  │ buffer_row(table, row)                   │                ║
║  │   buffered_rows["users"] += 1            │                ║
║  │   if buffered_rows.len() >= 1000:        │                ║
║  │     flush_page_group()                   │                ║
║  └───────────────┬──────────────────────────┘                ║
╚══════════════════╪═══════════════════════════════════════════╝
                   │ (after 1000 rows buffered)
                   ▼
╔═══════════════════════════════════════════════════════════════╗
║              flush_page_group() - Phase 1: Stage              ║
║  ┌──────────────────────────────────────────┐                ║
║  │ For each column:                         │                ║
║  │   1. Sort buffered rows by ORDER BY keys│                ║
║  │   2. Create new Page with 1000 entries  │                ║
║  │   3. Serialize with bincode              │                ║
║  │      → page_bytes (e.g., 45,678 bytes)   │                ║
║  │   4. Allocate disk space:                │                ║
║  │      allocator.allocate(45678)           │                ║
║  │      → { path: "storage/data.00000",     │                ║
║  │          offset: 262144,                 │                ║
║  │          alloc_len: 49152 } (48 KiB)     │                ║
║  └──────────────────────────────────────────┘                ║
╚══════════════════╪═══════════════════════════════════════════╝
                   │ Vec<StagedColumn>
                   ▼
╔═══════════════════════════════════════════════════════════════╗
║              flush_page_group() - Phase 2: Commit             ║
║  ┌──────────────────────────────────────────┐                ║
║  │ metadata_client.commit(table, updates)   │                ║
║  │   → PageDirectory::register_batch()      │                ║
║  │   → Acquires write lock                  │                ║
║  │   → Generates page IDs: ["p1001", "p1002"]│               ║
║  │   → Updates column metadata atomically   │                ║
║  │   → Returns Vec<PageDescriptor>          │                ║
║  └──────────────────────────────────────────┘                ║
╚══════════════════╪═══════════════════════════════════════════╝
                   │ Committed
                   ▼
╔═══════════════════════════════════════════════════════════════╗
║              flush_page_group() - Phase 3: Persist            ║
║  ┌──────────────────────────────────────────┐                ║
║  │ For each StagedColumn:                   │                ║
║  │   1. Zero-pad buffer to alloc_len        │                ║
║  │      buffer = vec![0; 49152]             │                ║
║  │      buffer[0..45678] = serialized_bytes │                ║
║  │   2. Open file (O_DIRECT on Linux)       │                ║
║  │   3. write_all_at(buffer, offset)        │                ║
║  │      → Writes to storage/data.00000      │                ║
║  └──────────────────────────────────────────┘                ║
╚══════════════════╪═══════════════════════════════════════════╝
                   │ Persisted
                   ▼
╔═══════════════════════════════════════════════════════════════╗
║              flush_page_group() - Phase 4: Cache              ║
║  ┌──────────────────────────────────────────┐                ║
║  │ For each page:                           │                ║
║  │   page_handler.write_back_uncompressed(  │                ║
║  │     descriptor.id, page                  │                ║
║  │   )                                      │                ║
║  │   → Inserts into UPC                     │                ║
║  │   → Next read hits cache immediately     │                ║
║  └──────────────────────────────────────────┘                ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## Level 3: Storage Architecture Deep-Dive

### Cache Hierarchy

```
╔══════════════════════════════════════════════════════════════════╗
║                   READ REQUEST: get_page("p42")                  ║
╚════════════════════════════╤═════════════════════════════════════╝
                             │
                             ▼
          ╔══════════════════════════════════════╗
          ║ LAYER 1: Uncompressed Page Cache    ║
          ║         (UPC / hot pages)            ║
          ╚══════════════════════════════════════╝
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        Read lock     HashMap lookup   Found?
              │              │              │
              └──────────────┴──YES─────────┴──▶ return Arc<Page>
                             │
                            NO (miss)
                             │
                             ▼
          ╔══════════════════════════════════════╗
          ║ LAYER 2: Compressed Page Cache       ║
          ║         (CPC / cold blobs)            ║
          ╚══════════════════════════════════════╝
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        Read lock     HashMap lookup   Found?
              │              │              │
              │              │             YES
              │              │              │
              └──────────────┴──────────────┤
                             │              │
                            NO              ▼
                             │     ┌────────────────────┐
                             │     │ Decompress blob    │
                             │     │ (lz4 + bincode)    │
                             │     └────────┬───────────┘
                             │              │
                             │              ▼
                             │     ┌────────────────────┐
                             │     │ Insert into UPC    │
                             │     └────────┬───────────┘
                             │              │
                             │              └──▶ return Arc<Page>
                             │
                             ▼
          ╔══════════════════════════════════════╗
          ║ LAYER 3: Disk I/O                    ║
          ║         (persistent storage)         ║
          ╚══════════════════════════════════════╝
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
     Lookup descriptor  Open file    Read bytes
              │              │              │
              │              │              ▼
              │              │     ┌────────────────────┐
              │              │     │ PageIO::read()     │
              │              │     │ • seek(offset)     │
              │              │     │ • read(alloc_len)  │
              │              │     └────────┬───────────┘
              │              │              │
              │              │              ▼
              │              │     ┌────────────────────┐
              │              │     │ Insert into CPC    │
              │              │     └────────┬───────────┘
              │              │              │
              │              │              ▼
              │              │     ┌────────────────────┐
              │              │     │ Decompress → UPC   │
              │              │     └────────┬───────────┘
              │              │              │
              │              └──────────────┴──▶ return Arc<Page>
              │
              ▼
        (page not found error)
```

### Cache Eviction Flow

```
╔══════════════════════════════════════════════════════════════════╗
║              UPC reaches capacity (10 entries)                   ║
╚════════════════════════════╤═════════════════════════════════════╝
                             │
                             ▼
          ┌──────────────────────────────────┐
          │ Identify LRU entry               │
          │   lru_queue.iter().next()        │
          │   → (used_time: 100, id: "p1")   │
          └────────────┬─────────────────────┘
                       │
                       ▼
          ┌──────────────────────────────────┐
          │ Remove from UPC                  │
          │   store.remove("p1")             │
          │   lru_queue.remove((100, "p1"))  │
          │   → Arc<Page>                    │
          └────────────┬─────────────────────┘
                       │
                       ▼
          ┌──────────────────────────────────┐
          │ Lifecycle callback               │
          │   UncompressedToCompressed       │
          │   .on_evict("p1", Arc<Page>)     │
          └────────────┬─────────────────────┘
                       │
                       ▼
          ┌──────────────────────────────────┐
          │ Compress page                    │
          │   bincode::serialize(&page)      │
          │   lz4_flex::compress(&bytes)     │
          │   → compressed_blob: Vec<u8>     │
          └────────────┬─────────────────────┘
                       │
                       ▼
          ┌──────────────────────────────────┐
          │ Insert into CPC                  │
          │   cpc.add("p1", compressed_blob) │
          └──────────────────────────────────┘
                       │
                       ▼
          ┌──────────────────────────────────┐
          │ CPC eviction (if full)           │
          │   → Lifecycle callback           │
          │   → CompressedToDisk             │
          │   → Write to storage file        │
          └──────────────────────────────────┘
```

---

## Level 4: Metadata Management

### PageDirectory Structure

```
╔══════════════════════════════════════════════════════════════════╗
║                         PageDirectory                            ║
║                         (façade layer)                           ║
╚════════════════════════════╤═════════════════════════════════════╝
                             │
                             ▼
╔══════════════════════════════════════════════════════════════════╗
║                       TableMetaStore                             ║
║                                                                  ║
║  ┌──────────────────────────────────────────────────────────┐   ║
║  │ tables: HashMap<TableName, Arc<RwLock<TableCatalog>>>    │   ║
║  │   "users" → TableCatalog {                               │   ║
║  │     columns: HashMap<ColumnName, ColumnCatalog>          │   ║
║  │       "id" → ColumnCatalog {                             │   ║
║  │         ranges: Vec<TableMetaStoreEntry>                 │   ║
║  │           [                                              │   ║
║  │             Entry { start: 0, end: 1000,                 │   ║
║  │                     versions: [                          │   ║
║  │                       MVCCEntry { page_id: "p1",         │   ║
║  │                                   commit: 100 },         │   ║
║  │                       MVCCEntry { page_id: "p2",         │   ║
║  │                                   commit: 200 }          │   ║
║  │                     ]                                    │   ║
║  │             },                                           │   ║
║  │             Entry { start: 1000, end: 2000,              │   ║
║  │                     versions: [ ... ]                    │   ║
║  │             }                                            │   ║
║  │           ]                                              │   ║
║  │       }                                                  │   ║
║  │   }                                                      │   ║
║  │                                                          │   ║
║  │ page_metadata: HashMap<PageId, Arc<PageMetadata>>        │   ║
║  │   "p1" → PageMetadata {                                  │   ║
║  │     id: "p1",                                            │   ║
║  │     disk_path: "storage/data.00000",                     │   ║
║  │     offset: 0,                                           │   ║
║  │     alloc_len: 49152,                                    │   ║
║  │     actual_len: 45678,                                   │   ║
║  │     entry_count: 1000                                    │   ║
║  │   }                                                      │   ║
║  └──────────────────────────────────────────────────────────┘   ║
╚══════════════════════════════════════════════════════════════════╝
```

### Metadata Lookup: latest_for_column("users", "id")

```
Step 1: Acquire read lock on TableCatalog
          │
          ▼
Step 2: Get column catalog
          column_catalog = tables["users"].columns["id"]
          │
          ▼
Step 3: Get last range entry
          last_entry = column_catalog.ranges.last()
          → Entry { start: 1000, end: 2000, versions: [...] }
          │
          ▼
Step 4: Get last MVCC version
          last_version = last_entry.versions.last()
          → MVCCEntry { page_id: "p2", commit: 200 }
          │
          ▼
Step 5: Lookup page metadata
          metadata = page_metadata["p2"]
          │
          ▼
Step 6: Build descriptor (lightweight copy)
          PageDescriptor {
            id: "p2",
            disk_path: "storage/data.00000",
            offset: 49152,
            alloc_len: 49152,
            actual_len: 45678,
            entry_count: 1000
          }
          │
          ▼
Step 7: Release read lock, return descriptor
```

### Metadata Update: register_batch()

```
Input: Vec<PendingPage> for atomic batch insert

Step 1: Acquire WRITE lock on TableCatalog
          (blocks all readers and other writers)
          │
          ▼
Step 2: For each PendingPage:
          a) Generate unique page ID
             page_id = format!("p{}", atomic_counter.fetch_add(1))
          │
          b) Insert into page_metadata map
             page_metadata[page_id] = Arc::new(PageMetadata { ... })
          │
          c) Update column's range list
             If replace_last:
               ranges.last_mut().versions.push(MVCCEntry { page_id, commit: now })
             Else:
               ranges.push(Entry { start, end, versions: [MVCCEntry { ... }] })
          │
          ▼
Step 3: Release write lock
          │
          ▼
Step 4: Return Vec<PageDescriptor>
          (all columns committed atomically)
```

---

## Level 5: Complete Request Trace

### Trace: SELECT with Filtering

```
╔═══════════════════════════════════════════════════════════════╗
║ SQL: SELECT name, salary FROM employees WHERE age > 30       ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
[1] Parse SQL → AST
      sqlparser::Parser::parse_sql(sql)
          │
          ▼
[2] Plan query
      executor::execute_select()
      • Required columns: [name, salary, age]
      • WHERE filter: age > 30
          │
          ▼
[3] Locate data
      PageDirectory::locate_range("employees", 0, MAX)
      → Returns: [PageDescriptor{"p100"}, PageDescriptor{"p101"}]
          │
          ▼
[4] Fetch pages
      PageHandler::get_pages([p100, p101])
      ├─ Check UPC (miss both)
      ├─ Check CPC (hit p100, miss p101)
      │   └─ Decompress p100 → UPC
      └─ Fetch p101 from disk → CPC → decompress → UPC
      → Returns: [Arc<Page>, Arc<Page>]
          │
          ▼
[5] Convert to columnar batch
      For each page.entries:
        Parse Entry.data as string
        Try parse as Int64
        If fail, try Float64
        If fail, store as Text
      → ColumnarBatch {
          columns: {
            0 → ColumnarPage { data: Text(["Alice", "Bob", ...]) },
            1 → ColumnarPage { data: Int64([80000, 90000, ...]) },
            2 → ColumnarPage { data: Int64([28, 35, ...]) }
          },
          num_rows: 2000
        }
          │
          ▼
[6] Apply WHERE filter
      evaluate_selection_on_batch(&where_expr, &batch)
      • Evaluate: age > 30
      • For each row i:
          if age_column[i] > 30:
            selection_bitmap.set(i)
      → Bitmap: [0,1,0,1,1,0,...]
          │
          ▼
[7] Gather filtered rows
      selected_indices = selection_bitmap.iter_ones().collect()
      → [1, 3, 4, ...]  (1200 indices)
      batch = batch.gather(&selected_indices)
      → ColumnarBatch with 1200 rows
          │
          ▼
[8] Project columns
      Select only [name, salary] columns
      Convert ColumnarBatch → Vec<Vec<String>>
      → [
          ["Alice", "80000"],
          ["Bob", "90000"],
          ...
        ]
          │
          ▼
[9] Return results
```

### Trace: INSERT with Batching

```
╔═══════════════════════════════════════════════════════════════╗
║ SQL: INSERT INTO users (id, name) VALUES (1, 'Alice')        ║
╚═════════════════════════╤═════════════════════════════════════╝
                          │
                          ▼
[1] Parse INSERT → Extract values
      executor::execute_insert()
      • Table: "users"
      • Columns: ["id", "name"]
      • Values: [("1", "Alice")]
          │
          ▼
[2] Create UpdateJob
      UpdateJob {
        table: "users",
        columns: [
          ColumnUpdate {
            column: "id",
            operations: [BufferRow { row: ["1"] }]
          },
          ColumnUpdate {
            column: "name",
            operations: [BufferRow { row: ["Alice"] }]
          }
        ]
      }
          │
          ▼
[3] Submit to Writer
      writer.submit(job)
      • Non-blocking, queues job in channel
          │
          ▼
[4] Worker thread receives job
      worker_context.handle_job(job)
          │
          ▼
[5] Buffer row
      buffered_rows["users"].push(["1", "Alice"])
      • buffered_rows.len() = 1 (< 1000 threshold)
      • Return (don't flush yet)
      ... (999 more inserts) ...
          │
          ▼
[6] Threshold reached (1000th insert)
      flush_page_group("users", buffered_rows)
          │
          ├─▶ [6a] Sort rows by ORDER BY columns
          │
          ├─▶ [6b] For each column:
          │     stage_column()
          │     ├─ Create Page with 1000 entries
          │     ├─ Serialize with bincode
          │     └─ Allocate disk space
          │     → StagedColumn { serialized, allocation, ... }
          │
          ├─▶ [6c] Commit metadata (atomic)
          │     metadata_client.commit(table, staged)
          │     ├─ Generate page IDs: ["p1001", "p1002"]
          │     ├─ Update column chains
          │     └─ Return descriptors
          │
          ├─▶ [6d] Persist to disk
          │     For each (staged, descriptor):
          │       ├─ Zero-pad buffer
          │       ├─ write_all_at(buffer, offset)
          │       └─ fsync
          │
          └─▶ [6e] Cache writeback
                For each page:
                  page_handler.write_back_uncompressed(id, page)
                  → Insert into UPC
          │
          ▼
[7] Return success
```

---

## Performance Characteristics

### Cache Hit Rates

```
Scenario: Random reads with 10-entry UPC
  Working set: 100 unique pages
  Hit rate: ~10% (UPC) + ~10% (CPC) = ~20% total
  → 80% requests hit disk

Scenario: Sequential scan (same pages)
  Working set: 5 pages
  Hit rate: ~100% after initial load
  → Amortized disk cost across many queries

Scenario: Write-heavy workload
  Insertions trigger page group flushes (1000 rows)
  Flushed pages immediately in UPC (100% hit rate)
  Reads of recently written data: ~100% UPC hit rate
```

### Columnar Batch Processing

```
Operation              Row-by-row    Columnar Batch
──────────────────────────────────────────────────────
WHERE age > 30         1 eval/row    1 eval/word (64 bits)
  (1000 rows)          1000 ops      ~16 ops + bitmap ops

SUM(salary)            1 parse/row   1 parse/row (but vectorized)
  (1000 rows)          1000 parses   Potential SIMD: 4× speedup

Sort by 2 columns      2 key builds  Batch key build
  (1000 rows)          per row       → 1 allocation for all keys

Memory layout          Cache-hostile Cache-friendly
                       (scattered)   (sequential access)
```

### Write Amplification

```
Scenario: Single row insert (without batching)
  1 row → 1 Page write → ~4 KiB minimum (4K alignment)
  Write amplification: ~4000× (1 byte → 4096 bytes)

Scenario: Page group batching (1000 rows)
  1000 rows → 1 Page write → ~48 KiB (45 KB actual + 3 KB padding)
  Write amplification: ~1.06× (45 KB → 48 KB)
  → 99% reduction in write amplification
```

---

## Related Documentation

- [Architecture Overview](architecture_overview) - Component-level design
- [SQL Executor](sql_executor) - Query execution engine
- [Columnar Batch](columnar_batch) - Batch processing internals
- [Writer](writer) - Write execution and atomicity
- [Page Handler](page_handler) - Cache orchestration
- [Metadata Store](metadata_store) - Catalog management
- [Cache System](cache) - Two-tier LRU caches
