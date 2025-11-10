---
title: "System Overview - Visual Guide"
layout: default
nav_order: 4
---

# System Overview - Visual Guide

Progressive deep-dive from 30,000ft to implementation details.

---

## SQL Processing Pipeline

```
╔═══════════════════════════════════════════════════════════════╗
║                       SQL Query Text                          ║
║         "SELECT id FROM users WHERE age > 18"                 ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                       SQL PARSER                              ║
║                    (parser.rs)                                ║
║  • Lexical analysis (tokenization)                            ║
║  • Syntax analysis                                            ║
║  • AST construction                                           ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │ Statement (AST)
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                     QUERY PLANNER                             ║
║                    (planner.rs)                               ║
║  • Extract table names                                        ║
║  • Collect read/write columns                                ║
║  • Build filter expression trees                             ║
║  • Validate support                                           ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │ QueryPlan
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                   PIPELINE BUILDER                            ║
║                   (builder.rs)                                ║
║  • Extract leaf filters                                       ║
║  • Group by column                                            ║
║  • Create execution steps                                     ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │ Pipeline
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                  QUERY EXECUTOR                               ║
║                   (future)                                    ║
║  • Apply filters step-by-step                                 ║
║  • Materialize results                                        ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## Level 1: 30,000ft View

```
╔═══════════════════════════════════════════════════════════════╗
║                         CLIENT                                ║
║              (upsert, update, range_scan)                     ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                       OPS HANDLER                             ║
║                   (API boundary)                              ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                                ▼
╔═══════════════════════════════════════════════════════════════╗
║                      PAGE HANDLER                             ║
║              (cache orchestration)                            ║
╚═══════════════════════════════╤═══════════════════════════════╝
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
         ╔═══════════╗   ╔═══════════╗   ╔═══════════╗
         ║    UPC    ║   ║    CPC    ║   ║   DISK    ║
         ║   (hot)   ║──▶║  (cold)   ║──▶║ (persist) ║
         ╚═══════════╝   ╚═══════════╝   ╚═══════════╝
```

---

## Level 2: Component Architecture

```
╔════════════════════════════════════════════════════════════════════╗
║                           METADATA LAYER                           ║
║  ┌──────────────────────┐         ┌─────────────────────────┐     ║
║  │  TableMetaStore      │◀────────│   PageDirectory         │     ║
║  │  - col_data          │         │   (façade)              │     ║
║  │  - page_data         │         │   - latest()            │     ║
║  │  - MVCC versions     │         │   - range()             │     ║
║  └──────────────────────┘         │   - lookup()            │     ║
║                                   └─────────────────────────┘     ║
╚════════════════════════════════════════════════════════════════════╝
                                   │
                                   ▼
╔════════════════════════════════════════════════════════════════════╗
║                         HANDLER LAYER                              ║
║                                                                    ║
║  ┌─────────────┐    ┌──────────────┐    ┌────────────────────┐   ║
║  │PageLocator  │    │ PageFetcher  │    │ PageMaterializer   │   ║
║  │- metadata   │    │ - CPC        │    │ - UPC              │   ║
║  │  lookups    │    │ - PageIO     │    │ - Compressor       │   ║
║  └─────────────┘    └──────────────┘    └────────────────────┘   ║
║         │                   │                      │              ║
║         └───────────────────┴──────────────────────┘              ║
║                             │                                     ║
║                    ┌────────▼────────┐                            ║
║                    │  PageHandler    │                            ║
║                    │  (coordinator)  │                            ║
║                    └─────────────────┘                            ║
╚════════════════════════════════════════════════════════════════════╝
                                   │
                                   ▼
╔════════════════════════════════════════════════════════════════════╗
║                          CACHE LAYER                               ║
║                                                                    ║
║  ╔════════════════════════════════════════════════════════════╗   ║
║  ║  UPC (Uncompressed Page Cache)                             ║   ║
║  ║  HashMap<PageId, Arc<Page>> + BTreeSet (LRU)               ║   ║
║  ║  MAX: 10 entries                                           ║   ║
║  ╚════════════════════╤═══════════════════════════════════════╝   ║
║                       │ eviction                                  ║
║                       ▼                                           ║
║         ╔═══════════════════════════════════════╗                 ║
║         ║ UncompressedToCompressedLifecycle     ║                 ║
║         ║ (compress via bincode + lz4)          ║                 ║
║         ╚═════════════════╤═══════════════════════               ║
║                           ▼                                       ║
║  ╔════════════════════════════════════════════════════════════╗   ║
║  ║  CPC (Compressed Page Cache)                               ║   ║
║  ║  HashMap<PageId, Arc<Vec<u8>>> + BTreeSet (LRU)            ║   ║
║  ║  MAX: 10 entries                                           ║   ║
║  ╚════════════════════╤═══════════════════════════════════════╝   ║
║                       │ eviction                                  ║
║                       ▼                                           ║
║         ╔═══════════════════════════════════════╗                 ║
║         ║ CompressedToDiskLifecycle             ║                 ║
║         ║ (flush to disk via PageIO)            ║                 ║
║         ╚═════════════════╤═══════════════════════               ║
║                           ▼                                       ║
╚═══════════════════════════╪═══════════════════════════════════════╝
                            │
                            ▼
╔════════════════════════════════════════════════════════════════════╗
║                         STORAGE LAYER                              ║
║                                                                    ║
║  ┌──────────────────────────────────────────────────────────┐     ║
║  │  Writer                                                  │     ║
║  │  ├─ DirectBlockAllocator (256 KiB blocks, 4 KiB tail)    │     ║
║  │  ├─ rotate files: storage/data.00000, data.00001, …      │     ║
║  │  └─ persists padded payloads via O_DIRECT writes         │     ║
║  │                                                          │     ║
║  │  PageIO                                                  │     ║
║  │  ├─ reads aligned regions (alloc_len)                    │     ║
║  │  └─ trims to actual_len before handing to callers        │     ║
║  └──────────────────────────────────────────────────────────┘     ║
╚════════════════════════════════════════════════════════════════════╝
```

---

## Level 3: Data Flow - Write Path

```
Pipeline-driven UPDATE/DELETE write
      │
      ▼
╔═══════════════════════════════════════════════════════════════╗
║ writer::submit(UpdateJob)                                     ║
╚═══════════════════════════════════════════════════════════════╝
      │
      ├─▶ stage_column                                          
      │        ├─▶ PageDirectory.latest(+PageHandler::get_page) 
      │        ├─▶ apply row mutations                          
      │        └─▶ serialize updated page                       
      │
      ├─▶ DirectBlockAllocator.allocate(actual_len)             
      │        └─▶ returns { path, offset, alloc_len }          
      │
      ├─▶ metadata.register_batch(pending_pages)                
      │        └─▶ replaces/appends column chain tail           
      │
      ├─▶ persist_allocation                                    
      │        └─▶ zero-pad buffer, write alloc_len bytes       
      │
      └─▶ PageHandler::write_back_uncompressed (cache refresh)  
               │
               ▼
          ╔═══════════════════════════════╗
          ║ UPC.add("p42", page)          ║
          ╚═══════════════════════════════╝
               │
               ▼ (if UPC full)
          ╔═══════════════════════════════╗
          ║ Evict oldest → Lifecycle      ║
          ║   └─▶ Compress → CPC          ║
          ╚═══════════════════════════════╝
```

---

## Level 4: Data Flow - Read Path (Range Scan)

```
Client range_scan("temperature", 1000, 5000, ts)
      │
      ▼
╔═══════════════════════════════════════════════════════════════╗
║ ops_handler::range_scan_column_entry                          ║
╚═══════════════════════════════════════════════════════════════╝
      │
      ├─▶ handler.locate_range("temperature", 1000, 5000, ts)
      │        │
      │        ▼
      │   ╔════════════════════════════════════════════════════╗
      │   ║ PageDirectory::range()                            ║
      │   ║   └─▶ TableMetaStore.get_ranged_pages_meta()     ║
      │   ╚════════════════════════════════════════════════════╝
      │        │
      │        ├─▶ Binary search col_data[column]
      │        │     └─▶ Find ranges overlapping [1000, 5000)
      │        │
      │        ├─▶ For each range:
      │        │     └─▶ Binary search MVCC versions
      │        │          └─▶ Pick newest commit_time ≤ ts
      │        │
      │        └─▶ Return [PageDescriptor{p5}, PageDescriptor{p7}, ...]
      │
      └─▶ handler.get_pages(descriptors)
               │
               ▼
          ╔═══════════════════════════════════════════════════╗
          ║ PageHandler::get_pages (batch)                    ║
          ╚═══════════════════════════════════════════════════╝
               │
               ├─▶ STEP 1: UPC sweep
               │     └─▶ Collect all hits in order
               │
               ├─▶ STEP 2: CPC sweep
               │     └─▶ Collect remaining, decompress
               │
               ├─▶ STEP 3: Disk fetch
               │     └─▶ Read missing pages
               │
               └─▶ STEP 4: Final UPC pass
                     └─▶ Return all pages in original order
               │
               ▼
          [Arc<Page>, Arc<Page>, Arc<Page>, ...]
               │
               ▼
          Concatenate all page.entries
               │
               ▼
          Vec<Entry>
```

---

## Level 5: Cache Eviction Chain

```
╔═════════════════════════════════════════════════════════════════╗
║                    UPC (10 entries max)                         ║
║  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐           ║
║  │ p1 │ p2 │ p3 │ p4 │ p5 │ p6 │ p7 │ p8 │ p9 │p10 │           ║
║  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘           ║
║                                                                 ║
║  New insert (p11) triggers eviction of p1 (oldest)             ║
╚═════════════════════════════╤═══════════════════════════════════╝
                              │
                              ▼
                ╔═════════════════════════════════╗
                ║ UncompressedToCompressedLifecycle║
                ╚═════════════════════════════════╝
                              │
                              ├─▶ Compressor.compress(p1)
                              │     └─▶ Page → bincode → lz4_flex
                              │
                              └─▶ CPC.add("p1", compressed_blob)
                                    │
                                    ▼
╔═════════════════════════════════════════════════════════════════╗
║                    CPC (10 entries max)                         ║
║  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐           ║
║  │ c1 │ c2 │ c3 │ c4 │ c5 │ c6 │ c7 │ c8 │ c9 │c10 │           ║
║  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘           ║
║                                                                 ║
║  New insert (p1 compressed) triggers eviction of c1 (oldest)   ║
╚═════════════════════════════╤═══════════════════════════════════╝
                              │
                              ▼
                ╔═════════════════════════════════╗
                ║ CompressedToDiskLifecycle       ║
                ╚═════════════════════════════════╝
                              │
                              ├─▶ PageDirectory.lookup("c1")
                              │     └─▶ PageDescriptor{path, offset}
                              │
                              └─▶ PageIO.write_to_path(path, offset, blob)
                                    │
                                    ▼
╔═════════════════════════════════════════════════════════════════╗
║                         DISK                                    ║
║                                                                 ║
║  page_file_0.bin:                                               ║
║  ┌───────────────┬─────────────────────────────────────┐        ║
║  │ 64B Metadata  │  Compressed Page Blob               │        ║
║  │ {read_size}   │  [lz4 compressed data...]           │        ║
║  └───────────────┴─────────────────────────────────────┘        ║
╚═════════════════════════════════════════════════════════════════╝
```

---

## Level 5.5: Batch Operations & Prefetching

```
╔═══════════════════════════════════════════════════════════════╗
║               BATCH READ (io_uring on Linux)                  ║
╚═══════════════════════════════════════════════════════════════╝

Input: [PageDescriptor{p1, file1.db, 0},
        PageDescriptor{p2, file1.db, 4096},
        PageDescriptor{p3, file2.db, 0}]
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 1: Group by disk_path                                   ║
║   file1.db → [p1@0, p2@4096]                                  ║
║   file2.db → [p3@0]                                           ║
╚═══════════════════════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 2: For each file (parallel via io_uring)                ║
║                                                               ║
║   Phase 1: Submit all metadata reads (64B each)              ║
║   ┌─────────┬─────────┐                                      ║
║   │ Meta p1 │ Meta p2 │  ──▶ io_uring submission queue       ║
║   └─────────┴─────────┘                                      ║
║                                                               ║
║   Phase 2: Submit all payload reads                          ║
║   ┌─────────┬─────────┐                                      ║
║   │ Data p1 │ Data p2 │  ──▶ io_uring submission queue       ║
║   └─────────┴─────────┘                                      ║
║                                                               ║
║   Wait for all completions (parallel I/O)                    ║
║   Single fsync per file                                      ║
╚═══════════════════════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 3: Single write lock                                    ║
║   Insert all pages into CPC at once                           ║
╚═══════════════════════════════════════════════════════════════╝

Performance: ~N× faster than sequential for N pages in same file


╔═══════════════════════════════════════════════════════════════╗
║               BATCH WRITE (io_uring on Linux)                 ║
╚═══════════════════════════════════════════════════════════════╝

Input: [(offset, data), (offset, data), ...]
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 1: Prepare all buffers                                  ║
║   For each write:                                             ║
║     • Serialize metadata (rkyv)                               ║
║     • Pad to 64B                                              ║
║     • Combine with compressed page data                       ║
║     • Store in stable buffer                                  ║
╚═══════════════════════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 2: Submit all Write opcodes                             ║
║   ┌──────┬──────┬──────┬──────┐                              ║
║   │Write1│Write2│Write3│Write4│  ──▶ io_uring queue          ║
║   └──────┴──────┴──────┴──────┘                              ║
║   (all writes submitted in parallel)                          ║
╚═══════════════════════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 3: Wait for completions                                 ║
║   All writes complete in parallel                             ║
╚═══════════════════════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════════════════════╗
║ Step 4: Single Fsync                                          ║
║   One fsync for all writes                                    ║
╚═══════════════════════════════════════════════════════════════╝


╔═══════════════════════════════════════════════════════════════╗
║                      PREFETCHING                              ║
╚═══════════════════════════════════════════════════════════════╝

Request: get_pages_with_prefetch([p1, p2, p3, p4, p5], k=2)
        │
        ├────────────────────┬─────────────────────────────┐
        ▼                    ▼                             │
   IMMEDIATE PATH      PREFETCH PATH                       │
        │                    │                             │
        ▼                    ▼                             │
  Return [p1, p2]     Send [p3, p4, p5]                    │
  (blocking)          to channel (non-blocking)            │
                             │                             │
                             ▼                             │
                 ╔═══════════════════════════╗             │
                 ║   Prefetch Thread         ║             │
                 ║   (background worker)     ║             │
                 ╚═══════════════════════════╝             │
                             │                             │
        ┌────────────────────┴────────────┐                │
        ▼                                 ▼                │
  recv_timeout(1ms)                  Work arrives?         │
        │                                 │                │
        ├─▶ Timeout → loop               └─▶ YES          │
        │                                     │            │
        └─▶ Disconnected → exit               ▼            │
                                    ╔═════════════════╗    │
                                    ║ Drain channel   ║    │
                                    ║ immediately     ║    │
                                    ╚═════════════════╝    │
                                             │             │
                                             ▼             │
                              Batch: [p3, p4, p5, ...]    │
                              (auto-batches overlapping    │
                               requests from multiple      │
                               get_pages_with_prefetch     │
                               calls)                      │
                                             │             │
                                             ▼             │
                              ensure_pages_cached(batch)   │
                                             │             │
                                             ▼             │
                              Load into UPC/CPC            │
                                             │             │
                                             └─────────────┘
                                                   │
                                                   ▼
                                     Ready for next request
                                     (within 1-2ms)

Performance characteristics:
• First k pages: blocking, immediate return
• Remaining pages: background prefetch within 1-2ms
• Zero CPU when idle (blocked on recv_timeout)
• Natural batching when requests overlap
```

---

## Level 6: Metadata Structure

```
╔═════════════════════════════════════════════════════════════════╗
║                      TableMetaStore                             ║
╚═════════════════════════════════════════════════════════════════╝

col_data: HashMap<ColumnName, Arc<RwLock<Vec<TableMetaStoreEntry>>>>

"temperature" ──▶ Arc<RwLock<Vec<TableMetaStoreEntry>>>
                        │
                        ├─▶ Entry[0]: [0, 1024)
                        │     └─▶ page_metas: [
                        │           MVCCKeeperEntry{p1, commit=100},
                        │           MVCCKeeperEntry{p2, commit=150},
                        │         ]
                        │
                        ├─▶ Entry[1]: [1024, 2048)
                        │     └─▶ page_metas: [
                        │           MVCCKeeperEntry{p3, commit=200},
                        │         ]
                        │
                        └─▶ Entry[2]: [2048, 4096)
                              └─▶ page_metas: [
                                    MVCCKeeperEntry{p4, commit=250},
                                  ]

page_data: HashMap<PageId, Arc<PageMetadata>>

"p1" ──▶ Arc<PageMetadata{ id: "p1", disk_path: "/data/0.bin", offset: 0 }>
"p2" ──▶ Arc<PageMetadata{ id: "p2", disk_path: "/data/0.bin", offset: 4096 }>
"p3" ──▶ Arc<PageMetadata{ id: "p3", disk_path: "/data/1.bin", offset: 0 }>
"p4" ──▶ Arc<PageMetadata{ id: "p4", disk_path: "/data/1.bin", offset: 8192 }>


╔═════════════════════════════════════════════════════════════════╗
║                      PageDirectory                              ║
║                      (Façade)                                   ║
╚═════════════════════════════════════════════════════════════════╝

latest("temperature")
      │
      └─▶ Read lock col_data["temperature"]
           └─▶ Get last TableMetaStoreEntry
                └─▶ Get last MVCCKeeperEntry
                     └─▶ Lookup page_data[page_id]
                          └─▶ Return PageDescriptor (clone)

range("temperature", 1000, 5000, ts=200)
      │
      └─▶ Read lock col_data["temperature"]
           │
           ├─▶ Binary search: find ranges overlapping [1000, 5000)
           │     └─▶ Matches: Entry[0], Entry[1], Entry[2]
           │
           ├─▶ For each entry:
           │     └─▶ Binary search MVCC: find version with commit ≤ 200
           │          └─▶ Entry[0]: p2 (commit=150)
           │          └─▶ Entry[1]: p3 (commit=200)
           │          └─▶ Entry[2]: p4 (commit=250) ❌ too new, skip
           │
           └─▶ Return [PageDescriptor{p2}, PageDescriptor{p3}]
```

---

## Level 7: Data Structures

```
╔═════════════════════════════════════════════════════════════════╗
║                          Entry                                  ║
╚═════════════════════════════════════════════════════════════════╝

struct Entry {
    prefix_meta: String,    // reserved
    data: String,           // "22.5"
    suffix_meta: String,    // reserved
}


╔═════════════════════════════════════════════════════════════════╗
║                           Page                                  ║
╚═════════════════════════════════════════════════════════════════╝

struct Page {
    page_metadata: String,
    entries: Vec<Entry>,    // [Entry{data: "21.0"}, Entry{data: "22.5"}, ...]
}

In Memory:
┌──────────────────────────────────────────────────────────┐
│ Page                                                     │
│  ├─ page_metadata: ""                                    │
│  └─ entries: [                                           │
│       Entry{ prefix_meta: "", data: "21.0", suffix: "" },│
│       Entry{ prefix_meta: "", data: "22.5", suffix: "" },│
│       Entry{ prefix_meta: "", data: "23.1", suffix: "" },│
│     ]                                                    │
└──────────────────────────────────────────────────────────┘

On Disk (compressed):
┌────────────┬──────────────────────────────────────────┐
│ 64B Header │ LZ4(bincode(Page))                       │
│ {size: N}  │ [compressed bytes...]                    │
└────────────┴──────────────────────────────────────────┘


╔═════════════════════════════════════════════════════════════════╗
║                      PageCacheEntry                             ║
╚═════════════════════════════════════════════════════════════════╝

struct PageCacheEntry<T> {
    page: Arc<T>,           // Arc to avoid cloning
    used_time: u64,         // epoch millis, for LRU
}

UPC: PageCacheEntry<PageCacheEntryUncompressed>
      └─▶ PageCacheEntryUncompressed { page: Page }

CPC: PageCacheEntry<PageCacheEntryCompressed>
      └─▶ PageCacheEntryCompressed { page: Vec<u8> }


╔═════════════════════════════════════════════════════════════════╗
║                         PageCache<T>                            ║
╚═════════════════════════════════════════════════════════════════╝

struct PageCache<T> {
    store: HashMap<PageId, PageCacheEntry<T>>,
    lru_queue: BTreeSet<(used_time, PageId)>,
    lifecycle: Option<Arc<dyn CacheLifecycle<T>>>,
}

Example UPC state:
store: {
    "p1" => PageCacheEntry{ page: Arc<Page{...}>, used_time: 1000 },
    "p5" => PageCacheEntry{ page: Arc<Page{...}>, used_time: 2000 },
    "p9" => PageCacheEntry{ page: Arc<Page{...}>, used_time: 3000 },
}

lru_queue: BTreeSet {
    (1000, "p1"),  ← oldest, evicted first
    (2000, "p5"),
    (3000, "p9"),  ← newest
}
```

---

## Level 8: Thread Safety & Ownership

```
╔═════════════════════════════════════════════════════════════════╗
║                    Arc/RwLock Topology                          ║
╚═════════════════════════════════════════════════════════════════╝

main.rs creates:

Arc<RwLock<PageCache<Uncompressed>>>  ─┐
                                       ├─▶ PageMaterializer
Arc<Compressor>  ──────────────────────┘

Arc<RwLock<PageCache<Compressed>>>  ───┐
                                       ├─▶ PageFetcher
Arc<PageIO>  ───────────────────────────┘

Arc<RwLock<TableMetaStore>>  ──────────▶ PageDirectory
                                              │
                                              ├─▶ PageLocator
                                              └─▶ CompressedToDiskLifecycle

Arc<PageDirectory>  ────────────────────┐
                                        ├─▶ PageLocator
                                        └─▶ CompressedToDiskLifecycle


Thread Safety Guarantees:

UPC/CPC operations:
  - Read locks: get_cached(), collect_cached()
  - Write locks: add(), evict()
  - Locks held briefly (dropped before decompression)

TableMetaStore operations:
  - Read locks: latest(), range(), lookup()
  - Write locks: register_page()
  - PageDescriptor cloned before lock drop

Lifecycle callbacks:
  - Execute on evicting thread
  - Hold cache write lock during callback
  - Compression/IO blocks eviction
```

---

## Level 9: Complete Request Trace

```
╔═════════════════════════════════════════════════════════════════╗
║ Client: upsert_data_into_column(handler, "temp", "22.5")       ║
╚═════════════════════════════════════════════════════════════════╝
                            │
      ┌─────────────────────┴─────────────────────┐
      │                                           │
      ▼                                           ▼
[1] locate_latest("temp")                   [2] get_page(desc)
      │                                           │
      ▼                                           ▼
  PageDirectory                             PageMaterializer
      │                                     .get_cached("p42")
      ▼                                           │
  RwLock::read()                                  ▼
      │                                     UPC.store.get("p42")
      ▼                                           │
  col_data["temp"]                                ├─▶ HIT
      │                                           │   └─▶ Arc<Page>
      └─▶ last entry                              │
           └─▶ last MVCC                          ├─▶ MISS
                └─▶ page_id="p42"                 │   │
                     │                            │   ▼
                     ▼                            │   PageFetcher
                page_data["p42"]                  │   .get_cached("p42")
                     │                            │        │
                     └─▶ PageDescriptor           │        ├─▶ HIT
                                                  │        │   └─▶ decompress
      ┌───────────────────────────────────────────┘        │       └─▶ UPC.add
      │                                                    │
      ▼                                                    └─▶ MISS
[3] Clone & Mutate                                             │
      │                                                        ▼
      ├─▶ let mut page = (*arc).clone()                   PageFetcher
      ├─▶ page.add_entry(Entry::new("22.5"))              .fetch_and_insert
      │                                                         │
      └─▶ [4] write_back_uncompressed("p42", page)             ▼
               │                                          PageIO::read
               ▼                                               │
          PageMaterializer                                     ├─▶ seek(offset)
          .write_back("p42", page)                             ├─▶ read 64B meta
               │                                               ├─▶ read N bytes
               ▼                                               │
          UPC.write().add("p42", page)                         └─▶ compressed blob
               │                                                    │
               ├─▶ store.insert("p42", entry)                      ▼
               ├─▶ lru_queue.insert((now, "p42"))            CPC.add("p42", blob)
               │                                                    │
               └─▶ if lru_queue.len() > 10:                        └─▶ decompress
                     │                                                  └─▶ UPC.add
                     └─▶ evict(oldest_id)
                           │
                           └─▶ lifecycle.on_evict(id, data)
                                     │
                                     ▼
                           UncompressedToCompressed
                           .on_evict("p99", Arc<Page>)
                                     │
                                     ├─▶ compress(page)
                                     │     └─▶ bincode + lz4
                                     │
                                     └─▶ CPC.add("p99", blob)
                                           │
                                           └─▶ if CPC full:
                                                 evict oldest
                                                   │
                                                   ▼
                                             CompressedToDisk
                                             .on_evict("c55", blob)
                                                   │
                                                   ├─▶ lookup("c55")
                                                   │     └─▶ descriptor
                                                   │
                                                   └─▶ PageIO::write
                                                         │
                                                         └─▶ disk
```

---

## Level 10: SQL Parser - AST Transformation

```
╔═══════════════════════════════════════════════════════════════╗
║ Input: "SELECT id, name FROM users WHERE age > 18"           ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                    TOKENIZATION                               ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
    [SELECT] [id] [,] [name] [FROM] [users] [WHERE] [age] [>] [18]
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                      PARSING                                  ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                Abstract Syntax Tree                           ║
╚═══════════════════════════════════════════════════════════════╝

Statement::Query
    └── body: SetExpr::Select
        ├── projection: Vec<SelectItem>
        │   ├── UnnamedExpr(Identifier("id"))
        │   └── UnnamedExpr(Identifier("name"))
        │
        ├── from: Vec<TableWithJoins>
        │   └── TableWithJoins
        │       └── relation: TableFactor::Table
        │           └── name: ObjectName("users")
        │
        └── selection: Option<Expr>
            └── BinaryOp
                ├── left: Identifier("age")
                ├── op: Gt
                └── right: Value(Number("18"))
```

---

## Level 11: Query Planner - Plan Construction

```
╔═══════════════════════════════════════════════════════════════╗
║ Input: Statement::Query (AST)                                 ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                    plan_statement()                           ║
║                         │                                     ║
║         ┌───────────────┴───────────────┐                     ║
║         ▼                               ▼                     ║
║   Statement::Query              Statement::Insert/           ║
║         │                       Update/Delete                ║
║         ▼                               │                     ║
║   plan_query()                          ├─▶ plan_insert()    ║
║         │                               ├─▶ plan_update()    ║
║         ▼                               └─▶ plan_delete()    ║
║   plan_select()                                              ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                   ANALYSIS PHASE                              ║
╚═══════════════════════════════════════════════════════════════╝
                            │
           ┌────────────────┼────────────────┐
           ▼                ▼                ▼
    Extract Table    Collect Columns   Build Filters
         │                 │                 │
         ▼                 ▼                 ▼
    "users"       {id, name, age}     FilterExpr::Leaf
                                         (age > 18)
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                      QueryPlan                                ║
║                                                               ║
║  tables: [                                                    ║
║    TableAccess {                                              ║
║      table_name: "users",                                     ║
║      read_columns: {"id", "name", "age"},                     ║
║      write_columns: {},                                       ║
║      filters: Some(FilterExpr::Leaf(age > 18))               ║
║    }                                                          ║
║  ]                                                            ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## Level 12: FilterExpr Tree Structure

```
╔═══════════════════════════════════════════════════════════════╗
║ WHERE age > 18 AND (region = 'US' OR vip = true)             ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                    build_filter()                             ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
                    FilterExpr::And
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
        FilterExpr::Leaf        FilterExpr::Or
         (age > 18)                     │
                            ┌───────────┴───────────┐
                            ▼                       ▼
                    FilterExpr::Leaf        FilterExpr::Leaf
                    (region = 'US')         (vip = true)


Visual representation:

                        AND
                    ┌────┴────┐
                    │         │
                  Leaf       OR
               (age > 18)  ┌──┴──┐
                           │     │
                         Leaf   Leaf
                      (region  (vip
                       = 'US')  = true)
```

---

## Level 13: Pipeline Builder - Step Generation

```
╔═══════════════════════════════════════════════════════════════╗
║ Input: QueryPlan with TableAccess                             ║
║   filters: And([Leaf(age > 18), Leaf(name = 'John')])        ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                  build_pipeline()                             ║
╚═══════════════════════════════════════════════════════════════╝
                            │
           ┌────────────────┼────────────────┐
           ▼                ▼                ▼
    Extract Leaves   Group by Column   Create Steps
           │                │                │
           ▼                ▼                ▼
    [Leaf(age>18),   {"age": [Leaf],   PipelineStep[
     Leaf(name='J')]  "name": [Leaf]}   {col, filters}]
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                          Job                                  ║
║                                                               ║
║  table_name: "users"                                          ║
║  cost: 2                                                      ║
║  steps: [                                                     ║
║    PipelineStep {                                             ║
║      column: "age",                                           ║
║      filters: [Leaf(age > 18)],                               ║
║      is_root: true                                            ║
║    },                                                         ║
║    PipelineStep {                                             ║
║      column: "name",                                          ║
║      filters: [Leaf(name = 'John')],                          ║
║      is_root: false                                           ║
║    }                                                          ║
║  ]                                                            ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## Level 14: Pipeline Executor

```
╔═══════════════════════════════════════════════════════════════╗
║                   Input: Job                                  ║
║  table: "users"                                               ║
║  cost: 3                                                      ║
║  steps: [age, name, status]                                   ║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
╔═══════════════════════════════════════════════════════════════╗
║                   Pipeline Executor                           ║
║                                                               ║
║  Job Queue (MPMC)                                             ║
║    • Producers: planner/builder                               ║
║    • Consumers: main workers                                  ║
║                                                               ║
║  Main Workers (≈85% threads)                                  ║
║    1. recv() Job                                              ║
║    2. insert Job into JobBoard (SkipSet ordered by cost)      ║
║    3. send wake-up (bool) to reserve channel                  ║
║    4. call job.get_next() (CAS claim + run PipelineStep)      ║
║                                                               ║
║  Reserve Workers (≈15% threads)                               ║
║    1. block on wake-up channel                                ║
║    2. pop heaviest Job from JobBoard                          ║
║    3. call job.get_next() (dup execution allowed)             ║
║                                                               ║
║  JobBoard = crossbeam_skiplist::SkipSet<JobHandle>            ║
║    • cost-major ordering, seq tie-breaker                     ║
║    • lock-free steals for reserve workers                     ║
╚═══════════════════════════════════════════════════════════════╝

Execution characteristics:
• No extra mutexes: skip list + channels do the coordination  
• Main pool keeps ingesting Jobs; reserve pool relieves pressure  
• Duplicated executions are intentional for heavy Jobs  
• Future work: actual row filtering still runs inside job/step `get_next` / `execute`
```

---

## Level 15: Complete SQL Query Flow

```
╔═══════════════════════════════════════════════════════════════╗
║ Client: "SELECT id FROM users WHERE age > 18 AND name='John'"║
╚═══════════════════════════════════════════════════════════════╝
                            │
                            ▼
[1] parse_sql(sql)
                            │
                            ▼
                    GenericDialect
                            │
                            ▼
                    Parser::parse_sql
                            │
                            ├─▶ Lexer tokenizes input
                            ├─▶ Parser builds AST
                            └─▶ Statement::Query
                                     │
                                     ▼
[2] plan_statement(&stmt)
                            │
                            ▼
                    plan_query()
                            │
                            ├─▶ extract_table_name() → "users"
                            ├─▶ collect_select_columns() → {id}
                            ├─▶ collect_expr_columns(WHERE) → {age, name}
                            └─▶ build_filter(WHERE) → FilterExpr::And
                                     │
                                     ▼
                            TableAccess {
                              table: "users",
                              read: {id, age, name},
                              filters: And([age>18, name='John'])
                            }
                                     │
                                     ▼
                            QueryPlan {
                              tables: [TableAccess]
                            }
                                     │
                                     ▼
[3] build_pipeline(&plan)
                            │
                            ├─▶ extract_leaf_filters()
                            │     └─▶ [Leaf(age>18), Leaf(name='John')]
                            │
                            ├─▶ group_filters_by_column()
                            │     └─▶ {"age": [...], "name": [...]}
                            │
                            └─▶ create PipelineSteps
                                     │
                                     ▼
                            Job {
                              table: "users",
                              cost: 2,
                              steps: [
                                {col: "age", filters: [...]},
                                {col: "name", filters: [...]}
                              ]
                            }
                                     │
                                     ▼
[4] Execute (future)
                            │
                            ├─▶ Fetch all rows from "users"
                            ├─▶ Apply step 1: filter age column
                            ├─▶ Apply step 2: filter name column
                            └─▶ Project id column
                                     │
                                     ▼
                            Result rows
```
