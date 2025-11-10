---
title: "Page Handler"
layout: default
nav_order: 17
---

# Page Handler

`PageHandler` used to be a monolith that talked to the metadata store, both caches, the compressor, and PageIO directly. It has now been decomposed into smaller services that make responsibilities explicit and shrink lock scope:

```
╔══════════════╗     ╔════════════════╗     ╔══════════════════╗
║ PageLocator  ║────▶║  PageFetcher   ║────▶║PageMaterializer  ║
╚══════╤═══════╝     ╚═══════╤════════╝     ╚═══════╤══════════╝
       │                     │                      │
       │                     │                      │
       ▼                     ▼                      ▼
 TableMetaStore        CPC + PageIO          UPC + Compressor
```

`PageHandler` itself is now a thin coordinator that wires these services together and exposes a stable API to the rest of the codebase.

## Components

**PageLocator:**
- Wraps `PageDirectory`
- Returns `PageDescriptor` (lightweight, no Arc)
- Methods: `latest_for_column`, `range_for_column`, `lookup`, `locate_row_in_table`

**PageFetcher:**
- Owns CPC + `PageIO`
- Methods: `get_cached`, `collect_cached`, `fetch_and_insert` (disk read → CPC)

**PageMaterializer:**
- Owns UPC + `Compressor`
- Methods: `get_cached`, `collect_cached`, `materialize_one/many` (decompress → UPC), `write_back`

**PageHandler:**
- Wires above three
- Spawns prefetch background thread
- API: `locate_latest`, `locate_range`, `locate_row_in_table`, `read_entry_at`, `get_page`, `get_pages`, `get_pages_with_prefetch`, `ensure_pages_cached`, `write_back_uncompressed`

### Row Lookup Helper

```
locate_row_in_table(table, column, row_idx)
        │
        ▼
╔══════════════════════════════════════╗
║ PageLocator.locate_row_in_table      ║
║  • binary search prefix array        ║
║  • returns descriptor + page offset  ║
╚══════════════════════════════════════╝
        │
        ▼
read_entry_at(table, column, row_idx)
        │
        ├─▶ get_page(descriptor) → UPC/CPC/disk
        └─▶ entries[page_offset]
```

`read_entry_at` is used by multi-column ORDER BY inserts and tests to materialise the exact row without re-implementing prefix maths.

## `get_page` Flow

```
Input: PageDescriptor { id, disk_path, offset }

╔═══════════════════════════════════════════════════════════╗
║ Step 1: UPC fast path                                     ║
║   ▶ if materializer.get_cached(id) hits                   ║
║     └─▶ return Arc immediately                            ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼ (miss)
╔═══════════════════════════════════════════════════════════╗
║ Step 2: CPC fast path                                     ║
║   ▶ if fetcher.get_cached(id) hits:                       ║
║     └─▶ materializer.materialize_one(id, compressed_arc)  ║
║         └─▶ return Arc                                    ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼ (miss)
╔═══════════════════════════════════════════════════════════╗
║ Step 3: Disk fallback                                     ║
║   ▶ comp_arc = fetcher.fetch_and_insert(descriptor)       ║
║     └─▶ uses PageIO + inserts to CPC                      ║
║   ▶ materializer.materialize_one(id, comp_arc)            ║
║     └─▶ populates UPC, returns Arc                        ║
╚═══════════════════════════════════════════════════════════╝
```

All expensive work (decompression and I/O) happens outside of cache locks. Each successful materialization ends with an `Arc<PageCacheEntryUncompressed>` stored in UPC and returned to the caller.

## `get_pages` Flow

```
Inputs: Vec<PageDescriptor> preserving user order

Initialize:
  • order := [page ids]
  • meta_map := { id -> descriptor }
  • result := []
  • already_pushed := {}

╔═══════════════════════════════════════════════════════════╗
║ Step 1: UPC sweep (single read lock)                      ║
║   ▶ materializer.collect_cached(order) returns hits       ║
║   ▶ Push hits in order, mark already_pushed               ║
║   ▶ Drop lock                                             ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 2: CPC sweep (single read lock)                      ║
║   ▶ fetcher.collect_cached(order) returns blobs           ║
║   ▶ materializer.materialize_many(blobs) outside locks    ║
║   ▶ Remove those ids from meta_map                        ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 3: Disk fetch                                        ║
║   ▶ For each descriptor still in meta_map:                ║
║     • comp = fetcher.fetch_and_insert(descriptor)         ║
║     • Stash (id, comp)                                    ║
║   ▶ materializer.materialize_many(fetched)                ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 4: Final UPC read                                    ║
║   ▶ Iterate `order`                                       ║
║   ▶ Push any ids not in already_pushed                    ║
╚═══════════════════════════════════════════════════════════╝
```

**Key properties:**
- Preserves input order
- Minimizes lock contention (two read passes)
- Decompresses outside locks

## Prefetching

`PageHandler` spawns a background thread that polls a channel every 1ms (tunable via `PREFETCH_POLL_INTERVAL_MS`) to prefetch pages that will be needed soon.

**API:**

`get_pages_with_prefetch(page_ids: &[String], k: usize) -> Vec<Arc<PageCacheEntryUncompressed>>`
- Returns first `k` pages immediately (blocking)
- Sends remaining `page_ids[k..]` to prefetch thread (non-blocking)
- Used when caller knows subsequent pages will be needed very soon

`ensure_pages_cached(page_ids: &[String])`
- Ensures pages are loaded into caches
- Returns immediately (no result)
- Called by prefetch thread to warm caches

**Prefetch Thread Flow:**

```
Loop:
  ╔═══════════════════════════════════════════════════════════╗
  ║ Wait for work (recv_timeout 1ms)                          ║
  ║   ▶ timeout? continue                                     ║
  ║   ▶ recv batch of page_ids                                ║
  ╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║ Drain channel immediately (try_recv loop)                 ║
  ║   ▶ Collect all pending requests                          ║
  ║   ▶ Flatten into single batch                             ║
  ╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║ ensure_pages_cached(batch)                                ║
  ║   ▶ locate_by_ids → Vec<PageDescriptor>                   ║
  ║   ▶ get_pages(descriptors) → loads into UPC               ║
  ╚═══════════════════════════════════════════════════════════╝
```

**Properties:**
- 1ms poll interval balances responsiveness and CPU usage
- Automatic batching when multiple requests arrive simultaneously
- Non-blocking send from caller (fire-and-forget)
- Pages loaded into UPC ready for immediate access

## Integration

- **ops_handler** calls `locate_latest()` / `locate_range()` → `get_page(s)` → `write_back_uncompressed`
- **Lifecycles** automatically demote UPC → CPC → disk
- **Metadata** uses `PageDescriptor` (lightweight clone) instead of `Arc<PageMetadata>`
