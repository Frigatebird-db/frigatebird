---
title: "Metadata Store"
layout: default
nav_order: 16
---

# Metadata Store

The metadata store is the catalog that maps each logical column to its physical
page chain. It no longer tracks MVCC timestamps or per-range buckets. Instead,
we maintain an append-only list of the latest page versions plus a prefix-sum
array that lets us binary-search by logical row id.

## Key Types

| Type | Purpose |
| --- | --- |
| `PageDescriptor` | Plain data describing the current on-disk page (id, path, offset, alloc_len, actual_len, entry_count) |
| `ColumnChain` | In-memory append-only list of descriptors and a matching prefix array |
| `TableMetaStore` | Owns the `HashMap<column, ColumnChain>` and a `HashMap<page_id, PageDescriptor>` |
| `PageDirectory` | Thread-safe façade exposing `latest`, `locate_row`, `locate_range`, `range`, `lookup`, and registration helpers |
| `PendingPage` | Batch-friendly request used by the writer to publish multiple descriptors at once |

## Column Chains

Each column gets a `ColumnChain`:

```
ColumnChain {
    pages: [PageDescriptor { entry_count: 3, … },
            PageDescriptor { entry_count: 8, … }, …],
    prefix_entries: [3, 11, …]   // cumulative row counts
}
```

* `pages` is append-only; the newest descriptor is always at the end.
* `prefix_entries[i]` stores the cumulative row count up to and including
  `pages[i]`. We update the tail using a SIMD-friendly helper whenever we
  replace the last descriptor.
* Binary-searching the prefix array gives us `O(log n)` lookups for
  `locate_row` and the bounds needed for `locate_range`.

## PageDescriptor Fields

| Field | Description |
| --- | --- |
| `id` | Internally generated 64-bit hex string |
| `disk_path` | Full path to the page file returned by the allocator |
| `offset` | 4 KiB-aligned file offset |
| `alloc_len` | Bytes reserved on disk (multiple of 4 KiB) |
| `actual_len` | Bytes of real payload stored in that allocation |
| `entry_count` | Logical row count contained in the page |
| `stats` | Optional per-column min/max/null-count metadata used by the executor to skip pages |

Both lengths are stored so reads can issue a single direct I/O request for the
aligned region (`alloc_len`) and then trim the tail back to `actual_len`.

## Registration APIs

Most callers use the façade in `PageDirectory`:

```rust
pub fn latest(&self, column: &str) -> Option<PageDescriptor>;
pub fn locate_row(&self, column: &str, row: u64) -> Option<RowLocation>;
pub fn locate_range(&self, column: &str, start: u64, end: u64) -> Vec<PageSlice>;
pub fn range(&self, column: &str, l: u64, r: u64, _ts: u64) -> Vec<PageDescriptor>;
pub fn lookup(&self, id: &str) -> Option<PageDescriptor>;
pub fn register_page(&self, column: &str, path: String, offset: u64) -> Option<PageDescriptor>;
pub fn register_page_with_sizes(&self,
    column: &str,
    path: String,
    offset: u64,
    alloc_len: u64,
    actual_len: u64,
    entry_count: u64,
) -> Option<PageDescriptor>;
pub fn update_latest_entry_count(&self,
    table: &str,
    column: &str,
    entry_count: u64,
) -> Result<(), CatalogError>;
```

`register_page_with_sizes` is the workhorse used by the writer. It expects both
lengths so the metadata store can keep the prefix sums and on-disk footprint in
sync. `update_latest_entry_count` lets higher-level components (sorted inserts,
compaction rewrites) adjust the tail descriptor when an in-place mutation
changes the logical row count without rewriting the physical page. Both
functions accept `entry_count == 0`, allowing callers to register an empty page
and populate it lazily.

## Lookup Helpers

* **`latest(column)`** – returns the newest descriptor by taking the tail of
  the column chain.
* **`locate_row(column, row)`** – binary-searches the prefix array, returns the
  descriptor plus the row offset inside that page.
* **`locate_range(column, start, end)`** – finds the first and last page touched
  by the requested span and produces a `Vec<PageSlice>` describing the per-page
  offsets we should read.
* **`range(column, l, r, _ts)`** – convenience wrapper that drops the offsets
  and returns just the descriptors; the timestamp parameter is currently ignored
  (all callers see the latest data).

## Batch Publication

Writers publish new descriptors by building a `Vec<PendingPage>` and calling
`PageDirectory::register_batch`. Each `PendingPage` includes:

```
PendingPage {
    column: String,
    disk_path: String,
    offset: u64,
    alloc_len: u64,
    actual_len: u64,
    entry_count: u64,
    replace_last: bool,
    stats: Option<ColumnStats>,
}
```

`replace_last` allows the writer to atomically swap in a freshly written page
(for example, after rewriting an existing range) without rebuilding the entire
chain.

## Concurrency

* Reads acquire a shared lock on the column chain, clone the descriptors they
  need, and drop the lock immediately.
* Writes grab a single `Mutex` in the writer, stage all allocations, and then
  call `register_batch`, which takes the `TableMetaStore` write lock once to
  append or replace descriptors. The critical section is small: just pointer
  swaps and prefix updates.

## Why This Design

* Prefix sums let us answer “which page contains row X?” in logarithmic time.
* Keeping both `alloc_len` and `actual_len` makes the writer and reader agree on
  how much direct I/O to perform while keeping tail padding minimal.
* Dropping MVCC timestamps simplifies the fast path; if we need multi-version
  readers later, we can layer that on top of the existing column chains without
  reintroducing nested vectors.
