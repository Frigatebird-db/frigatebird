---
title: "Operations Handler"
layout: default
nav_order: 12
---

# Operations Handler

The `ops_handler` module exposes the high-level APIs for mutating and reading column data. It acts as the boundary between external operations and the internal caching/metadata machinery.

## API

- `upsert_data_into_column(handler, col, data) -> Result<bool>`
- `insert_sorted_row(handler, table, &[(&str, &str)]) -> Result<()>`
- `read_row(handler, table, row_idx) -> Result<Vec<String>>`
- `overwrite_row(handler, table, row_idx, &[String]) -> Result<()>`
- `delete_row(handler, table, row_idx) -> Result<()>`
- `update_column_entry(handler, col, data, row) -> Result<bool>`
- `range_scan_column_entry(handler, col, l_row, r_row, ts) -> Vec<Entry>`

## Architectural Position

```
      External Client / API Layer
                    │
                    ▼
           ╔══════════════════════╗
           ║    ops_handler       ║
           ║  (upsert/update/scan)║
           ╚══════════╤═══════════╝
                      │
                      ▼
           PageHandler façade (locator + caches)
```

Each function coordinates metadata lookups with page retrieval/manipulation, keeping locks short and relying on `PageHandler` to manage caches.

## Sorted Row Inserts (`insert_sorted_row`)

Tables with ORDER BY clauses maintain sorted order across inserts. The implementation differs based on whether the ORDER BY is single-column or multi-column.

**See [Sorted Inserts](sorted_inserts) for complete documentation including:**
- Binary search (O(log n)) for single-column ORDER BY
- Linear scan (O(n)) for multi-column ORDER BY
- Type-aware comparison logic (numeric-first, then lexicographic)
- Empty string defaults for non-provided columns
- UPDATE/DELETE behavior with ORDER BY columns

### Quick Overview

```
Client row (k columns)
        │
        ▼
╔═══════════════════════════════════════════════╗
║ Validate inputs                               ║
║  • load TableCatalog                          ║
║  • ensure all ORDER BY columns provided       ║
╚═══════════════════════════════════════════════╝
        │
        ▼
╔═══════════════════════════════════════════════╗
║ Find insertion point                          ║
║  • Single-column: binary search O(log n)      ║
║  • Multi-column: linear scan O(n)             ║
╚═══════════════════════════════════════════════╝
        │
        ▼ shared index `idx`
╔═══════════════════════════════════════════════╗
║ For each column                               ║
║  • fetch latest page                          ║
║  • splice entry at `idx` (InsertAt)           ║
║  • update metadata entry count                ║
╚═══════════════════════════════════════════════╝
```

## Row-Level Utilities

```
╔═══════════╦═══════════════╦════════════════════════════════╗
║ Function  ║ Purpose       ║ Notes                          ║
╠═══════════╬═══════════════╬════════════════════════════════╣
║ read_row  ║ Materialises  ║ Uses PageHandler::read_entry_at║
║           ║ full logical  ║ for each column; returns Vec   ║
║           ║ row           ║ of strings in column order     ║
╠═══════════╬═══════════════╬════════════════════════════════╣
║ overwrite_║ In-place row  ║ Overwrites every column at the ║
║ row       ║ rewrite       ║ specified logical index        ║
╠═══════════╬═══════════════╬════════════════════════════════╣
║ delete_row║ Remove row    ║ Drops the entry in each column ║
║           ║ from all      ║ and updates metadata entry     ║
║           ║ columns       ║ counts                         ║
╚═══════════╩═══════════════╩════════════════════════════════╝
```

These helpers power the UPDATE/DELETE paths in the SQL executor. They reuse the
same page-level mechanics (`InsertAt`, metadata deltas) so the writer thread
does not gain any new behavior.

## Upsert Flow (`upsert_data_into_column`)

```
Input: column name, payload string

╔═══════════════════════════════════════════════════════════╗
║ Step 1: Ask handler.locate_latest(col)                    ║
║   ▶ Returns PageDescriptor                                ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 2: Fetch page via handler.get_page(descriptor)       ║
║   ▶ Returns Arc<PageCacheEntryUncompressed>               ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 3: Clone the underlying page                         ║
║   ▶ (*page_arc).clone()                                   ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 4: Append new Entry::new(data) to page.entries       ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 5: Write back via handler.write_back_uncompressed    ║
║   ▶ Push change into UPC                                  ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 6: Return Ok(true)                                   ║
╚═══════════════════════════════════════════════════════════╝
```

### ASCII Sequence

```
Client → ops_handler::upsert_data_into_column
      │
      ▼
PageHandler.locate_latest(col) ──▶ PageDescriptor{id="pX", …}
      │
      ▼ PageHandler::get_page("pX")
      │
PageHandler ──▶ UPC/CPC/Disk (as needed) ──▶ Arc<PageCacheEntryUncompressed>
      │
      ▼ clone, add Entry::new(data)
      │
handler.write_back_uncompressed("pX", updated_page)
```

**Limitation:** Does not update `(start_idx,end_idx)` ranges or metadata when page grows.

## Update Flow (`update_column_entry`)

```
Input: column name, payload string, row index

Same steps as upsert to retrieve Page.

After cloning Page:
      │
      ├─▶ if row >= entries.len():
      │     └─▶ return Ok(false)
      │
      └─▶ else:
            └─▶ entries[row] = Entry::new(data)

Insert updated page back into UPC.
```

Squarely relies on `PageHandler` for fetching the latest committed version and uses row index to overwrite the existing entry.

## Metadata Synchronisation

Both `insert_sorted_row` and the single-column helper update metadata via `PageHandler::update_entry_count_in_table`, ensuring the prefix arrays driving `locate_row`/`locate_range` stay accurate after in-page inserts.

## Range Scan Flow (`range_scan_column_entry`)

```
Input: column name, [l_row, r_row), commit_time_upper_bound

╔═══════════════════════════════════════════════════════════╗
║ Step 1: Call handler.locate_range(...)                    ║
║   ▶ Returns Vec<PageDescriptor>                           ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 2: Pass descriptors to handler.get_pages             ║
║   ▶ Returns Vec<Arc<PageCacheEntryUncompressed>>          ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 3: Iterate through returned pages                    ║
║   ▶ Concatenate their Page::entries                       ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ Step 4: Return Vec<Entry>                                 ║
╚═══════════════════════════════════════════════════════════╝
```

### ASCII Diagram

```
╔═════════════════════════════╗
║ PageHandler::locate_range   ║
╚═════════════╤═══════════════╝
              ▼
   [PageDescriptor{"p5"}, PageDescriptor{"p7"}, …]
              │
              ▼
╔═════════════════════════════╗
║ PageHandler::get_pages      ║
╚═════════════╤═══════════════╝
              ▼
[Arc<PageCacheEntryUncompressed>, …]
              │
              ▼
Vec<Entry> (concatenate pages sequentially)
```

**Limitation:** Returns entire pages; no row slicing within page boundaries.

## Error Handling

- Returns `Result<bool>` but internally `.unwrap()`s — panics if metadata or caches not populated
- Range scans panic if column missing from metadata store

## Integration

- Requires metadata primed with at least one page per column
- Cache writes go through UPC; lifecycle handles compression/flush
