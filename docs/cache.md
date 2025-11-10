---
title: "Cache System"
layout: default
nav_order: 7
---

# Cache System

The cache system implements a two-tier LRU cache architecture with lifecycle hooks that automatically manage page eviction across tiers. It consists of the Uncompressed Page Cache (UPC), Compressed Page Cache (CPC), and lifecycle callbacks that connect them.

## Structure

```rust
pub struct PageCache<T> {
    store: HashMap<PageId, PageCacheEntry<T>>,
    lru_queue: BTreeSet<(used_time, PageId)>,
    lifecycle: Option<Arc<dyn CacheLifecycle<T>>>,
}
```

Fixed LRU size: 10 entries per cache.

## Core Structures

### PageCacheEntry

```
╔═════════════════════════════╗   ╔══════════════════════════════╗
║ PageCacheEntry<T>           ║   ║ PageCacheEntryUncompressed   ║
║  ├─ page     : Arc<T>       ║   ║  └─ page : Page              ║
║  └─ used_time: u64          ║   ╚══════════════════════════════╝
╚═════════════════════════════╝
╔══════════════════════════════╗
║ PageCacheEntryCompressed     ║
║  └─ page : Vec<u8> (blob)    ║
╚══════════════════════════════╝
```

- `Arc<T>` allows multiple consumers to share a cached page without copying.
- `used_time` stores the access timestamp (epoch millis) driving the LRU ordering.

### PageCache<T>

```
PageCache<T>
├─ store     : HashMap<PageId, PageCacheEntry<T>>
├─ lru_queue : BTreeSet<(used_time, PageId)>
└─ lifecycle : Option<Arc<dyn CacheLifecycle<T>>>
```

- `store` provides O(1) lookups by page ID.
- `lru_queue` maintains a sorted set of `(used_time, PageId)` pairs, enabling quick retrieval of the oldest entry.
- `lifecycle` is an optional callback invoked whenever an entry is evicted, allowing cache tiers to chain work together (compress, flush, etc.).
- `LRUsize` is fixed at 10 (tunable later); when exceeded, the cache evicts the least recently used entry.

## Flow: Adding & Evicting Entries

```
PageCache::add(id, page)
      │
      ├─▶ if id already present:
      │     └─▶ remove old (used_time, id) from lru_queue
      │
      ├─▶ used_time := current_epoch_millis()
      │
      ├─▶ store[id] := PageCacheEntry { page: Arc::new(page), used_time }
      │
      └─▶ lru_queue.insert((used_time, id))
            │
            └─▶ if lru_queue.len() > LRUsize:
                  oldest := lru_queue.iter().next()
                  evict(oldest_id)
```

**Eviction + Lifecycle**

```
evict(id)
      │
      ├─▶ remove entry from store (yielding PageCacheEntry<T>)
      │
      ├─▶ remove (used_time, id) from lru_queue
      │
      └─▶ if lifecycle set:
            lifecycle.on_evict(id, Arc::clone(&entry.page))
```

The lifecycle hook allows each cache to notify its downstream tier:

- UPC registers `UncompressedToCompressedLifecycle`, which compresses the evicted page and inserts it into CPC.
- CPC registers `CompressedToDiskLifecycle`, which looks up the matching `PageDescriptor` via `PageDirectory` and writes the compressed bytes to disk with `PageIO`.

## ASCII Diagram: Dual Cache Setup

```
╔═══════════════════════════╗
║ Uncompressed Page Cache   ║
║ (UPC)                     ║
║  store: { "p1": Arc<Page>,║
║           "p3": Arc<Page> }║
║  lru : { (t=101,"p3"),    ║
║          (t=150,"p1") }   ║
╚═══════════╤═══════════════╝
            │ Arc<PageCacheEntryUncompressed>
            ▼
     Hot Page consumers

╔═══════════════════════════╗
║ Compressed Page Cache     ║
║ (CPC)                     ║
║  store: { "p2": Arc<Vec<u8>> } ║
║  lru : { (t=80,"p2") }         ║
╚═══════════╤════════════════════╝
            │ Arc<PageCacheEntryCompressed>
            ▼
      PageHandler → Compressor → UPC
```

## Access Patterns

```
get(id)
      │
      ├─▶ lookup store[id]
      └─▶ return Arc::clone(&entry.page)

has(id)
      │
      └─▶ store.contains_key(id)
```

**Note:** `get` does not update `used_time`. Call `add` to refresh LRU position.

---

## Compressor (`src/helpers/compressor.rs`)

Stateless helper bridging uncompressed `Page` and compressed bytes.

**API:**
- `compress(Arc<PageCacheEntryUncompressed>) -> PageCacheEntryCompressed`
- `decompress(Arc<PageCacheEntryCompressed>) -> PageCacheEntryUncompressed`

```
Arc<PageCacheEntryUncompressed>
       │
       ▼ Extract Page struct
       │
bincode::serialize(Page) → Vec<u8>
       │
       ▼ lz4_flex::compress_prepend_size
       │
Vec<u8> (compressed blob)
       │
       └─▶ PageCacheEntryCompressed { page: Vec<u8> }
```

```
                 ╔═══════════════════════╗
                 ║  Page (struct)        ║
                 ║  • page_metadata      ║
                 ║  • entries: Vec<Entry>║
                 ╚═══════════╤═══════════╝
                             │
         bincode serialize   │   lz4 compress (size prepended)
 Arc<PageCacheEntryUncompressed> ────────────────────────────▶ Vec<u8>
                             │
                             ▼
                 PageCacheEntryCompressed
```

---

## Lifecycle Hooks (`src/cache/lifecycle.rs`)

Chain cache evictions across tiers.

```
╔═══════════════════════════════════════════════════════════╗
║ UPC eviction                                              ║
║   └─▶ UncompressedToCompressedLifecycle::on_evict        ║
║         ├─▶ Compressor::compress(page)                    ║
║         └─▶ CPC.add(id, compressed_page)                  ║
╚═══════════════════════════════════════════════════════════╝
                         │
                         ▼
╔═══════════════════════════════════════════════════════════╗
║ CPC eviction                                              ║
║   └─▶ CompressedToDiskLifecycle::on_evict                ║
║         ├─▶ PageDirectory::lookup(id) → descriptor        ║
║         └─▶ PageIO::write_to_path(disk_path, offset, page)║
╚═══════════════════════════════════════════════════════════╝
```

### CacheLifecycle Trait

```rust
pub trait CacheLifecycle<T>: Send + Sync {
    fn on_evict(&self, id: &str, data: Arc<T>);
}
```

Invoked by `PageCache::evict()` after removing entry.

### UncompressedToCompressedLifecycle

```
on_evict(id, uncompressed_page)
      │
      ├─▶ compressor.compress(uncompressed_page) → compressed_blob
      │
      └─▶ compressed_cache.write().add(id, compressed_blob)
```

### CompressedToDiskLifecycle

```
on_evict(id, compressed_page)
      │
      ├─▶ directory.lookup(id) → Option<PageDescriptor>
      │     └─▶ if None: skip
      │
      └─▶ page_io.write_to_path(
            descriptor.disk_path,
            descriptor.offset,
            compressed_page.page
          )
```

### Wiring

```rust
// UPC → CPC
let upc_lifecycle = Arc::new(UncompressedToCompressedLifecycle::new(
    Arc::clone(&compressor),
    Arc::clone(&compressed_page_cache),
));
uncompressed_page_cache.write().unwrap().set_lifecycle(Some(upc_lifecycle));

// CPC → disk
let cpc_lifecycle = Arc::new(CompressedToDiskLifecycle::new(
    Arc::clone(&page_io),
    Arc::clone(&page_directory),
));
compressed_page_cache.write().unwrap().set_lifecycle(Some(cpc_lifecycle));
```

## Integration

- **PageHandler** holds `Arc<RwLock<PageCache<_>>>` for both caches
- **Lifecycle** hooks move data between tiers automatically
- **PageDirectory** provides disk coordinates for CPC lifecycle evictions
