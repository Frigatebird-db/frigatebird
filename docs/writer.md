---
title: "Writer Module"
layout: default
nav_order: 15
---

# Writer Module

The Writer is Satori's **write execution subsystem**, responsible for serializing update operations, coordinating with the allocator for disk space, and persisting pages to storage with metadata atomicity guarantees.

---

## Architecture Overview

```
┌──────────────┐
│  UpdateJob   │  (Table + ColumnUpdate[])
└──────┬───────┘
       │ submit()
       ▼
┌─────────────────────────────────────┐
│           Writer                    │
│  ┌───────────────────────────────┐  │
│  │  Crossbeam Channel (Unbounded)│  │
│  └───────────┬───────────────────┘  │
│              │                       │
│              ▼                       │
│  ┌───────────────────────────────┐  │
│  │     WorkerContext             │  │
│  │  (Single Background Thread)   │  │
│  └───────────┬───────────────────┘  │
│              │                       │
└──────────────┼───────────────────────┘
               │
               ▼
      ┌────────────────┐
      │  handle_job()  │
      └────────┬───────┘
               │
    ┌──────────┴──────────┐
    │  For Each Column:   │
    │                     │
    │  1. stage_column()  │
    │  2. commit()        │
    │  3. persist()       │
    └─────────────────────┘
```

---

## Components

### 1. UpdateJob

**Purpose**: Unit of work describing mutations across multiple columns.

**Structure** (`src/writer/update_job.rs`):
```rust
pub struct UpdateJob {
    pub table: String,
    pub columns: Vec<ColumnUpdate>,
}

pub struct ColumnUpdate {
    pub column: String,
    pub operations: Vec<UpdateOp>,
}

pub enum UpdateOp {
    Overwrite { row: u64, entry: Entry },
    Append { entry: Entry },
}
```

**Semantics**:
- **Overwrite**: Replace logical row `N` with new entry (auto-extends page with empty entries if needed)
- **Append**: Push entry to end of page's entry vector

**Builder Pattern**:
```rust
let job = UpdateJob::new("users", vec![
    ColumnUpdate::new("email", vec![
        UpdateOp::Append { entry: Entry::new("user@example.com") },
    ]),
]);
```

---

### 2. Writer

**Purpose**: Orchestrates write execution via a dedicated background thread.

**Key Fields** (`src/writer/executor.rs:205-209`):
```rust
pub struct Writer {
    tx: Sender<WriterMessage>,              // Job submission channel
    handle: Option<JoinHandle<()>>,         // Worker thread handle
    is_shutdown: Arc<AtomicBool>,           // Shutdown coordination flag
}
```

**Lifecycle**:

```
 new()                submit()              shutdown()               Drop
   │                     │                      │                      │
   │                     │                      │                      │
   ▼                     ▼                      ▼                      ▼
┌──────┐             ┌──────┐              ┌──────┐              ┌──────┐
│Spawn │────────────▶│Queue │─────────────▶│CAS   │─────────────▶│Join  │
│Thread│             │Job   │              │Flag  │              │Worker│
└──────┘             └──────┘              └──┬───┘              └──────┘
   │                                           │
   │                                           ▼
   └─────────────────────────────────▶Send Shutdown Message
```

**Thread Safety**:
- `is_shutdown` uses **Acquire/Release** semantics for clean shutdown coordination
- `compare_exchange(false, true, AcqRel, Acquire)` ensures only one shutdown wins

---

### 3. WorkerContext

**Purpose**: Execution environment for the background worker thread.

**Dependencies**:
```
┌───────────────────┐
│  WorkerContext    │
├───────────────────┤
│ page_handler      │──▶ PageHandler (cache/IO coordination)
│ allocator         │──▶ PageAllocator (disk space allocation)
│ metadata          │──▶ MetadataClient (catalog updates)
│ rx                │──▶ Receiver<WriterMessage>
└───────────────────┘
```

**Worker Loop** (`src/writer/executor.rs:267-276`):
```rust
fn run_worker(ctx: WorkerContext, shutdown_flag: Arc<AtomicBool>) {
    while let Ok(message) = ctx.rx.recv() {
        match message {
            WriterMessage::Job(job) => ctx.handle_job(job),
            WriterMessage::Shutdown => {
                shutdown_flag.store(true, Ordering::Release);
                break;
            }
        }
    }
}
```

---

## Write Execution Flow

### Phase 1: Staging

**Goal**: Prepare each column's new page version in memory.

```
For Each Column in UpdateJob:
    ┌─────────────────────────────────────────┐
    │        stage_column()                   │
    └─────────────┬───────────────────────────┘
                  │
    ┌─────────────▼──────────────────┐
    │ 1. Fetch Latest PageDescriptor │  (via MetadataClient)
    └─────────────┬──────────────────┘
                  │
    ┌─────────────▼──────────────────┐
    │ 2. Load Base Page from Cache   │  (via PageHandler::get_page)
    │    or Create Empty Page         │
    └─────────────┬──────────────────┘
                  │
    ┌─────────────▼──────────────────┐
    │ 3. Apply Operations in Order   │
    │    - Overwrite: page.entries[row] = entry
    │    - Append:    page.entries.push(entry)
    └─────────────┬──────────────────┘
                  │
    ┌─────────────▼──────────────────┐
    │ 4. Serialize Page with bincode │
    └─────────────┬──────────────────┘
                  │
    ┌─────────────▼──────────────────┐
    │ 5. Allocate Disk Space         │  (via PageAllocator::allocate)
    └─────────────┬──────────────────┘
                  │
                  ▼
           StagedColumn { serialized, allocation, page, ... }
```

**Code** (`src/writer/executor.rs:155-189`):
```rust
fn stage_column(&self, table: &str, update: ColumnUpdate) -> Option<StagedColumn> {
    // 1. Get latest version
    let latest = self.metadata.latest_descriptor(table, &update.column);

    // 2. Load base page or start fresh
    let base_page = latest.as_ref()
        .and_then(|desc| self.page_handler.get_page(desc.clone()));

    let mut prepared = base_page
        .map(|page| (*page).clone())
        .unwrap_or_else(|| PageCacheEntryUncompressed { page: Page::new() });

    // 3. Apply operations
    apply_operations(&mut prepared, &update.operations);

    // 4. Serialize
    let serialized = bincode::serialize(&prepared.page)?;
    let actual_len = serialized.len() as u64;

    // 5. Allocate space
    let allocation = self.allocator.allocate(actual_len)?;

    Some(StagedColumn {
        column: update.column,
        page: prepared,
        entry_count: prepared.page.entries.len() as u64,
        disk_path: allocation.path,
        offset: allocation.offset,
        actual_len,
        alloc_len: allocation.alloc_len,
        serialized,
        replace_last: latest.is_some(),  // Replace vs Append to chain
    })
}
```

**Auto-Extension on Overwrite** (`src/writer/executor.rs:322-330`):
```rust
fn ensure_capacity(page: &mut Page, row_idx: usize) {
    if row_idx < page.entries.len() {
        return;
    }
    // Pad with empty entries up to row_idx
    let missing = row_idx + 1 - page.entries.len();
    page.entries.extend((0..missing).map(|_| Entry::new("")));
}
```

---

### Phase 2: Metadata Commit

**Goal**: Atomically publish all new page versions to the catalog.

```
┌────────────────────────────┐
│  Build MetadataUpdate[]    │
│  (from StagedColumn[])     │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ MetadataClient::commit()   │
│                            │
│ ┌────────────────────────┐ │
│ │ DirectoryMetadata      │ │
│ │ Client calls           │ │
│ │ PageDirectory::        │ │
│ │ register_batch()       │ │
│ └────────────────────────┘ │
└────────────┬───────────────┘
             │
             ▼
      Returns Vec<PageDescriptor> with generated IDs
```

**MetadataUpdate Structure** (`src/writer/executor.rs:29-38`):
```rust
pub struct MetadataUpdate {
    pub column: String,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
    pub replace_last: bool,  // Replace last in chain vs append new version
}
```

**Commit Implementation** (`src/writer/executor.rs:81-98`):
```rust
impl MetadataClient for DirectoryMetadataClient {
    fn commit(&self, _table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor> {
        let pending: Vec<PendingPage> = updates.into_iter()
            .map(|update| PendingPage {
                column: update.column,
                disk_path: update.disk_path,
                offset: update.offset,
                alloc_len: update.alloc_len,
                actual_len: update.actual_len,
                entry_count: update.entry_count,
                replace_last: update.replace_last,
            })
            .collect();

        // Atomic batch registration with ID generation
        self.directory.register_batch(&pending)
    }
}
```

**Atomicity Guarantee**:
- `register_batch()` acquires **write lock** on PageDirectory
- All columns committed together or none at all
- IDs generated monotonically via epoch timestamps

---

### Phase 3: Disk Persistence

**Goal**: Write serialized pages to allocated disk locations.

```
For Each (StagedColumn, PageDescriptor) Pair:

┌────────────────────────────┐
│   persist_allocation()     │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ 1. Zero-pad buffer to      │
│    alloc_len (4K aligned)  │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ 2. Copy serialized bytes   │
│    to buffer start         │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ 3. Create parent dirs      │
│    (fs::create_dir_all)    │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ 4. Open file with O_CREAT  │
│    (Linux: O_DIRECT)       │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ 5. write_all_at(offset)    │  Unix: pwrite()
│    (or seek_write Windows) │  Windows: WriteFile with OVERLAPPED
└────────────────────────────┘
```

**Implementation** (`src/writer/executor.rs:279-305`):
```rust
fn persist_allocation(prepared: &StagedColumn, descriptor: &PageDescriptor) -> io::Result<()> {
    // Zero-initialized aligned buffer
    let mut buffer = vec![0u8; descriptor.alloc_len as usize];
    buffer[..prepared.actual_len as usize].copy_from_slice(&prepared.serialized);

    let path = Path::new(&descriptor.disk_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    #[cfg(target_family = "unix")]
    {
        file.write_all_at(&buffer, descriptor.offset)?;
    }

    #[cfg(target_family = "windows")]
    {
        file.seek_write(&buffer, descriptor.offset)?;
    }

    Ok(())
}
```

**Note**: O_DIRECT is set on file open in `allocator.rs`, not here.

---

### Phase 4: Cache Writeback

**Goal**: Insert the new page version into the uncompressed cache.

```
┌────────────────────────────┐
│  Update page.page_metadata │
│  with new descriptor.id    │
└────────────┬───────────────┘
             │
┌────────────▼───────────────┐
│ PageHandler::              │
│   write_back_uncompressed()│
│                            │
│  Inserts into UPC with new │
│  ID, potentially evicting  │
│  oldest entries            │
└────────────────────────────┘
```

**Code** (`src/writer/executor.rs:148-152`):
```rust
let mut page = prepared.page;
page.page.page_metadata = descriptor.id.clone();
self.page_handler.write_back_uncompressed(&descriptor.id, page);
```

**Effect**:
- Next read of this page can hit uncompressed cache immediately
- Old page version (if any) remains valid until evicted (MVCC-lite)

---

## Complete Job Execution

**Putting it all together** (`src/writer/executor.rs:114-153`):

```
handle_job(UpdateJob):
    │
    ├─▶ staged = []
    │   For each column:
    │       stage_column() ──▶ staged.push(StagedColumn)
    │
    ├─▶ metadata_updates = staged.map(to_MetadataUpdate)
    │
    ├─▶ descriptors = metadata.commit(metadata_updates)
    │   (Atomic catalog update)
    │
    └─▶ For each (staged, descriptor):
            persist_allocation(staged, descriptor)  ← Disk write
            write_back_uncompressed(descriptor)     ← Cache insert
```

**Error Handling**:
- Staging failures for individual columns are **silently skipped** (allocation/serialization errors)
- If all columns fail staging, job completes with no effect
- Persistence failures are **logged to stderr** but don't abort other columns
- No rollback mechanism (write-once semantics)

---

## Metadata Client Abstraction

### Trait Definition

```rust
pub trait MetadataClient: Send + Sync {
    fn latest_descriptor(&self, table: &str, column: &str) -> Option<PageDescriptor>;
    fn commit(&self, table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor>;
}
```

### Implementations

| Implementation | Purpose | Behavior |
|----------------|---------|----------|
| **NoopMetadataClient** | Testing/Placeholder | Returns fake descriptors, no persistence |
| **DirectoryMetadataClient** | Production | Backed by `PageDirectory`, atomic batch registration |

**DirectoryMetadataClient Wiring**:
```rust
let directory = Arc::new(PageDirectory::new(...));
let metadata_client = Arc::new(DirectoryMetadataClient::new(Arc::clone(&directory)));
let writer = Writer::new(page_handler, allocator, metadata_client);
```

---

## Concurrency Properties

### Single Writer Thread

**Design Choice**: Writer uses **one dedicated background thread** to serialize all writes.

```
Main Thread(s)              Writer Thread
─────────────              ─────────────
submit(job1) ──┐
               │
submit(job2) ──┼───▶ Channel ───▶ handle(job1)
               │                        │
submit(job3) ──┘                        ▼
                                   handle(job2)
                                        │
                                        ▼
                                   handle(job3)
```

**Benefits**:
- **No write-write conflicts**: Operations on same column are naturally serialized
- **Predictable latency**: FIFO queue guarantees order
- **Simple reasoning**: No distributed coordination needed

**Trade-offs**:
- **Sequential bottleneck**: Cannot parallelize writes across columns
- **Head-of-line blocking**: Large jobs block small jobs behind them

---

### Memory Ordering

| Operation | Ordering | Justification |
|-----------|----------|---------------|
| `is_shutdown.load()` | **Acquire** | Synchronizes with `store(Release)` to see shutdown flag |
| `is_shutdown.compare_exchange()` | **AcqRel/Acquire** | CAS winner publishes shutdown, losers observe |
| `shutdown_flag.store()` | **Release** | Worker thread publishes completion to joining thread |

**Shutdown Sequence**:
```
Thread A (shutdown caller)          Worker Thread
─────────────────────────          ─────────────
  CAS(false→true, AcqRel) ────────────┐
         │                             │
         └──▶ send(Shutdown) ──────────┼──▶ recv()
                                       │      │
                                       │      ▼
                                       │   store(true, Release)
                                       │      │
  join() ◀──────────────────────────────────┘
```

---

## Integration with PageHandler

### Read Path (Before Write)

```
Writer::stage_column()
    │
    └──▶ page_handler.get_page(descriptor)
             │
             ├──▶ Check uncompressed cache
             ├──▶ Check compressed cache
             └──▶ Load from disk via PageIO
```

### Write Path (After Commit)

```
Writer::handle_job()
    │
    └──▶ page_handler.write_back_uncompressed(id, page)
             │
             └──▶ Uncompressed cache.insert(id, page)
                      │
                      └──▶ Potential eviction ──▶ Compress ──▶ Compressed cache
```

**Cache Consistency**:
- Writer always inserts the **latest version** after successful commit
- Old versions remain in cache until evicted (no explicit invalidation)
- Readers may see stale versions if they hold PageDescriptor references from before commit

---

## Testing Coverage

### Unit Tests (`src/writer/allocator.rs:134-196`)

| Test | Coverage |
|------|----------|
| `round_up_4k_aligns_lengths` | 4K alignment arithmetic |
| `compute_alloc_len_handles_full_blocks_and_tail` | Block+tail allocation sizing |
| `file_state_allocates_monotonically` | Sequential offset tracking |
| `file_state_rotates_when_full` | File rotation at 4GB boundary |

**No integration tests exist for Writer module** (opportunity for expansion).

---

## Usage Example

```rust
// Setup
let page_handler = Arc::new(PageHandler::new(...));
let allocator = Arc::new(DirectBlockAllocator::new()?);
let metadata = Arc::new(DirectoryMetadataClient::new(directory));
let writer = Writer::new(page_handler, allocator, metadata);

// Submit write job
let job = UpdateJob::new("users", vec![
    ColumnUpdate::new("email", vec![
        UpdateOp::Append { entry: Entry::new("alice@example.com") },
    ]),
    ColumnUpdate::new("age", vec![
        UpdateOp::Overwrite { row: 0, entry: Entry::new("30") },
    ]),
]);

writer.submit(job)?;

// Cleanup (automatic via Drop, or explicit)
writer.shutdown();
```

---

## Key Invariants

1. **Serialized Execution**: All writes execute in submission order on worker thread
2. **Atomic Metadata Commit**: All columns in a job commit together via `register_batch()`
3. **4K Alignment**: All persisted allocations are 4K-aligned (O_DIRECT requirement)
4. **No Rollback**: Failed columns are skipped; successful columns persist
5. **Cache Insertion**: Successful writes always insert into uncompressed cache

---

## Related Documentation

- **Allocator**: [allocator](allocator) - Block allocation strategy
- **Metadata Store**: [metadata_store](metadata_store) - Catalog and version chains
- **Page Handler**: [page_handler](page_handler) - Cache coordination
- **Page I/O**: [page_io](page_io) - Disk read/write primitives
- **Storage Strategy**: [storage_strategy](storage_strategy) - File layout and rotation
