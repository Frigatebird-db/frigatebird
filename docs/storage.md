---
title: "Storage Layer"
layout: default
nav_order: 6
---

# Storage Layer

The storage layer manages disk allocation, file organization, and page I/O operations. It consists of the DirectBlockAllocator for space management and PageIO for reading/writing compressed pages to disk.

## DirectBlockAllocator (`src/writer/allocator.rs`)

The allocator manages disk space in 256 KiB blocks with 4K alignment for O_DIRECT compatibility.

### Constants

```rust
const ALIGN_4K: u64 = 4096;
const BLOCK_SIZE: u64 = 262_144;  // 256 KiB
const FILE_MAX_BYTES: u64 = 4_294_967_296;  // 4 GiB
const DATA_DIR: &str = "storage";
```

### File State

```rust
struct FileState {
    file_id: u32,
    offset: u64,
    file: File,
    max_size: u64,
}
```

Files follow the naming pattern: `storage/data.{file_id:05}` (data.00000, data.00001, ...)

### Allocation

```rust
pub struct PageAllocation {
    pub file_id: u32,
    pub path: String,
    pub offset: u64,
    pub actual_len: u64,
    pub alloc_len: u64,
}
```

**Allocation Strategy:**

`compute_alloc_len(actual_len)`:
```
full_blocks = actual_len / 256K
tail        = actual_len % 256K
alloc_len   = (full_blocks * 256K) + round_up_4k(tail)
```

`round_up_4k(len)`:
```rust
(len + 4095) & !4095
```

**Example:**
```
actual_len = 270,000 bytes

full_blocks = 270,000 / 262,144 = 1 block
tail = 270,000 % 262,144 = 7,856 bytes
tail_aligned = round_up_4k(7,856) = 8,192 bytes (2 pages)

alloc_len = (1 * 262,144) + 8,192 = 270,336 bytes
```

### File Rotation

When the current file would exceed 4 GiB, the allocator rotates to a new file:

```
if offset + size > FILE_MAX_BYTES:
    file_id += 1
    offset = 0
    file = open(data.{file_id:05})
```

**File Rotation Example:**
```
Current: data.00000 at offset 4,294,900,000 (near 4 GiB limit)
Request: allocate 100,000 bytes
Action: Create data.00001, allocate at offset 0
```

### O_DIRECT Support (Linux only)

On Linux, files are opened with O_DIRECT to bypass the kernel page cache:

```rust
#[cfg(target_os = "linux")]
opts.custom_flags(libc::O_DIRECT);
```

**Requirements:**
- All I/O must be 4K-aligned (offset and size)
- Buffers must be page-aligned in memory
- Reduces memory pressure by avoiding double-buffering

---

## PageIO (`src/page_handler/page_io.rs`)

PageIO handles reading and writing compressed pages to disk with a 64-byte metadata prefix.

### API

```rust
read_from_path(path, offset) -> PageCacheEntryCompressed
read_batch_from_path(path, offsets: &[u64]) -> Vec<PageCacheEntryCompressed>
write_to_path(path, offset, data: Vec<u8>)
write_batch_to_path(path, writes: &[(u64, Vec<u8>)])
```

### On-Disk Format

Each page on disk consists of a fixed 64-byte metadata header followed by the compressed page data:

```
Offset: N
╔═══════════════════════════════════════════╗
║ 64B Metadata (rkyv serialized)           ║
║   • read_size : usize                     ║
╚═══════════════════════════════════════════╝
Offset: N + 64
╔═══════════════════════════════════════════╗
║ Compressed Page Bytes (LZ4 + bincode)    ║
╚═══════════════════════════════════════════╝
```

**Metadata Structure:**
```rust
struct PageMetadata {
    read_size: usize,  // Size of compressed payload
}
```

### Read Flow (Single Page)

```
read_from_path(path, offset)
      │
      ├─▶ seek to offset
      │
      ├─▶ read 64 bytes → deserialize metadata (rkyv)
      │     └─▶ extract read_size
      │
      ├─▶ read read_size bytes → compressed_data
      │
      └─▶ return PageCacheEntryCompressed { page: compressed_data }
```

### Batch Read (Linux: io_uring)

On Linux, batch reads use io_uring for parallel I/O:

```
offsets[]
    │
    ├─▶ Phase 1: Queue all metadata reads (64B each)
    │     └─▶ submit io_uring Read opcodes
    │
    ├─▶ Wait for CQEs → parse read_size from each
    │
    ├─▶ Phase 2: Allocate payload buffers based on read_size
    │
    ├─▶ Phase 3: Queue all payload reads (offset + 64, size = read_size)
    │     └─▶ submit io_uring Read opcodes
    │
    └─▶ Wait for CQEs → return Vec<PageCacheEntryCompressed>
```

**Performance:**
- All metadata reads happen in parallel
- All payload reads happen in parallel
- Single fsync at the end
- ~N× faster than sequential reads for N pages

**Non-Linux:** Falls back to sequential reads using standard file I/O.

### Write Flow (Single Page)

```
write_to_path(path, offset, data)
      │
      ├─▶ Serialize metadata { read_size: data.len() } with rkyv
      │     └─▶ pad to 64 bytes
      │
      ├─▶ Open file (with O_CREAT)
      │
      ├─▶ Write metadata at offset
      │
      ├─▶ Write compressed data at offset + 64
      │
      └─▶ fsync()
```

### Batch Write (Linux: io_uring)

On Linux, batch writes use io_uring for parallel I/O:

```
writes: &[(offset, data)]
    │
    ├─▶ Step 1: Prepare all buffers
    │     │
    │     ├─▶ For each write:
    │     │     ├─▶ serialize metadata (rkyv)
    │     │     ├─▶ pad to 64B
    │     │     ├─▶ combine metadata + compressed data
    │     │     └─▶ store in stable buffer
    │     │
    │     └─▶ Buffers must remain valid until io_uring completes
    │
    ├─▶ Step 2: Queue all Write opcodes
    │     └─▶ submit to io_uring ring buffer
    │
    ├─▶ Step 3: Wait for all completions (CQEs)
    │     └─▶ all writes complete in parallel
    │
    └─▶ Step 4: Single Fsync opcode
          └─▶ durability guarantee
```

**Performance:**
- All writes submitted in one batch
- Kernel parallelizes I/O operations
- Single fsync for all writes
- Reduces context switches

**Non-Linux:** Falls back to sequential writes using standard file I/O.

### Constants

```rust
const PREFIX_META_SIZE: usize = 64;
```

---

## Integration

### Writer → Allocator → PageIO

```
Writer::persist_allocation()
      │
      ├─▶ DirectBlockAllocator::allocate(actual_len)
      │     └─▶ returns PageAllocation { path, offset, alloc_len }
      │
      ├─▶ Zero-pad buffer to alloc_len
      │
      └─▶ PageIO::write_to_path(path, offset, padded_buffer)
            └─▶ Persists to disk
```

### PageHandler → PageIO

```
PageHandler::get_page()
      │
      ├─▶ Cache miss
      │
      └─▶ PageFetcher::fetch_and_insert(descriptor)
            │
            └─▶ PageIO::read_from_path(descriptor.disk_path, descriptor.offset)
                  └─▶ Returns compressed page
```

---

## Platform Differences

| Feature | Linux | Windows/macOS |
|---------|-------|---------------|
| I/O Engine | io_uring | Standard file I/O |
| O_DIRECT | Yes | No |
| Batch Operations | Parallel | Sequential |
| Performance | ~N× speedup | Baseline |

---

## Error Handling

- Allocation failures return `None` (out of space, I/O errors)
- Read/write failures propagate `io::Error`
- File rotation creates parent directories automatically
- Missing files are created on first write

---

## Module Locations

- **Allocator:** `src/writer/allocator.rs`
- **PageIO:** `src/page_handler/page_io.rs`
- **Tests:** `src/writer/allocator.rs` (unit tests for alignment and rotation)
