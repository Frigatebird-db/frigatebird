use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::sync::atomic::{AtomicU64, Ordering};

/// Describes a new physical location for a rewritten page.
#[derive(Debug, Clone)]
pub struct PageAllocation {
    pub page_id: String,
    pub disk_path: String,
    pub offset: u64,
    pub buffer: Vec<u8>,
}

impl PageAllocation {
    pub fn new(page_id: String, disk_path: String, offset: u64, buffer: Vec<u8>) -> Self {
        PageAllocation {
            page_id,
            disk_path,
            offset,
            buffer,
        }
    }
}

/// Abstraction over page allocation so we can upgrade the allocator later.
pub trait PageAllocator: Send + Sync {
    fn allocate(&self) -> PageAllocation;
}

/// Temporary allocator that fabricates unique identifiers and locations.
pub struct DummyPageAllocator {
    counter: AtomicU64,
}

impl DummyPageAllocator {
    pub fn new() -> Self {
        DummyPageAllocator {
            counter: AtomicU64::new(0),
        }
    }

    fn random_suffix() -> String {
        let mut rng = thread_rng();
        (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
    }
}

impl Default for DummyPageAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PageAllocator for DummyPageAllocator {
    fn allocate(&self) -> PageAllocation {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let page_id = format!("writer-{:016x}-{}", seq, Self::random_suffix());
        let disk_path = format!("/var/tmp/pages/segment-{}", seq);
        let offset = seq * 4096;
        PageAllocation::new(page_id, disk_path, offset, Vec::new())
    }
}
