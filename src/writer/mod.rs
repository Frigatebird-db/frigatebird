use std::sync::atomic::AtomicUsize;

pub mod allocator;
pub mod executor;
pub mod update_job;

/// Controls how many writer shards are spawned by default.
pub static GLOBAL_WRITER_SHARD_COUNT: AtomicUsize = AtomicUsize::new(1);

pub use allocator::{DirectBlockAllocator, PageAllocator};
pub use executor::{DirectoryMetadataClient, MetadataClient, Writer};
pub use update_job::{ColumnUpdate, UpdateJob, UpdateOp};
