use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::metadata_store::PageDescriptor;
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::writer::allocator::{PageAllocation, PageAllocator};
use crate::writer::update_job::{ColumnUpdate, UpdateJob, UpdateOp};
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};

/// Abstraction for publishing new page versions into persistent metadata.
pub trait MetadataClient: Send + Sync {
    fn latest_descriptor(&self, table: &str, column: &str) -> Option<PageDescriptor>;
    fn publish_new_version(&self, table: &str, column: &str, allocation: &PageAllocation);
}

/// Placeholder metadata client used until the real store integration lands.
pub struct NoopMetadataClient;

impl MetadataClient for NoopMetadataClient {
    fn latest_descriptor(&self, _table: &str, _column: &str) -> Option<PageDescriptor> {
        None
    }

    fn publish_new_version(&self, _table: &str, _column: &str, _allocation: &PageAllocation) {}
}

enum WriterMessage {
    Job(UpdateJob),
    Shutdown,
}

struct WorkerContext {
    page_handler: Arc<PageHandler>,
    allocator: Arc<dyn PageAllocator>,
    metadata: Arc<dyn MetadataClient>,
    rx: Receiver<WriterMessage>,
}

impl WorkerContext {
    fn handle_job(&self, job: UpdateJob) {
        for column in job.columns {
            self.handle_column(&job.table, column);
        }
    }

    fn handle_column(&self, table: &str, update: ColumnUpdate) {
        let latest = self.metadata.latest_descriptor(table, &update.column);
        let base_page = latest
            .as_ref()
            .and_then(|descriptor| self.page_handler.get_page(descriptor.clone()));

        let mut prepared = base_page
            .map(|page| (*page).clone())
            .unwrap_or_else(|| PageCacheEntryUncompressed { page: Page::new() });

        apply_operations(&mut prepared, &update.operations);

        let allocation = self.allocator.allocate();
        let mut staged = prepared.clone();
        staged.page.page_metadata = allocation.page_id.clone();

        self.page_handler
            .write_back_uncompressed(&allocation.page_id, staged);
        self.metadata
            .publish_new_version(table, &update.column, &allocation);
    }
}

/// Serial writer that executes update jobs in order.
pub struct Writer {
    tx: Sender<WriterMessage>,
    handle: Option<JoinHandle<()>>,
    is_shutdown: Arc<AtomicBool>,
}

impl Writer {
    pub fn new(
        page_handler: Arc<PageHandler>,
        allocator: Arc<dyn PageAllocator>,
        metadata: Arc<dyn MetadataClient>,
    ) -> Self {
        let (tx, rx) = channel::unbounded::<WriterMessage>();
        let is_shutdown = Arc::new(AtomicBool::new(false));
        let ctx = WorkerContext {
            page_handler,
            allocator,
            metadata,
            rx,
        };
        let shutdown_flag = Arc::clone(&is_shutdown);

        let handle = thread::spawn(move || run_worker(ctx, shutdown_flag));

        Writer {
            tx,
            handle: Some(handle),
            is_shutdown,
        }
    }

    pub fn submit(&self, job: UpdateJob) -> Result<(), WriterError> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(WriterError::Shutdown);
        }
        self.tx
            .send(WriterMessage::Job(job))
            .map_err(|_| WriterError::ChannelClosed)
    }

    pub fn shutdown(&mut self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let _ = self.tx.send(WriterMessage::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

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

fn apply_operations(page: &mut PageCacheEntryUncompressed, operations: &[UpdateOp]) {
    for op in operations {
        match op {
            UpdateOp::Overwrite { row, entry } => {
                let row_idx = *row as usize;
                ensure_capacity(&mut page.page, row_idx);
                page.page.entries[row_idx] = entry.clone();
            }
            UpdateOp::Append { entry } => {
                page.page.entries.push(entry.clone());
            }
        }
    }
}

fn ensure_capacity(page: &mut Page, row_idx: usize) {
    if row_idx < page.entries.len() {
        return;
    }

    let missing = row_idx + 1 - page.entries.len();
    page.entries
        .extend((0..missing).map(|_| crate::entry::Entry::new("")));
}

#[derive(Debug)]
pub enum WriterError {
    Shutdown,
    ChannelClosed,
}
