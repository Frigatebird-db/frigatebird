use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::metadata_store::{PageDescriptor, PageDirectory, PendingPage};
use crate::page::Page;
use crate::page_handler::PageHandler;
use crate::writer::allocator::PageAllocator;
use crate::writer::update_job::{ColumnUpdate, UpdateJob, UpdateOp};
use bincode;
use crossbeam::channel::{self, Receiver, Sender};
use std::fs::{self, OpenOptions};
use std::io;
#[cfg(target_family = "unix")]
use std::os::unix::fs::FileExt;
#[cfg(target_family = "windows")]
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};

/// Abstraction for publishing new page versions into persistent metadata.
pub trait MetadataClient: Send + Sync {
    fn latest_descriptor(&self, table: &str, column: &str) -> Option<PageDescriptor>;
    fn commit(&self, table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor>;
}

/// Describes a staged update ready to be made visible.
#[derive(Clone, Debug)]
pub struct MetadataUpdate {
    pub column: String,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
    pub replace_last: bool,
}

/// Placeholder metadata client used until the real store integration lands.
#[allow(dead_code)]
pub struct NoopMetadataClient;

impl MetadataClient for NoopMetadataClient {
    fn latest_descriptor(&self, _table: &str, _column: &str) -> Option<PageDescriptor> {
        None
    }

    fn commit(&self, _table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor> {
        updates
            .into_iter()
            .enumerate()
            .map(|(idx, update)| PageDescriptor {
                id: format!("noop-{}-{idx}", update.column),
                disk_path: update.disk_path,
                offset: update.offset,
                alloc_len: update.alloc_len,
                actual_len: update.actual_len,
                entry_count: update.entry_count,
            })
            .collect()
    }
}

/// Metadata client backed by the shared page directory.
pub struct DirectoryMetadataClient {
    directory: Arc<PageDirectory>,
}

impl DirectoryMetadataClient {
    pub fn new(directory: Arc<PageDirectory>) -> Self {
        DirectoryMetadataClient { directory }
    }
}

impl MetadataClient for DirectoryMetadataClient {
    fn latest_descriptor(&self, _table: &str, column: &str) -> Option<PageDescriptor> {
        self.directory.latest(column)
    }

    fn commit(&self, _table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor> {
        if updates.is_empty() {
            return Vec::new();
        }
        let pending: Vec<PendingPage> = updates
            .into_iter()
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
        self.directory.register_batch(&pending)
    }
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
        let mut staged = Vec::with_capacity(job.columns.len());
        for column in job.columns {
            if let Some(prepared) = self.stage_column(&job.table, column) {
                staged.push(prepared);
            }
        }

        if staged.is_empty() {
            return;
        }

        let metadata_updates: Vec<MetadataUpdate> = staged
            .iter()
            .map(|prepared| MetadataUpdate {
                column: prepared.column.clone(),
                disk_path: prepared.disk_path.clone(),
                offset: prepared.offset,
                alloc_len: prepared.alloc_len,
                actual_len: prepared.actual_len,
                entry_count: prepared.entry_count,
                replace_last: prepared.replace_last,
            })
            .collect();

        let descriptors = self.metadata.commit(&job.table, metadata_updates);
        if descriptors.len() != staged.len() {
            return;
        }

        for (prepared, descriptor) in staged.into_iter().zip(descriptors.into_iter()) {
            if let Err(err) = persist_allocation(&prepared, &descriptor) {
                eprintln!("writer: failed to persist allocation: {err}");
            }
            let mut page = prepared.page;
            page.page.page_metadata = descriptor.id.clone();
            self.page_handler
                .write_back_uncompressed(&descriptor.id, page);
        }
    }

    fn stage_column(&self, table: &str, update: ColumnUpdate) -> Option<StagedColumn> {
        let latest = self.metadata.latest_descriptor(table, &update.column);
        let base_page = latest
            .as_ref()
            .and_then(|descriptor| self.page_handler.get_page(descriptor.clone()));

        let mut prepared = base_page
            .map(|page| (*page).clone())
            .unwrap_or_else(|| PageCacheEntryUncompressed { page: Page::new() });

        apply_operations(&mut prepared, &update.operations);

        let entry_count = prepared.page.entries.len() as u64;
        let serialized = match bincode::serialize(&prepared.page) {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };
        let actual_len = serialized.len() as u64;
        let allocation = match self.allocator.allocate(actual_len) {
            Ok(a) => a,
            Err(_) => return None,
        };

        Some(StagedColumn {
            column: update.column,
            page: prepared,
            entry_count,
            disk_path: allocation.path,
            offset: allocation.offset,
            actual_len,
            alloc_len: allocation.alloc_len,
            serialized,
            replace_last: latest.is_some(),
        })
    }
}

struct StagedColumn {
    column: String,
    page: PageCacheEntryUncompressed,
    entry_count: u64,
    disk_path: String,
    offset: u64,
    actual_len: u64,
    alloc_len: u64,
    serialized: Vec<u8>,
    replace_last: bool,
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

fn persist_allocation(prepared: &StagedColumn, descriptor: &PageDescriptor) -> io::Result<()> {
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
