use crate::cache::page_cache::PageCacheEntryUncompressed;
use crate::entry::Entry;
use crate::metadata_store::{
    CatalogError, ColumnCatalog, ColumnDefinition, ColumnStats, ColumnStatsKind, MetaJournal,
    MetaJournalEntry, MetaRecord, PageDescriptor, PageDirectory, PendingPage, ROWS_PER_PAGE_GROUP,
    TableDefinition,
};
use crate::page::Page;
use crate::page_handler::{PageHandler, page_io::PageIO};
use crate::sql::executor::batch::{ColumnData, ColumnarPage};
use crate::wal::Walrus;
use crate::writer::GLOBAL_WRITER_SHARD_COUNT;
use crate::writer::allocator::PageAllocator;
use crate::writer::update_job::{ColumnUpdate, UpdateJob, UpdateOp};
use bincode;
use crossbeam::channel::{self, Receiver, Sender};
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::{AlignedVec, Infallible, archived_root, to_bytes};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};

/// Abstraction for publishing new page versions into persistent metadata.
pub trait MetadataClient: Send + Sync {
    fn latest_descriptor(&self, table: &str, column: &str) -> Option<PageDescriptor>;
    fn commit(&self, table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor>;
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Describes a staged update ready to be made visible.
#[derive(Clone, Debug)]
pub struct MetadataUpdate {
    pub table: String,
    pub column: String,
    pub descriptor_id: Option<String>,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
    pub replace_last: bool,
    pub stats: Option<ColumnStats>,
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
                id: update
                    .descriptor_id
                    .unwrap_or_else(|| format!("noop-{}-{idx}", update.column)),
                disk_path: update.disk_path,
                offset: update.offset,
                alloc_len: update.alloc_len,
                actual_len: update.actual_len,
                entry_count: update.entry_count,
                stats: update.stats,
            })
            .collect()
    }

    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Metadata client backed by the shared page directory.
pub struct DirectoryMetadataClient {
    directory: Arc<PageDirectory>,
    journal: Arc<MetaJournal>,
}

impl DirectoryMetadataClient {
    pub fn new(directory: Arc<PageDirectory>, journal: Arc<MetaJournal>) -> Self {
        DirectoryMetadataClient { directory, journal }
    }
}

impl MetadataClient for DirectoryMetadataClient {
    fn latest_descriptor(&self, table: &str, column: &str) -> Option<PageDescriptor> {
        self.directory.latest_in_table(table, column)
    }

    fn commit(&self, table: &str, updates: Vec<MetadataUpdate>) -> Vec<PageDescriptor> {
        if updates.is_empty() {
            return Vec::new();
        }

        let mut table_name = table.to_string();
        if let Some(explicit) = updates.iter().find(|update| !update.table.is_empty()) {
            table_name = explicit.table.clone();
        }

        let mut unique_columns: BTreeSet<String> = BTreeSet::new();
        for update in &updates {
            unique_columns.insert(update.column.clone());
        }

        if !unique_columns.is_empty() {
            let column_defs: Vec<ColumnDefinition> = unique_columns
                .iter()
                .map(|name| ColumnDefinition::new(name.clone(), "String"))
                .collect();
            let order: Vec<String> = unique_columns.iter().cloned().collect();
            let definition = TableDefinition::new(table_name.clone(), column_defs.clone(), order);
            match self.directory.register_table(definition) {
                Ok(_) => {}
                Err(CatalogError::TableExists(_)) => {
                    if let Err(err) = self
                        .directory
                        .add_columns_to_table(&table_name, column_defs)
                    {
                        eprintln!(
                            "metadata client: failed to extend table {}: {}",
                            table_name, err
                        );
                    }
                }
                Err(err) => {
                    eprintln!(
                        "metadata client: failed to register table {}: {}",
                        table_name, err
                    );
                }
            }
        }

        let mut journal_entries = Vec::with_capacity(updates.len());
        let pending: Vec<PendingPage> = updates
            .into_iter()
            .map(|update| {
                let descriptor_id = update.descriptor_id.unwrap_or_else(|| {
                    self.directory
                        .reserve_descriptor_id()
                        .expect("descriptor id allocation failed")
                });
                let table_for_update = if update.table.is_empty() {
                    table_name.clone()
                } else {
                    update.table
                };
                journal_entries.push(MetaJournalEntry {
                    column: update.column.clone(),
                    descriptor_id: descriptor_id.clone(),
                    disk_path: update.disk_path.clone(),
                    offset: update.offset,
                    alloc_len: update.alloc_len,
                    actual_len: update.actual_len,
                    entry_count: update.entry_count,
                    replace_last: update.replace_last,
                    stats: update.stats.clone(),
                });
                PendingPage {
                    table: table_for_update,
                    column: update.column,
                    descriptor_id: Some(descriptor_id),
                    disk_path: update.disk_path,
                    offset: update.offset,
                    alloc_len: update.alloc_len,
                    actual_len: update.actual_len,
                    entry_count: update.entry_count,
                    replace_last: update.replace_last,
                    stats: update.stats,
                }
            })
            .collect();

        let record = MetaRecord::PublishPages {
            table: table_name.clone(),
            entries: journal_entries,
        };

        self.journal
            .append_commit(&table_name, &record)
            .expect("metadata journal append failed");
        self.directory.register_batch(&pending)
    }

    fn table_names(&self) -> Vec<String> {
        self.directory.table_names()
    }
}

enum WriterMessage {
    Job(UpdateJob),
    Flush { table: String, ack: Sender<()> },
    Shutdown,
}

struct WorkerContext {
    page_handler: Arc<PageHandler>,
    allocator: Arc<dyn PageAllocator>,
    metadata: Arc<dyn MetadataClient>,
    wal: Arc<Walrus>,
    rx: Receiver<WriterMessage>,
    buffered_rows: HashMap<String, Vec<Vec<String>>>,
}

impl WorkerContext {
    fn handle_job(&mut self, job: UpdateJob) {
        let UpdateJob { table, mut columns } = job;

        if columns.len() == 1 {
            if let Some(column_update) = columns.get_mut(0) {
                if column_update.operations.len() == 1 {
                    if matches!(column_update.operations[0], UpdateOp::BufferRow { .. }) {
                        if let UpdateOp::BufferRow { row } = column_update.operations.remove(0) {
                            self.buffer_row(&table, row);
                            return;
                        }
                    }
                }
            }
        }

        let mut staged = Vec::with_capacity(columns.len());
        for column in columns {
            if let Some(prepared) = self.stage_column(&table, column) {
                staged.push(prepared);
            }
        }

        self.publish_staged_columns(&table, staged);
        self.ack_wal_entries(&table, 1);
    }

    fn stage_column(&self, table: &str, update: ColumnUpdate) -> Option<StagedColumn> {
        let latest = self.metadata.latest_descriptor(table, &update.column);
        let base_page = latest
            .as_ref()
            .and_then(|descriptor| self.page_handler.get_page(descriptor.clone()));

        let mut prepared = base_page
            .map(|page| (*page).clone())
            .unwrap_or_else(|| PageCacheEntryUncompressed::from_disk_page(Page::new()));

        apply_operations(&mut prepared, &update.operations);

        let entry_count = prepared.page.len() as u64;
        let disk_page = prepared.page.as_disk_page();
        let serialized = match bincode::serialize(&disk_page) {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };
        let actual_len = serialized.len() as u64;
        let allocation = match self.allocator.allocate(actual_len) {
            Ok(a) => a,
            Err(_) => return None,
        };
        let stats = derive_column_stats_from_page(&prepared.page);

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
            stats,
        })
    }

    fn publish_staged_columns(&mut self, table: &str, staged: Vec<StagedColumn>) {
        if staged.is_empty() {
            return;
        }

        let metadata_updates: Vec<MetadataUpdate> = staged
            .iter()
            .map(|prepared| MetadataUpdate {
                table: table.to_string(),
                column: prepared.column.clone(),
                descriptor_id: None,
                disk_path: prepared.disk_path.clone(),
                offset: prepared.offset,
                alloc_len: prepared.alloc_len,
                actual_len: prepared.actual_len,
                entry_count: prepared.entry_count,
                replace_last: prepared.replace_last,
                stats: prepared.stats.clone(),
            })
            .collect();

        let descriptors = self.metadata.commit(table, metadata_updates);
        if descriptors.len() != staged.len() {
            eprintln!(
                "writer: metadata commit returned {} descriptors for {} staged columns",
                descriptors.len(),
                staged.len()
            );
            return;
        }

        for (prepared, descriptor) in staged.into_iter().zip(descriptors.into_iter()) {
            if let Err(err) = persist_allocation(&prepared, &descriptor) {
                eprintln!("writer: failed to persist allocation: {err}");
                continue;
            }
            let mut page = prepared.page;
            page.page.page_metadata = descriptor.id.clone();
            self.page_handler
                .write_back_uncompressed(&descriptor.id, page);
        }
    }

    fn ack_wal_entries(&self, table: &str, mut count: usize) {
        if count == 0 {
            return;
        }
        let mut handled = 0usize;
        while count > 0 {
            match self.wal.read_next(table, true) {
                Ok(Some(_)) => {
                    count -= 1;
                    handled += 1;
                }
                Ok(None) => {
                    eprintln!(
                        "writer: wal ack shortfall for table {} (wanted {}, handled {})",
                        table, count + handled, handled
                    );
                    break;
                }
                Err(err) => {
                    eprintln!("writer: wal ack failed for table {}: {}", table, err);
                    break;
                }
            }
        }
    }

    fn buffer_row(&mut self, table: &str, row: Vec<String>) {
        let rows_per_group = self
            .page_handler
            .table_catalog(table)
            .map(|catalog| catalog.rows_per_page_group)
            .unwrap_or(ROWS_PER_PAGE_GROUP);

        let entry = self
            .buffered_rows
            .entry(table.to_string())
            .or_insert_with(Vec::new);
        entry.push(row);

        if entry.len() >= rows_per_group as usize {
            if let Some(rows_to_flush) = self.buffered_rows.remove(table) {
                self.flush_page_group(table, rows_to_flush);
            }
        }
    }

    fn flush_page_group(&mut self, table: &str, mut rows: Vec<Vec<String>>) {
        if rows.is_empty() {
            return;
        }
        let total_rows = rows.len();
        eprintln!("[flush_page_group] table={}, initial rows={}", table, total_rows);

        let catalog = match self.page_handler.table_catalog(table) {
            Some(catalog) => catalog,
            None => {
                eprintln!(
                    "writer: unable to flush rows for table {} - catalog not found",
                    table
                );
                return;
            }
        };

        let columns: Vec<_> = catalog.columns().to_vec();
        if columns.is_empty() {
            eprintln!(
                "writer: unable to flush rows for table {} - no columns defined",
                table
            );
            return;
        }

        let sort_key_columns = catalog.sort_key();
        let sort_key_ordinals: Vec<usize> =
            sort_key_columns.iter().map(|col| col.ordinal).collect();

        if sort_key_ordinals.is_empty() {
            eprintln!(
                "writer: unable to flush rows for table {} - missing sort key (columns={:?}, sort_key_columns={:?})",
                table,
                catalog
                    .columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect::<Vec<_>>(),
                sort_key_columns
                    .iter()
                    .map(|col| col.name.clone())
                    .collect::<Vec<_>>()
            );
            return;
        }

        rows.sort_unstable_by(|left, right| compare_rows(left, right, &sort_key_ordinals));

        self.extend_partial_tail(table, &columns, catalog.rows_per_page_group, &mut rows);
        eprintln!("[flush_page_group] after extend_partial_tail, rows remaining={}", rows.len());

        let full_group_size = catalog.rows_per_page_group as usize;
        while rows.len() >= full_group_size {
            let chunk: Vec<Vec<String>> = rows.drain(..full_group_size).collect();
            self.stage_rows_as_new_group(table, &columns, chunk);
        }

        if !rows.is_empty() {
            self.stage_rows_as_new_group(table, &columns, rows);
        }

        self.ack_wal_entries(table, total_rows);
        self.wal.mark_topic_clean(table);
    }

    fn flush_pending(&mut self, table: &str) {
        if let Some(rows) = self.buffered_rows.remove(table) {
            self.flush_page_group(table, rows);
        }
    }

    fn extend_partial_tail(
        &mut self,
        table: &str,
        columns: &[ColumnCatalog],
        rows_per_page_group: u64,
        rows: &mut Vec<Vec<String>>,
    ) {
        if rows.is_empty() {
            return;
        }
        let first_column = match columns.first() {
            Some(column) => column,
            None => return,
        };
        let descriptor = match self
            .page_handler
            .locate_latest_in_table(table, &first_column.name)
        {
            Some(desc) => desc,
            None => return,
        };
        if descriptor.entry_count >= rows_per_page_group {
            return;
        }

        let available = (rows_per_page_group - descriptor.entry_count) as usize;
        if available == 0 {
            return;
        }
        let take = available.min(rows.len());
        if take == 0 {
            return;
        }

        let mut staged = Vec::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            let descriptor = match self
                .page_handler
                .locate_latest_in_table(table, &column.name)
            {
                Some(desc) => desc,
                None => continue,
            };
            if descriptor.entry_count >= rows_per_page_group {
                continue;
            }

            let page_arc = match self.page_handler.get_page(descriptor.clone()) {
                Some(page) => page,
                None => continue,
            };
            let mut updated = (*page_arc).clone();
            for row in rows.iter().take(take) {
                let value = row.get(idx).map(|v| v.as_str()).unwrap_or_default();
                updated.mutate_disk_page(|disk_page| {
                    disk_page.add_entry(Entry::new(value));
                });
            }
            let disk_page = updated.page.as_disk_page();
            let serialized = match bincode::serialize(&disk_page) {
                Ok(bytes) => bytes,
                Err(err) => {
                    eprintln!(
                        "writer: failed to serialize tail update for {}.{}: {}",
                        table, column.name, err
                    );
                    continue;
                }
            };
            let actual_len = serialized.len() as u64;
            let allocation = match self.allocator.allocate(actual_len) {
                Ok(alloc) => alloc,
                Err(err) => {
                    eprintln!(
                        "writer: failed to allocate space for {}.{} tail update: {}",
                        table, column.name, err
                    );
                    continue;
                }
            };

            let stats = derive_column_stats_from_page(&updated.page);
            staged.push(StagedColumn {
                column: column.name.clone(),
                page: updated,
                entry_count: descriptor.entry_count + take as u64,
                disk_path: allocation.path,
                offset: allocation.offset,
                actual_len,
                alloc_len: allocation.alloc_len,
                serialized,
                replace_last: true,
                stats,
            });
        }

        if !staged.is_empty() {
            eprintln!("[extend_partial_tail] publishing {} staged columns, extending with {} rows", staged.len(), take);
            self.publish_staged_columns(table, staged);
            rows.drain(0..take);
        }
    }

    fn stage_rows_as_new_group(
        &mut self,
        table: &str,
        columns: &[ColumnCatalog],
        rows: Vec<Vec<String>>,
    ) {
        if rows.is_empty() {
            return;
        }

        let mut column_pages: Vec<PageCacheEntryUncompressed> = (0..columns.len())
            .map(|_| PageCacheEntryUncompressed::from_disk_page(Page::new()))
            .collect();

        for row in rows.iter() {
            if row.len() != columns.len() {
                eprintln!(
                    "writer: skipping row with mismatched column count for table {}",
                    table
                );
                return;
            }
            for (idx, value) in row.iter().enumerate() {
                if let Some(page) = column_pages.get_mut(idx) {
                    page.mutate_disk_page(|disk_page| {
                        disk_page.add_entry(Entry::new(value));
                    });
                }
            }
        }

        let row_count = column_pages
            .get(0)
            .map(|page| page.page.len() as u64)
            .unwrap_or(0);
        if row_count == 0 {
            return;
        }

        let mut staged = Vec::with_capacity(columns.len());
        for (idx, page_entry) in column_pages.into_iter().enumerate() {
            let disk_page = page_entry.page.as_disk_page();
            let serialized = match bincode::serialize(&disk_page) {
                Ok(bytes) => bytes,
                Err(err) => {
                    eprintln!(
                        "writer: failed to serialize column {}.{}: {}",
                        table, columns[idx].name, err
                    );
                    continue;
                }
            };
            let actual_len = serialized.len() as u64;
            let allocation = match self.allocator.allocate(actual_len) {
                Ok(alloc) => alloc,
                Err(err) => {
                    eprintln!(
                        "writer: failed to allocate space for column {}.{}: {}",
                        table, columns[idx].name, err
                    );
                    continue;
                }
            };

            let stats = derive_column_stats_from_page(&page_entry.page);
            staged.push(StagedColumn {
                column: columns[idx].name.clone(),
                page: page_entry,
                entry_count: row_count,
                disk_path: allocation.path,
                offset: allocation.offset,
                actual_len,
                alloc_len: allocation.alloc_len,
                serialized,
                replace_last: false,
                stats,
            });
        }

        if !staged.is_empty() {
            self.publish_staged_columns(table, staged);
            println!("writer: flushed {} rows into table {}", row_count, table);
        }
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
    stats: Option<ColumnStats>,
}

struct WriterShard {
    tx: Sender<WriterMessage>,
    handle: Option<JoinHandle<()>>,
    is_shutdown: Arc<AtomicBool>,
}

/// Sharded writer that serializes updates per table while allowing parallel tables.
pub struct Writer {
    shards: Vec<WriterShard>,
    wal: Arc<Walrus>,
    is_shutdown: AtomicBool,
}

impl Writer {
    pub fn new(
        page_handler: Arc<PageHandler>,
        allocator: Arc<dyn PageAllocator>,
        metadata: Arc<dyn MetadataClient>,
        wal: Arc<Walrus>,
    ) -> Self {
        let shard_count = GLOBAL_WRITER_SHARD_COUNT.load(Ordering::Acquire).max(1);
        Writer::with_shard_count(page_handler, allocator, metadata, wal, shard_count)
    }

    pub fn with_shard_count(
        page_handler: Arc<PageHandler>,
        allocator: Arc<dyn PageAllocator>,
        metadata: Arc<dyn MetadataClient>,
        wal: Arc<Walrus>,
        shard_count: usize,
    ) -> Self {
        Self::replay_pending_jobs(
            Arc::clone(&page_handler),
            Arc::clone(&allocator),
            Arc::clone(&metadata),
            Arc::clone(&wal),
        )
        .expect("writer: wal replay failed");

        let shard_total = shard_count.max(1);
        let mut shards = Vec::with_capacity(shard_total);
        for _ in 0..shard_total {
            let (tx, rx) = channel::unbounded::<WriterMessage>();
            let is_shutdown = Arc::new(AtomicBool::new(false));
            let ctx = WorkerContext {
                page_handler: Arc::clone(&page_handler),
                allocator: Arc::clone(&allocator),
                metadata: Arc::clone(&metadata),
                wal: Arc::clone(&wal),
                rx,
                buffered_rows: HashMap::new(),
            };
            let shutdown_flag = Arc::clone(&is_shutdown);
            let handle = thread::spawn(move || run_worker(ctx, shutdown_flag));
            shards.push(WriterShard {
                tx,
                handle: Some(handle),
                is_shutdown,
            });
        }

        Writer {
            shards,
            wal,
            is_shutdown: AtomicBool::new(false),
        }
    }

    pub fn submit(&self, job: UpdateJob) -> Result<(), WriterError> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(WriterError::Shutdown);
        }
        self.append_to_wal(&job)?;
        let shard_index = self.shard_index_for_table(&job.table);
        self.send_job_to_shard(shard_index, WriterMessage::Job(job))
    }

    pub fn flush_table(&self, table: &str) -> Result<(), WriterError> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(WriterError::Shutdown);
        }
        let (ack_tx, ack_rx) = channel::bounded::<()>(0);
        let shard_index = self.shard_index_for_table(table);
        self.send_job_to_shard(
            shard_index,
            WriterMessage::Flush {
                table: table.to_string(),
                ack: ack_tx,
            },
        )?;
        ack_rx.recv().map_err(|_| WriterError::ChannelClosed)?;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self
            .is_shutdown
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        for shard in &self.shards {
            if shard
                .is_shutdown
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _ = shard.tx.send(WriterMessage::Shutdown);
            }
        }
        for shard in &mut self.shards {
            if let Some(handle) = shard.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn append_to_wal(&self, job: &UpdateJob) -> Result<(), WriterError> {
        let bytes: AlignedVec = to_bytes::<_, 512>(job)
            .map_err(|err| WriterError::Serialization(format!("{err:?}")))?;
        self.wal
            .append_for_topic(&job.table, &bytes)
            .map_err(WriterError::Wal)
    }

    fn replay_pending_jobs(
        page_handler: Arc<PageHandler>,
        allocator: Arc<dyn PageAllocator>,
        metadata: Arc<dyn MetadataClient>,
        wal: Arc<Walrus>,
    ) -> Result<(), WriterError> {
        let tables = metadata.table_names();
        if tables.is_empty() {
            return Ok(());
        }

        let (_stub_tx, stub_rx) = channel::unbounded::<WriterMessage>();
        let mut ctx = WorkerContext {
            page_handler,
            allocator,
            metadata,
            wal: Arc::clone(&wal),
            rx: stub_rx,
            buffered_rows: HashMap::new(),
        };

        for table in tables {
            if wal.topic_is_clean(&table) {
                continue;
            }
            loop {
                match wal.read_next(&table, true) {
                    Ok(Some(entry)) => {
                        let mut aligned = AlignedVec::with_capacity(entry.data.len());
                        aligned.extend_from_slice(&entry.data);
                        let archived = unsafe { archived_root::<UpdateJob>(&aligned[..]) };
                        let job = archived.deserialize(&mut Infallible).map_err(|_| {
                            WriterError::Serialization("wal replay deserialize failed".to_string())
                        })?;
                        ctx.handle_job(job);
                    }
                    Ok(None) => break,
                    Err(err) => return Err(WriterError::Wal(err)),
                }
            }
            ctx.flush_pending(&table);
            wal.mark_topic_clean(&table);
        }
        Ok(())
    }

    fn shard_index_for_table(&self, table: &str) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        (fast_table_hash(table) as usize) % self.shards.len()
    }

    fn send_job_to_shard(
        &self,
        shard_index: usize,
        message: WriterMessage,
    ) -> Result<(), WriterError> {
        if let Some(shard) = self.shards.get(shard_index) {
            if shard.is_shutdown.load(Ordering::Acquire) {
                return Err(WriterError::Shutdown);
            }
            shard
                .tx
                .send(message)
                .map_err(|_| WriterError::ChannelClosed)
        } else {
            Err(WriterError::Shutdown)
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[inline]
fn fast_table_hash(table: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in table.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn derive_column_stats_from_page(page: &ColumnarPage) -> Option<ColumnStats> {
    if page.num_rows == 0 {
        return None;
    }

    let null_count = page.null_bitmap.count_ones() as u64;
    let mut stats = ColumnStats {
        min_value: None,
        max_value: None,
        null_count,
        kind: ColumnStatsKind::Text,
    };

    match &page.data {
        ColumnData::Int64(values) => {
            stats.kind = ColumnStatsKind::Int64;
            let mut min_val: Option<i64> = None;
            let mut max_val: Option<i64> = None;
            for (idx, value) in values.iter().enumerate().take(page.num_rows) {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                min_val = Some(min_val.map(|current| current.min(*value)).unwrap_or(*value));
                max_val = Some(max_val.map(|current| current.max(*value)).unwrap_or(*value));
            }
            stats.min_value = min_val.map(|v| v.to_string());
            stats.max_value = max_val.map(|v| v.to_string());
        }
        ColumnData::Float64(values) => {
            stats.kind = ColumnStatsKind::Float64;
            let mut min_val: Option<f64> = None;
            let mut max_val: Option<f64> = None;
            for (idx, value) in values.iter().enumerate().take(page.num_rows) {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                min_val = Some(min_val.map(|current| current.min(*value)).unwrap_or(*value));
                max_val = Some(max_val.map(|current| current.max(*value)).unwrap_or(*value));
            }
            stats.min_value = min_val.map(|v| v.to_string());
            stats.max_value = max_val.map(|v| v.to_string());
        }
        ColumnData::Text(values) => {
            stats.kind = ColumnStatsKind::Text;
            let mut min_val: Option<String> = None;
            let mut max_val: Option<String> = None;
            for (idx, value) in values.iter().enumerate().take(page.num_rows) {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                if min_val
                    .as_ref()
                    .map(|current| value < current)
                    .unwrap_or(true)
                {
                    min_val = Some(value.clone());
                }
                if max_val
                    .as_ref()
                    .map(|current| value > current)
                    .unwrap_or(true)
                {
                    max_val = Some(value.clone());
                }
            }
            stats.min_value = min_val;
            stats.max_value = max_val;
        }
    }

    if stats.min_value.is_none() && stats.max_value.is_none() && stats.null_count == 0 {
        return None;
    }

    Some(stats)
}

#[cfg(test)]
mod writer_stats_tests {
    use super::*;
    use crate::sql::executor::batch::Bitmap;

    fn make_bitmap(len: usize, null_indices: &[usize]) -> Bitmap {
        let mut bitmap = Bitmap::new(len);
        for &idx in null_indices {
            bitmap.set(idx);
        }
        bitmap
    }

    #[test]
    fn derive_stats_from_int_page() {
        let page = ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Int64(vec![1, 2, 7, -3]),
            null_bitmap: make_bitmap(4, &[]),
            num_rows: 4,
        };
        let stats = derive_column_stats_from_page(&page).expect("stats");
        assert_eq!(stats.min_value.as_deref(), Some("-3"));
        assert_eq!(stats.max_value.as_deref(), Some("7"));
        assert_eq!(stats.null_count, 0);
        assert!(matches!(stats.kind, ColumnStatsKind::Int64));
    }

    #[test]
    fn derive_stats_respects_null_entries() {
        let page = ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Float64(vec![10.0, 20.0, 30.0]),
            null_bitmap: make_bitmap(3, &[1]),
            num_rows: 3,
        };
        let stats = derive_column_stats_from_page(&page).expect("stats");
        assert_eq!(stats.min_value.as_deref(), Some("10"));
        assert_eq!(stats.max_value.as_deref(), Some("30"));
        assert_eq!(stats.null_count, 1);
        assert!(matches!(stats.kind, ColumnStatsKind::Float64));
    }

    #[test]
    fn derive_stats_tracks_all_null_pages() {
        let page = ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Text(vec!["a".into(), "b".into()]),
            null_bitmap: make_bitmap(2, &[0, 1]),
            num_rows: 2,
        };
        let stats = derive_column_stats_from_page(&page).expect("stats");
        assert_eq!(stats.null_count, 2);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
        assert!(matches!(stats.kind, ColumnStatsKind::Text));
    }
}

fn run_worker(mut ctx: WorkerContext, shutdown_flag: Arc<AtomicBool>) {
    while let Ok(message) = ctx.rx.recv() {
        match message {
            WriterMessage::Job(job) => ctx.handle_job(job),
            WriterMessage::Flush { table, ack } => {
                ctx.flush_pending(&table);
                let _ = ack.send(());
            }
            WriterMessage::Shutdown => {
                shutdown_flag.store(true, Ordering::Release);
                break;
            }
        }
    }
}

fn persist_allocation(prepared: &StagedColumn, descriptor: &PageDescriptor) -> io::Result<()> {
    let page_io = PageIO {};
    page_io.write_to_path(
        &descriptor.disk_path,
        descriptor.offset,
        prepared.serialized.clone(),
    )
}

fn apply_operations(page: &mut PageCacheEntryUncompressed, operations: &[UpdateOp]) {
    page.mutate_disk_page(|disk_page| {
        for op in operations {
            match op {
                UpdateOp::Overwrite { row, entry } => {
                    let row_idx = *row as usize;
                    ensure_capacity(disk_page, row_idx);
                    disk_page.entries[row_idx] = entry.clone();
                }
                UpdateOp::Append { entry } => {
                    disk_page.entries.push(entry.clone());
                }
                UpdateOp::InsertAt { row, entry } => {
                    let len = disk_page.entries.len() as u64;
                    let target = (*row).min(len);
                    let row_idx = target as usize;
                    disk_page.entries.insert(row_idx, entry.clone());
                }
                UpdateOp::BufferRow { .. } => {
                    // BufferRow ops should be intercepted before staging.
                }
            }
        }
    });
}

fn ensure_capacity(page: &mut Page, row_idx: usize) {
    if row_idx < page.entries.len() {
        return;
    }

    let missing = row_idx + 1 - page.entries.len();
    page.entries.extend((0..missing).map(|_| Entry::new("")));
}

#[derive(Debug)]
pub enum WriterError {
    Shutdown,
    ChannelClosed,
    Wal(std::io::Error),
    Serialization(String),
}

fn compare_rows(left: &[String], right: &[String], sort_ordinals: &[usize]) -> CmpOrdering {
    for &ordinal in sort_ordinals {
        let left_val = left.get(ordinal);
        let right_val = right.get(ordinal);
        match (left_val, right_val) {
            (Some(l), Some(r)) => {
                let cmp = compare_field_values(l, r);
                if cmp != CmpOrdering::Equal {
                    return cmp;
                }
            }
            (Some(_), None) => return CmpOrdering::Greater,
            (None, Some(_)) => return CmpOrdering::Less,
            (None, None) => continue,
        }
    }
    CmpOrdering::Equal
}

fn compare_field_values(left: &str, right: &str) -> CmpOrdering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or_else(|| left.cmp(right)),
        _ => left.cmp(right),
    }
}
