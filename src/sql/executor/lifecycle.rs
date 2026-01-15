use crate::executor::PipelineExecutor;
use crate::metadata_store::{MetaJournal, PageDirectory};
use crate::page_handler::PageHandler;
use crate::wal::{FsyncSchedule, ReadConsistency, Walrus};
use crate::writer::{
    DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator, Writer,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::core::SqlExecutor;
use super::wal_options::SqlExecutorWalOptions;

static SQL_EXECUTOR_WAL_COUNTER: AtomicUsize = AtomicUsize::new(0);
const SQL_EXECUTOR_WAL_PREFIX: &str = "sql-executor-";

impl SqlExecutor {
    pub fn new(page_handler: Arc<PageHandler>, page_directory: Arc<PageDirectory>) -> Self {
        Self::new_with_writer_mode(page_handler, page_directory, true)
    }

    pub fn new_with_writer_mode(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
    ) -> Self {
        let wal_id = SQL_EXECUTOR_WAL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let wal_namespace = format!("{SQL_EXECUTOR_WAL_PREFIX}{wal_id}");
        let options = SqlExecutorWalOptions::new(wal_namespace);
        SqlExecutor::with_wal_options(page_handler, page_directory, use_writer_inserts, options)
    }

    pub fn with_wal_options(
        page_handler: Arc<PageHandler>,
        page_directory: Arc<PageDirectory>,
        use_writer_inserts: bool,
        options: SqlExecutorWalOptions,
    ) -> Self {
        let SqlExecutorWalOptions {
            namespace,
            cleanup_on_drop,
            reset_namespace,
            storage_dir,
            fsync_schedule,
            wal_enabled,
        } = options;
        let allocator: Arc<dyn PageAllocator> = if let Some(dir) = storage_dir {
            Arc::new(DirectBlockAllocator::with_data_dir(dir).expect("allocator init failed"))
        } else {
            Arc::new(DirectBlockAllocator::new().expect("allocator init failed"))
        };
        crate::sql::runtime::wal_helpers::ensure_sql_executor_wal_root();
        let meta_namespace = format!("{namespace}-meta");
        if reset_namespace {
            crate::sql::runtime::wal_helpers::remove_sql_executor_wal_dir(&namespace);
            crate::sql::runtime::wal_helpers::remove_sql_executor_wal_dir(&meta_namespace);
        }
        let wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &namespace,
                ReadConsistency::StrictlyAtOnce,
                fsync_schedule,
            )
            .expect("wal init failed"),
        );
        let meta_wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &meta_namespace,
                ReadConsistency::AtLeastOnce {
                    persist_every: u32::MAX,
                },
                FsyncSchedule::SyncEach,
            )
            .expect("metadata wal init failed"),
        );
        let meta_journal = Arc::new(MetaJournal::new(Arc::clone(&meta_wal), 16));
        meta_journal
            .replay_into(&page_directory)
            .expect("metadata journal replay failed");
        let metadata_client: Arc<dyn MetadataClient> = Arc::new(DirectoryMetadataClient::new(
            Arc::clone(&page_directory),
            Arc::clone(&meta_journal),
        ));

        let shard_count = crate::writer::GLOBAL_WRITER_SHARD_COUNT
            .load(AtomicOrdering::Acquire)
            .max(1);

        let writer = Arc::new(Writer::with_shard_count(
            Arc::clone(&page_handler),
            allocator,
            metadata_client,
            wal,
            shard_count,
            wal_enabled,
        ));
        let executor_threads = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(2);
        let pipeline_executor = Arc::new(PipelineExecutor::new(executor_threads));

        SqlExecutor {
            page_handler,
            page_directory,
            writer,
            pipeline_executor: Some(pipeline_executor),
            meta_journal: Some(meta_journal),
            use_writer_inserts,
            wal_namespace: namespace,
            meta_namespace,
            cleanup_wal_on_drop: cleanup_on_drop,
        }
    }
}
