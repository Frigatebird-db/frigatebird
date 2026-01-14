use crate::metadata_store::{MetaJournal, PageDirectory, TableCatalog};
use crate::page_handler::PageHandler;
use crate::writer::Writer;
use std::sync::Arc;

pub struct SqlExecutor {
    pub(crate) page_handler: Arc<PageHandler>,
    pub(crate) page_directory: Arc<PageDirectory>,
    pub(crate) writer: Arc<Writer>,
    pub(crate) meta_journal: Option<Arc<MetaJournal>>,
    pub(crate) use_writer_inserts: bool,
    pub(crate) wal_namespace: String,
    pub(crate) meta_namespace: String,
    pub(crate) cleanup_wal_on_drop: bool,
}

impl SqlExecutor {
    pub(crate) fn table_catalog(&self, table: &str) -> Option<TableCatalog> {
        self.page_directory.table_catalog(table)
    }

    pub(crate) fn page_handler(&self) -> &Arc<PageHandler> {
        &self.page_handler
    }

    pub(crate) fn writer(&self) -> &Writer {
        &self.writer
    }

    pub(crate) fn use_writer_inserts(&self) -> bool {
        self.use_writer_inserts
    }

    pub(crate) fn page_directory(&self) -> &Arc<PageDirectory> {
        &self.page_directory
    }

    pub(crate) fn meta_journal(&self) -> Option<&MetaJournal> {
        self.meta_journal.as_ref().map(|journal| journal.as_ref())
    }
}

impl Drop for SqlExecutor {
    fn drop(&mut self) {
        if self.cleanup_wal_on_drop {
            crate::sql::runtime::wal_helpers::remove_sql_executor_wal_dir(&self.wal_namespace);
            crate::sql::runtime::wal_helpers::remove_sql_executor_wal_dir(&self.meta_namespace);
        }
    }
}

// Legacy row-id refinement path removed after pipeline refactor.
