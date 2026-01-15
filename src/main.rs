#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    unused_assignments,
    unused_mut
)]

use crate::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::page_handler::{
    PageFetcher, PageHandler, PageLocator, PageMaterializer, page_io::PageIO,
};
use crate::wal::{FsyncSchedule, ReadConsistency, Walrus};
use crate::writer::{
    DirectBlockAllocator, DirectoryMetadataClient, MetadataClient, PageAllocator, Writer,
};
mod cache;
mod entry;
mod helpers;
mod metadata_store;
mod ops_handler;
mod executor;
mod page;
mod page_handler;
mod pipeline;
mod pool;
mod sql;
#[path = "wal/lib.rs"]
mod wal;
mod writer;
use cache::lifecycle::{CompressedToDiskLifecycle, UncompressedToCompressedLifecycle};
use helpers::compressor::Compressor;
use metadata_store::{MetaJournal, PageDirectory, TableMetaStore};
use std::sync::{Arc, RwLock};

fn main() {
    let compressed_page_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let uncompressed_page_cache =
        Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let page_io = Arc::new(PageIO {});
    let metadata_store = Arc::new(RwLock::new(TableMetaStore::new()));
    let page_directory = Arc::new(PageDirectory::new(Arc::clone(&metadata_store)));
    let compressor = Arc::new(Compressor::new());

    {
        let lifecycle = Arc::new(UncompressedToCompressedLifecycle::new(
            Arc::clone(&compressor),
            Arc::clone(&compressed_page_cache),
        ));
        let mut upc = uncompressed_page_cache.write().unwrap();
        upc.set_lifecycle(Some(lifecycle));
    }

    {
        let lifecycle = Arc::new(CompressedToDiskLifecycle::new(
            Arc::clone(&page_io),
            Arc::clone(&page_directory),
        ));
        let mut cpc = compressed_page_cache.write().unwrap();
        cpc.set_lifecycle(Some(lifecycle));
    }

    let locator = Arc::new(PageLocator::new(Arc::clone(&page_directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_page_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_page_cache),
        Arc::clone(&compressor),
    ));
    let page_handler = Arc::new(PageHandler::new(locator, fetcher, materializer));

    let allocator: Arc<dyn PageAllocator> =
        Arc::new(DirectBlockAllocator::new().expect("allocator init failed"));
    let metadata_client: Arc<dyn MetadataClient>;
    let writer_wal = Arc::new(
        Walrus::with_consistency_and_schedule_for_key(
            "satori-writer",
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .expect("writer wal init failed"),
    );
    let meta_wal = Arc::new(
        Walrus::with_consistency_and_schedule_for_key(
            "satori-meta",
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .expect("metadata wal init failed"),
    );
    let meta_journal = Arc::new(MetaJournal::new(Arc::clone(&meta_wal), 16));
    meta_journal
        .replay_into(&page_directory)
        .expect("metadata journal replay failed");
    metadata_client = Arc::new(DirectoryMetadataClient::new(
        Arc::clone(&page_directory),
        Arc::clone(&meta_journal),
    ));

    let _writer = Writer::new(
        Arc::clone(&page_handler),
        Arc::clone(&allocator),
        Arc::clone(&metadata_client),
        Arc::clone(&writer_wal),
    );

    // cleanup
    drop(uncompressed_page_cache);
    drop(compressed_page_cache);
    drop(page_io);
    drop(compressor);
}
