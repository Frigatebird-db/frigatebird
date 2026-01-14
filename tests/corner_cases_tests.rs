use crossbeam::channel as crossbeam_channel;
use idk_uwu_ig::cache::page_cache::{
    PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDescriptor, PageDirectory, TableMetaStore};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::pipeline::{Job, PipelineStep};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

fn create_test_page(entries: usize) -> Page {
    let mut page = Page::new();
    for i in 0..entries {
        page.add_entry(Entry::new(&format!("data_{}", i)));
    }
    page
}

fn setup_page_handler() -> (Arc<PageHandler>, Arc<PageDirectory>) {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let compressor = Arc::new(Compressor::new());

    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        Arc::clone(&compressor),
    ));

    let handler = Arc::new(PageHandler::new(locator, fetcher, materializer));
    (handler, directory)
}

// Page/Entry corner cases

#[test]
fn entry_empty_string() {
    let entry = Entry::new("");
    assert_eq!(entry.get_data(), "");
}

#[test]
fn entry_special_characters() {
    let special = "!@#$%^&*(){}[]|\\:;\"'<>,.?/~`\n\t\r";
    let entry = Entry::new(special);
    assert_eq!(entry.get_data(), special);
}

#[test]
fn entry_null_bytes() {
    let with_null = "before\0after";
    let entry = Entry::new(with_null);
    assert_eq!(entry.get_data(), with_null);
}

#[test]
fn entry_very_long_string() {
    let long = "x".repeat(10_000_000); // 10MB
    let entry = Entry::new(&long);
    assert_eq!(entry.get_data().len(), 10_000_000);
}

#[test]
fn page_empty_entries_vector() {
    let page = Page::new();
    assert_eq!(page.entries.len(), 0);

    // Should be serializable
    let serialized = bincode::serialize(&page).unwrap();
    assert!(!serialized.is_empty());
}

#[test]
fn page_single_entry() {
    let mut page = Page::new();
    page.add_entry(Entry::new("single"));
    assert_eq!(page.entries.len(), 1);
}

#[test]
fn page_many_entries() {
    let mut page = Page::new();
    for i in 0..10000 {
        page.add_entry(Entry::new(&format!("entry_{}", i)));
    }
    assert_eq!(page.entries.len(), 10000);
}

// Compressor corner cases

#[test]
fn compress_empty_page() {
    let compressor = Compressor::new();
    let page = Page::new();
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        idk_uwu_ig::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));
    assert_eq!(decompressed.entries.len(), 0);
}

#[test]
fn compress_single_entry_page() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    page.add_entry(Entry::new("test"));
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page,
        idk_uwu_ig::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));
    assert_eq!(decompressed.entries.len(), 1);
    assert_eq!(decompressed.entries[0].get_data(), "test");
}

#[test]
fn compress_already_compressed_data() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    // Add random-like data that won't compress well
    for i in 0..100 {
        let random_like = format!("{:x}{:x}{:x}{:x}", i * 7, i * 13, i * 19, i * 23);
        page.add_entry(Entry::new(&random_like));
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page.clone(),
        idk_uwu_ig::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(decompressed.entries.len(), page.entries.len());
}

#[test]
fn compress_decompress_preserves_order() {
    let compressor = Compressor::new();
    let mut page = Page::new();
    for i in 0..100 {
        page.add_entry(Entry::new(&format!("entry_{}", i)));
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(
        page.clone(),
        idk_uwu_ig::sql::DataType::String,
    ));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    for i in 0..100 {
        assert_eq!(
            decompressed.entries[i].get_data(),
            page.entries[i].get_data()
        );
    }
}

// Metadata store corner cases

#[test]
fn metadata_empty_column_name() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let desc = directory.register_page("", "test.db".to_string(), 0);
    assert!(desc.is_some());

    let latest = directory.latest("");
    assert!(latest.is_some());
}

#[test]
fn metadata_column_name_with_special_chars() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let weird_name = "col!@#$%^&*()";
    let desc = directory.register_page(weird_name, "test.db".to_string(), 0);
    assert!(desc.is_some());

    let latest = directory.latest(weird_name);
    assert!(latest.is_some());
}

#[test]
fn metadata_range_boundary_exact_match() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Query with exact boundaries
    let results = directory.range("col1", 0, 1, u64::MAX);
    assert!(!results.is_empty());
}

#[test]
fn metadata_range_start_equals_end() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Zero-width range
    let results = directory.range("col1", 5, 5, u64::MAX);
    assert_eq!(results.len(), 0);
}

#[test]
fn metadata_range_end_before_start() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Inverted range
    let results = directory.range("col1", 10, 5, u64::MAX);
    assert_eq!(results.len(), 0);
}

#[test]
fn metadata_lookup_nonexistent_id() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let result = directory.lookup("nonexistent_id_12345");
    assert!(result.is_none());
}

#[test]
fn metadata_lookup_empty_id() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let result = directory.lookup("");
    assert!(result.is_none());
}

#[test]
fn metadata_register_same_offset_multiple_times() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Register multiple pages with same offset (different versions)
    let desc1 = directory.register_page("col1", "test.db".to_string(), 0);
    thread::sleep(Duration::from_millis(2));
    let desc2 = directory.register_page("col1", "test.db".to_string(), 0);

    assert!(desc1.is_some());
    assert!(desc2.is_some());
    assert_ne!(desc1.as_ref().unwrap().id, desc2.as_ref().unwrap().id);
}

// Cache corner cases

#[test]
fn cache_add_same_key_repeatedly() {
    let mut cache = PageCache::new();

    // Add same key multiple times
    for i in 0..50 {
        cache.add("same_key", create_test_page(i % 5 + 1));
    }

    // Should only have one entry
    assert!(cache.has("same_key"));
    let page = cache.get("same_key").unwrap();
    assert!(!page.entries.is_empty());
}

#[test]
fn cache_get_nonexistent_key() {
    let cache = PageCache::<PageCacheEntryUncompressed>::new();
    let result = cache.get("nonexistent");
    assert!(result.is_none());
}

#[test]
fn cache_has_after_eviction() {
    let mut cache = PageCache::new();
    let capacity = PageCache::<PageCacheEntryUncompressed>::capacity_limit();

    // Fill to capacity and beyond
    for i in 0..(capacity + 5) {
        cache.add(&format!("page{}", i), create_test_page(1));
    }

    // First pages should be evicted
    assert!(
        !cache.has("page0"),
        "oldest entry should be evicted once capacity is exceeded"
    );
    assert!(
        !cache.has("page1"),
        "second-oldest entry should also be evicted once capacity is exceeded"
    );

    // Recent pages should exist
    assert!(cache.has(&format!("page{}", capacity + 4)));
    assert!(cache.has(&format!("page{}", capacity + 3)));
}

#[test]
fn cache_concurrent_add_same_key() {
    let cache = Arc::new(RwLock::new(PageCache::new()));
    let mut handles = vec![];

    for i in 0..10 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            let mut c = cache_clone.write().unwrap();
            c.add("shared_key", create_test_page(i));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have the key with one of the values
    let c = cache.read().unwrap();
    assert!(c.has("shared_key"));
}

// PageHandler corner cases

#[test]
fn page_handler_get_pages_empty_list() {
    let (handler, _directory) = setup_page_handler();

    let empty: Vec<PageDescriptor> = vec![];
    let results = handler.get_pages(empty);

    assert_eq!(results.len(), 0);
}

#[test]
fn page_handler_get_pages_duplicate_descriptors() {
    let (handler, directory) = setup_page_handler();

    let desc = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    handler.write_back_uncompressed(
        &desc.id,
        PageCacheEntryUncompressed::from_disk_page(
            create_test_page(5),
            idk_uwu_ig::sql::DataType::String,
        ),
    );

    // Request same descriptor multiple times
    let descriptors = vec![desc.clone(), desc.clone(), desc.clone()];
    let results = handler.get_pages(descriptors);

    // Should handle duplicates gracefully
    assert_eq!(results.len(), 3);
}

#[test]
fn page_handler_write_back_then_read() {
    let (handler, directory) = setup_page_handler();

    let desc = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let page = create_test_page(10);

    handler.write_back_uncompressed(
        &desc.id,
        PageCacheEntryUncompressed::from_disk_page(page.clone(), idk_uwu_ig::sql::DataType::String),
    );

    let results = handler.get_page(desc);
    assert!(results.is_some());
    assert_eq!(results.unwrap().page.len(), 10);
}

#[test]
fn page_handler_concurrent_write_back_same_id() {
    let (handler, directory) = setup_page_handler();

    let desc = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let mut handles = vec![];

    for i in 0..10 {
        let handler_clone = Arc::clone(&handler);
        let id = desc.id.clone();
        let handle = thread::spawn(move || {
            handler_clone.write_back_uncompressed(
                &id,
                PageCacheEntryUncompressed::from_disk_page(
                    create_test_page(i),
                    idk_uwu_ig::sql::DataType::String,
                ),
            );
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have one of the pages
    let result = handler.get_page(desc);
    assert!(result.is_some());
}

#[test]
fn prefetch_all_pages_k_equals_total() {
    let (handler, directory) = setup_page_handler();

    let desc1 = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let desc2 = directory
        .register_page("col1", "test.db".to_string(), 1024)
        .unwrap();

    handler.write_back_uncompressed(
        &desc1.id,
        PageCacheEntryUncompressed::from_disk_page(
            create_test_page(5),
            idk_uwu_ig::sql::DataType::String,
        ),
    );
    handler.write_back_uncompressed(
        &desc2.id,
        PageCacheEntryUncompressed::from_disk_page(
            create_test_page(5),
            idk_uwu_ig::sql::DataType::String,
        ),
    );

    let page_ids = vec![desc1.id, desc2.id];
    let results = handler.get_pages_with_prefetch(&page_ids, 2);

    // Should return all pages immediately, nothing to prefetch
    assert_eq!(results.len(), 2);
}

#[test]
fn prefetch_k_greater_than_total() {
    let (handler, directory) = setup_page_handler();

    let desc = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    handler.write_back_uncompressed(
        &desc.id,
        PageCacheEntryUncompressed::from_disk_page(
            create_test_page(5),
            idk_uwu_ig::sql::DataType::String,
        ),
    );

    let page_ids = vec![desc.id];
    let results = handler.get_pages_with_prefetch(&page_ids, 100);

    // Should return all available pages
    assert_eq!(results.len(), 1);
}

// Executor/Pipeline corner cases

#[test]
fn job_with_zero_steps() {
    let job = Job {
        table_name: "test".to_string(),
        steps: vec![],
        cost: 0,
        next_free_slot: AtomicUsize::new(0),
        id: "job_zero".to_string(),
        entry_producer: crossbeam_channel::unbounded().0,
        output_receiver: crossbeam_channel::unbounded().1,
    };

    // Should handle gracefully
    job.get_next();
    assert_eq!(
        job.next_free_slot
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn job_single_step() {
    let (tx, rx) = crossbeam_channel::unbounded();
    let (tx2, _rx2) = crossbeam_channel::unbounded();
    let (page_handler, _) = setup_page_handler();

    let step = PipelineStep {
        current_producer: tx2,
        previous_receiver: rx,
        column: "col1".to_string(),
        column_ordinal: 0,
        filters: vec![],
        is_root: true,
        table: "test".to_string(),
        page_handler,
        row_ids: None,
    };

    let job = Job {
        table_name: "test".to_string(),
        steps: vec![step],
        cost: 1,
        next_free_slot: AtomicUsize::new(0),
        id: "job_single".to_string(),
        entry_producer: tx,
        output_receiver: crossbeam_channel::unbounded().1,
    };

    job.get_next();
    assert_eq!(
        job.next_free_slot
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn job_get_next_after_completion() {
    let (tx, _rx) = crossbeam_channel::unbounded();

    let job = Job {
        table_name: "test".to_string(),
        steps: vec![],
        cost: 0,
        next_free_slot: AtomicUsize::new(0),
        id: "job_complete".to_string(),
        entry_producer: tx,
        output_receiver: crossbeam_channel::unbounded().1,
    };

    // Call multiple times after completion
    job.get_next();
    job.get_next();
    job.get_next();

    // Should remain at 0 since no steps
    assert_eq!(
        job.next_free_slot
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

// PageFetcher corner cases

#[test]
fn fetcher_collect_cached_empty_list() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(compressed_cache, page_io);

    let empty: Vec<String> = vec![];
    let results = fetcher.collect_cached(&empty);

    assert_eq!(results.len(), 0);
}

#[test]
fn fetcher_collect_cached_all_miss() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(compressed_cache, page_io);

    let ids = vec![
        "miss1".to_string(),
        "miss2".to_string(),
        "miss3".to_string(),
    ];
    let results = fetcher.collect_cached(&ids);

    assert_eq!(results.len(), 0);
}

#[test]
fn fetcher_collect_cached_partial_hit() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(Arc::clone(&compressed_cache), page_io);

    // Add one page to cache
    {
        let mut cache = compressed_cache.write().unwrap();
        cache.add(
            "hit",
            PageCacheEntryCompressed {
                page: vec![1, 2, 3],
            },
        );
    }

    let ids = vec!["miss".to_string(), "hit".to_string(), "miss2".to_string()];
    let results = fetcher.collect_cached(&ids);

    // Should find the one hit
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "hit");
}

// Range query edge cases

#[test]
fn range_query_large_range() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Very large range
    let results = directory.range("col1", 0, u64::MAX, u64::MAX);
    assert!(!results.is_empty());
}

#[test]
fn range_query_max_timestamp() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    let results = directory.range("col1", 0, 10, u64::MAX);
    assert!(!results.is_empty());
}

#[test]
fn range_query_min_timestamp() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Timestamp is ignored in the simplified metadata store, so this should still return results.
    let results = directory.range("col1", 0, 10, 0);
    assert!(!results.is_empty());
}

#[test]
fn concurrent_range_queries_same_column() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Pre-populate
    for i in 0..10 {
        directory.register_page("col1", format!("test{}.db", i), i * 1024);
    }

    let mut handles = vec![];

    for _ in 0..20 {
        let dir = Arc::clone(&directory);
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                let results = dir.range("col1", 0, 100, u64::MAX);
                assert!(!results.is_empty());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
