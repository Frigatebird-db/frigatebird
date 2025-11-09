use idk_uwu_ig::cache::page_cache::{
    PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{
    PageDescriptor, PageDirectory, TableMetaStore, ROWS_PER_PAGE_GROUP,
};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
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

// Prefetch edge cases
// Note: Most prefetch tests require disk I/O and are covered in integration tests

#[test]
fn prefetch_k_equals_zero() {
    let (handler, directory) = setup_page_handler();

    let desc1 = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let desc2 = directory
        .register_page("col1", "test.db".to_string(), 1024)
        .unwrap();

    let page_ids = vec![desc1.id, desc2.id];
    let results = handler.get_pages_with_prefetch(&page_ids, 0);

    assert_eq!(results.len(), 0, "k=0 should return empty immediately");
}

#[test]
fn prefetch_empty_list() {
    let (handler, _directory) = setup_page_handler();

    let page_ids: Vec<String> = vec![];
    let results = handler.get_pages_with_prefetch(&page_ids, 5);

    assert_eq!(results.len(), 0, "Empty list should return empty");
}

#[test]
fn prefetch_nonexistent_pages() {
    let (handler, _directory) = setup_page_handler();

    let page_ids = vec![
        "fake1".to_string(),
        "fake2".to_string(),
        "fake3".to_string(),
    ];
    let results = handler.get_pages_with_prefetch(&page_ids, 2);

    // Should handle nonexistent pages gracefully
    assert_eq!(results.len(), 0, "Nonexistent pages should return empty");
}

#[test]
fn ensure_pages_cached_empty_list() {
    let (handler, _directory) = setup_page_handler();

    let empty: Vec<String> = vec![];
    handler.ensure_pages_cached(&empty);
    // Should not panic
}

#[test]
fn ensure_pages_cached_nonexistent() {
    let (handler, _directory) = setup_page_handler();

    let ids = vec!["fake1".to_string(), "fake2".to_string()];
    handler.ensure_pages_cached(&ids);
    // Should handle gracefully without panicking
}

#[test]
fn ensure_pages_cached_already_cached() {
    let (handler, directory) = setup_page_handler();

    let desc = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let page = create_test_page(5);
    handler.write_back_uncompressed(&desc.id, PageCacheEntryUncompressed::from_disk_page(page));

    // Call again - should be no-op
    handler.ensure_pages_cached(&[desc.id.clone()]);
    handler.ensure_pages_cached(&[desc.id]);
    // Should not panic or cause issues
}

// Batch operation edge cases
// Note: Actual batch I/O tests require real disk files and are covered in integration tests

#[test]
fn fetch_batch_empty_list() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(compressed_cache, page_io);

    let empty_descs: Vec<PageDescriptor> = vec![];
    let results = fetcher.fetch_and_insert_batch(&empty_descs);

    assert_eq!(results.len(), 0, "Empty batch should return empty");
}

// Compressor stress tests

#[test]
fn compress_alternating_pattern() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Alternating high-low entropy data
    for i in 0..100 {
        if i % 2 == 0 {
            page.add_entry(Entry::new("aaaaaaaaaa"));
        } else {
            page.add_entry(Entry::new(&format!("{:x}", i)));
        }
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(page));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(decompressed.len(), 100);
}

#[test]
fn compress_random_binary_data() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Simulate random binary data (low compressibility)
    for i in 0..50 {
        let random_like = format!("{:x}{:x}{:x}", i * 7, i * 13, i * 19);
        page.add_entry(Entry::new(&random_like));
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(page));
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(decompressed.len(), 50);
}

#[test]
fn compress_degenerate_case_all_same_char() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Degenerate case: all entries identical
    for _ in 0..1000 {
        page.add_entry(Entry::new("a"));
    }

    let original_size = bincode::serialize(&page).unwrap().len();
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(page));
    let compressed = compressor.compress(Arc::clone(&uncompressed));

    // Should compress extremely well
    assert!(
        compressed.page.len() < original_size / 100,
        "Degenerate case should compress to <1% of original"
    );
}

// Page cache stress tests

#[test]
fn cache_thrash_add_evict_repeatedly() {
    let mut cache = PageCache::new();

    // Repeatedly add and evict by exceeding capacity
    for round in 0..10 {
        for i in 0..20 {
            cache.add(&format!("r{}p{}", round, i), create_test_page(5));
        }
    }

    // Cache should remain stable at capacity
    let mut count = 0;
    for round in 0..10 {
        for i in 0..20 {
            if cache.has(&format!("r{}p{}", round, i)) {
                count += 1;
            }
        }
    }
    assert!(
        count <= PageCache::<PageCacheEntryUncompressed>::capacity_limit(),
        "Cache should maintain capacity limit"
    );
}

#[test]
fn cache_concurrent_readers_single_writer() {
    let cache = Arc::new(RwLock::new(PageCache::new()));

    // Writer adds pages
    {
        let mut c = cache.write().unwrap();
        for i in 0..10 {
            c.add(&format!("page{}", i), create_test_page(5));
        }
    }

    let mut handles = vec![];

    // Many concurrent readers
    for _ in 0..20 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let c = cache_clone.read().unwrap();
                for i in 0..10 {
                    let _ = c.has(&format!("page{}", i));
                }
            }
        });
        handles.push(handle);
    }

    // Single writer updating
    let cache_clone = Arc::clone(&cache);
    let writer = thread::spawn(move || {
        for i in 0..50 {
            let mut c = cache_clone.write().unwrap();
            c.add(&format!("new{}", i), create_test_page(3));
        }
    });

    for handle in handles {
        handle.join().unwrap();
    }
    writer.join().unwrap();
}

#[test]
fn column_chain_handles_many_versions() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    for i in 0..20 {
        let entry_count = if i == 19 {
            ROWS_PER_PAGE_GROUP / 2
        } else {
            ROWS_PER_PAGE_GROUP
        };
        directory
            .register_page_with_sizes(
                "col1",
                format!("test{}.db", i),
                i * 1024,
                256 * 1024,
                entry_count,
                entry_count,
            )
            .unwrap();
    }

    let latest = directory.latest("col1").unwrap();
    assert_eq!(latest.disk_path, "test19.db");
    assert_eq!(latest.offset, 19 * 1024);
    assert_eq!(latest.entry_count, ROWS_PER_PAGE_GROUP / 2);

    let total_rows = 19 * ROWS_PER_PAGE_GROUP + (ROWS_PER_PAGE_GROUP / 2);
    let slices = directory.locate_range("col1", 0, total_rows.saturating_sub(1));
    assert_eq!(slices.len(), 20);
    assert_eq!(slices[0].descriptor.entry_count, ROWS_PER_PAGE_GROUP);
    assert_eq!(slices[19].descriptor.entry_count, ROWS_PER_PAGE_GROUP / 2);
}
