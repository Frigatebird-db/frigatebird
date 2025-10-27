use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page::Page;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

fn create_page(entries: usize) -> Page {
    let mut page = Page::new();
    for i in 0..entries {
        page.add_entry(Entry::new(&format!("entry_{}", i)));
    }
    page
}

// Test range query edge cases that previously failed
#[test]
fn range_query_exact_boundary() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Register pages that create entry range [0, 3)
    directory.register_page("col1", "test.db".to_string(), 0);
    directory.register_page("col1", "test.db".to_string(), 1024);
    directory.register_page("col1", "test.db".to_string(), 2048);

    // Test exact boundary: should this include the entry at [0,3)?
    let results = directory.range("col1", 0, 3, u64::MAX);
    assert!(results.len() > 0, "Range [0,3) should find entry [0,3)");

    // Test one before boundary
    let results = directory.range("col1", 0, 2, u64::MAX);
    assert!(results.len() > 0, "Range [0,2) should find entry [0,3)");

    // Test one after start
    let results = directory.range("col1", 1, 3, u64::MAX);
    assert!(results.len() > 0, "Range [1,3) should find entry [0,3)");
}

#[test]
fn range_query_zero_width() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Zero-width range
    let results = directory.range("col1", 5, 5, u64::MAX);
    assert_eq!(results.len(), 0, "Zero-width range should return empty");

    // Inverted range
    let results = directory.range("col1", 10, 5, u64::MAX);
    assert_eq!(results.len(), 0, "Inverted range should return empty");
}

#[test]
fn range_query_far_past_existing_entries() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Query far beyond existing entries
    let results = directory.range("col1", 1000, 2000, u64::MAX);
    assert_eq!(results.len(), 0, "Range far beyond entries should be empty");
}

#[test]
fn mvcc_multiple_versions_same_timestamp() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Register multiple pages as fast as possible (might get same timestamp)
    let desc1 = directory.register_page("col1", "test.db".to_string(), 0);
    let desc2 = directory.register_page("col1", "test.db".to_string(), 1024);
    let desc3 = directory.register_page("col1", "test.db".to_string(), 2048);

    assert!(desc1.is_some());
    assert!(desc2.is_some());
    assert!(desc3.is_some());

    // Should still be able to query even if timestamps collide
    let results = directory.range("col1", 0, 10, u64::MAX);
    assert!(results.len() > 0, "Should find at least one version");
}

#[test]
fn mvcc_timestamp_boundary_exact_match() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    thread::sleep(Duration::from_millis(5));
    let timestamp = idk_uwu_ig::entry::current_epoch_millis();

    thread::sleep(Duration::from_millis(5));
    directory.register_page("col1", "test.db".to_string(), 1024);

    // Query with exact timestamp - should find the first page only
    let results = directory.range("col1", 0, 10, timestamp);
    assert!(results.len() > 0, "Should find version before timestamp");
}

#[test]
fn mvcc_timestamp_before_all_versions() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let old_timestamp = idk_uwu_ig::entry::current_epoch_millis();

    thread::sleep(Duration::from_millis(10));
    directory.register_page("col1", "test.db".to_string(), 0);

    // Query with timestamp before any version exists
    let results = directory.range("col1", 0, 10, old_timestamp);
    assert_eq!(results.len(), 0, "Should find no versions before timestamp");
}

#[test]
fn page_cache_eviction_lru_order_stress() {
    let mut cache = PageCache::new();

    // Fill to capacity (10 pages)
    for i in 0..10 {
        cache.add(&format!("page{}", i), create_page(5));
    }

    // Re-add some pages to move them to front of LRU: 1, 3, 5, 7, 9
    for i in (1..10).step_by(2) {
        cache.add(&format!("page{}", i), create_page(5));
    }

    // Add one more page - should evict oldest (page0)
    cache.add("page10", create_page(5));

    // page0 should be evicted (oldest), page1 should remain (recently re-added)
    assert!(
        !cache.has("page0"),
        "page0 should have been evicted as oldest"
    );
    assert!(
        cache.has("page1"),
        "page1 was re-added recently, should remain"
    );
    assert!(
        cache.has("page10"),
        "page10 was just added, should be present"
    );
}

#[test]
fn page_cache_concurrent_eviction_and_add() {
    let cache = Arc::new(RwLock::new(PageCache::new()));

    // Fill cache to capacity
    {
        let mut c = cache.write().unwrap();
        for i in 0..10 {
            c.add(&format!("init{}", i), create_page(1));
        }
    }

    let mut handles = vec![];

    // Spawn threads that add pages concurrently
    for i in 0..20 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            let mut c = cache_clone.write().unwrap();
            c.add(&format!("thread{}", i), create_page(1));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Cache should still be at capacity
    let c = cache.read().unwrap();
    let mut count = 0;
    for i in 0..10 {
        if c.has(&format!("init{}", i)) {
            count += 1;
        }
    }
    for i in 0..20 {
        if c.has(&format!("thread{}", i)) {
            count += 1;
        }
    }
    assert!(count <= 10, "Cache should not exceed capacity");
}

#[test]
fn compressor_zero_byte_page() {
    let compressor = Compressor::new();
    let page = Page::new(); // Empty page

    let uncompressed = Arc::new(PageCacheEntryUncompressed { page });
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(decompressed.page.entries.len(), 0);
}

#[test]
fn compressor_single_byte_entries() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Add 100 single-byte entries
    for _ in 0..100 {
        page.add_entry(Entry::new("a"));
    }

    let uncompressed = Arc::new(PageCacheEntryUncompressed { page: page.clone() });
    let compressed = compressor.compress(Arc::clone(&uncompressed));
    let decompressed = compressor.decompress(Arc::new(compressed));

    assert_eq!(decompressed.page.entries.len(), 100);
    assert_eq!(
        uncompressed.page.entries.len(),
        decompressed.page.entries.len()
    );
}

#[test]
fn entry_max_size_string() {
    // Test with very large entry (1MB)
    let large_data = "x".repeat(1_000_000);
    let entry = Entry::new(&large_data);

    // Serialize and check size
    let serialized = bincode::serialize(&entry).unwrap();
    assert!(
        serialized.len() > 1_000_000,
        "Large entry should serialize to >1MB"
    );
}

#[test]
fn entry_unicode_boundary_cases() {
    // Emoji and multi-byte characters
    let entry = Entry::new("👍🏽🎉🚀");
    let serialized = bincode::serialize(&entry).unwrap();
    assert!(serialized.len() > 0);

    // Mix of ASCII and unicode
    let entry = Entry::new("Hello 世界 мир");
    let serialized = bincode::serialize(&entry).unwrap();
    assert!(serialized.len() > 0);

    // Zero-width joiners and combining characters
    let entry = Entry::new("e\u{0301}"); // é with combining acute
    let serialized = bincode::serialize(&entry).unwrap();
    assert!(serialized.len() > 0);
}

#[test]
fn concurrent_register_same_column() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let mut handles = vec![];

    // All threads register to same column concurrently
    for i in 0..50 {
        let dir = Arc::clone(&directory);
        let handle = thread::spawn(move || {
            dir.register_page("col1", format!("test{}.db", i), i * 1024);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should be able to query the column
    let results = directory.range("col1", 0, 100, u64::MAX);
    assert!(results.len() > 0, "Should find at least one page");
}

#[test]
fn concurrent_register_and_query() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Pre-register one page
    directory.register_page("col1", "test.db".to_string(), 0);

    let dir1 = Arc::clone(&directory);
    let writer = thread::spawn(move || {
        for i in 0..20 {
            dir1.register_page("col1", format!("test{}.db", i), i * 1024);
            thread::sleep(Duration::from_millis(1));
        }
    });

    let dir2 = Arc::clone(&directory);
    let reader = thread::spawn(move || {
        for _ in 0..20 {
            let results = dir2.range("col1", 0, 100, u64::MAX);
            assert!(results.len() > 0, "Should always find at least one page");
            thread::sleep(Duration::from_millis(1));
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn page_directory_lookup_after_many_versions() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Register 100 versions of the same entry range
    let mut ids = vec![];
    for i in 0..100 {
        if let Some(desc) = directory.register_page("col1", format!("test{}.db", i), i * 1024) {
            ids.push(desc.id);
        }
    }

    // All IDs should be lookupable
    for id in &ids {
        let result = directory.lookup(id);
        assert!(
            result.is_some(),
            "Should be able to lookup any registered ID"
        );
    }

    // Query should still work with MVCC max versions (8)
    let results = directory.range("col1", 0, 10, u64::MAX);
    assert!(results.len() > 0, "Should find at least one version");
}

#[test]
fn cache_update_same_key_rapidly() {
    let mut cache = PageCache::new();

    // Rapidly update the same key
    for i in 0..100 {
        cache.add("page1", create_page(i % 10 + 1));
    }

    // Should have the latest version
    let page = cache.get("page1").unwrap();
    assert!(page.entries.len() > 0);
}

#[test]
fn compressor_highly_repetitive_data() {
    let compressor = Compressor::new();
    let mut page = Page::new();

    // Add same entry 1000 times
    for _ in 0..1000 {
        page.add_entry(Entry::new("aaaaaaaaaaaaaaaaaaaa"));
    }

    let original_size = bincode::serialize(&page).unwrap().len();
    let uncompressed = Arc::new(PageCacheEntryUncompressed { page });
    let compressed = compressor.compress(Arc::clone(&uncompressed));

    // Highly repetitive data should compress very well
    assert!(
        compressed.page.len() < original_size / 50,
        "Highly repetitive data should compress to <2% of original size"
    );
}

#[test]
fn empty_column_range_query() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    // Query column that has never been registered
    let results = directory.range("nonexistent", 0, 100, u64::MAX);
    assert_eq!(results.len(), 0, "Nonexistent column should return empty");
}

#[test]
fn max_u64_timestamp_query() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Query with maximum possible timestamp
    let results = directory.range("col1", 0, 10, u64::MAX);
    assert!(
        results.len() > 0,
        "u64::MAX timestamp should find all versions"
    );
}

#[test]
fn zero_timestamp_query() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    // Query with zero timestamp (before any possible version)
    let results = directory.range("col1", 0, 10, 0);
    assert_eq!(results.len(), 0, "Timestamp 0 should find no versions");
}
