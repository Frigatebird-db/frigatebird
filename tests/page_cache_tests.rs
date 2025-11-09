use idk_uwu_ig::cache::page_cache::{
    CacheLifecycle, PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::page::Page;
use std::sync::{Arc, Mutex};

fn create_test_page(id: usize) -> Page {
    let mut page = Page::new();
    page.add_entry(Entry::new(&format!("data_{}", id)));
    page
}

#[test]
fn page_cache_new_creates_empty() {
    let cache: PageCache<Page> = PageCache::new();
    assert_eq!(cache.store.len(), 0);
    assert_eq!(cache.lru_queue.len(), 0);
}

#[test]
fn page_cache_add_single_page() {
    let mut cache = PageCache::new();
    let page = create_test_page(1);
    cache.add("page1", page);

    assert!(cache.has("page1"));
    assert_eq!(cache.store.len(), 1);
    assert_eq!(cache.lru_queue.len(), 1);
}

#[test]
fn page_cache_add_multiple_pages() {
    let mut cache = PageCache::new();

    for i in 0..5 {
        cache.add(&format!("page{}", i), create_test_page(i));
    }

    assert_eq!(cache.store.len(), 5);
    assert_eq!(cache.lru_queue.len(), 5);
}

#[test]
fn page_cache_get_existing_page() {
    let mut cache = PageCache::new();
    let page = create_test_page(1);
    cache.add("page1", page);

    let retrieved = cache.get("page1");
    assert!(retrieved.is_some());
}

#[test]
fn page_cache_get_nonexistent_page_returns_none() {
    let cache: PageCache<Page> = PageCache::new();
    assert!(cache.get("nonexistent").is_none());
}

#[test]
fn page_cache_has_returns_correct_status() {
    let mut cache = PageCache::new();
    assert!(!cache.has("page1"));

    cache.add("page1", create_test_page(1));
    assert!(cache.has("page1"));
}

#[test]
fn page_cache_eviction_at_capacity() {
    let mut cache = PageCache::new();
    let capacity = PageCache::<Page>::capacity_limit();

    // Add pages up to and beyond capacity
    for i in 0..(capacity + 2) {
        cache.add(&format!("page{}", i), create_test_page(i));
    }

    // Should only have `capacity` pages
    assert_eq!(cache.store.len(), capacity);
    assert_eq!(cache.lru_queue.len(), capacity);

    // Oldest pages should be evicted
    assert!(!cache.has("page0"));
    assert!(!cache.has("page1"));
    assert!(cache.has(&format!("page{}", capacity + 1)));
}

#[test]
fn page_cache_lru_order() {
    let mut cache = PageCache::new();
    let capacity = PageCache::<Page>::capacity_limit();

    for i in 0..capacity {
        cache.add(&format!("page{}", i), create_test_page(i));
        std::thread::sleep(std::time::Duration::from_micros(100));
    }

    // Add a few more to trigger evictions
    for i in capacity..(capacity + 5) {
        cache.add(&format!("page{}", i), create_test_page(i));
        std::thread::sleep(std::time::Duration::from_micros(100));
    }

    // First page added should be evicted first
    assert!(!cache.has("page0"));
}

#[test]
fn page_cache_update_existing_page() {
    let mut cache = PageCache::new();
    cache.add("page1", create_test_page(1));

    let old_size = cache.lru_queue.len();

    // Update with new page
    cache.add("page1", create_test_page(2));

    // Should still have same number of entries
    assert_eq!(cache.lru_queue.len(), old_size);
    assert!(cache.has("page1"));
}

#[test]
fn page_cache_manual_evict() {
    let mut cache = PageCache::new();
    cache.add("page1", create_test_page(1));

    assert!(cache.has("page1"));
    cache.evict("page1");

    assert!(!cache.has("page1"));
    assert_eq!(cache.store.len(), 0);
    assert_eq!(cache.lru_queue.len(), 0);
}

#[test]
fn page_cache_evict_nonexistent() {
    let mut cache: PageCache<Page> = PageCache::new();
    cache.evict("nonexistent"); // Should not panic
    assert_eq!(cache.store.len(), 0);
}

struct TestLifecycle {
    evicted: Arc<Mutex<Vec<String>>>,
}

impl CacheLifecycle<Page> for TestLifecycle {
    fn on_evict(&self, id: &str, _data: Arc<Page>) {
        self.evicted.lock().unwrap().push(id.to_string());
    }
}

#[test]
fn page_cache_lifecycle_called_on_evict() {
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(TestLifecycle {
        evicted: Arc::clone(&evicted),
    });

    let mut cache = PageCache::with_lifecycle(Some(lifecycle));

    cache.add("page1", create_test_page(1));
    cache.evict("page1");

    let evicted_ids = evicted.lock().unwrap();
    assert_eq!(evicted_ids.len(), 1);
    assert_eq!(evicted_ids[0], "page1");
}

#[test]
fn page_cache_lifecycle_called_on_capacity_evict() {
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(TestLifecycle {
        evicted: Arc::clone(&evicted),
    });

    let mut cache = PageCache::with_lifecycle(Some(lifecycle));
    let capacity = PageCache::<Page>::capacity_limit();

    // Add enough pages to trigger eviction
    for i in 0..(capacity + 1) {
        cache.add(&format!("page{}", i), create_test_page(i));
        std::thread::sleep(std::time::Duration::from_micros(100));
    }

    let evicted_ids = evicted.lock().unwrap();
    assert_eq!(evicted_ids.len(), 1);
    assert_eq!(evicted_ids[0], "page0");
}

#[test]
fn page_cache_set_lifecycle_after_creation() {
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(TestLifecycle {
        evicted: Arc::clone(&evicted),
    });

    let mut cache: PageCache<Page> = PageCache::new();
    cache.set_lifecycle(Some(lifecycle));

    cache.add("page1", create_test_page(1));
    cache.evict("page1");

    let evicted_ids = evicted.lock().unwrap();
    assert_eq!(evicted_ids.len(), 1);
}

#[test]
fn page_cache_compressed_entry_creation() {
    let compressed = PageCacheEntryCompressed {
        page: vec![1, 2, 3, 4],
    };
    assert_eq!(compressed.page.len(), 4);
}

#[test]
fn page_cache_uncompressed_entry_creation() {
    let page = create_test_page(1);
    let uncompressed = PageCacheEntryUncompressed::from_disk_page(page);
    assert_eq!(uncompressed.page.len(), 1);
}

#[test]
fn page_cache_arc_sharing() {
    let mut cache = PageCache::new();
    cache.add("page1", create_test_page(1));

    let arc1 = cache.get("page1").unwrap();
    let arc2 = cache.get("page1").unwrap();

    // Both Arcs should point to same data
    assert!(Arc::ptr_eq(&arc1, &arc2));
}

#[test]
fn page_cache_rapid_adds_and_evicts() {
    let mut cache = PageCache::new();
    let capacity = PageCache::<Page>::capacity_limit();
    let total = capacity + 100;

    for i in 0..total {
        cache.add(&format!("page{}", i), create_test_page(i));
    }

    // Should maintain capacity
    assert_eq!(cache.store.len(), capacity);
    assert_eq!(cache.lru_queue.len(), capacity);

    // Should have latest pages
    assert!(cache.has(&format!("page{}", total - 1)));
    assert!(cache.has(&format!("page{}", total - 10)));
}

#[test]
fn page_cache_clone_entries() {
    let compressed = PageCacheEntryCompressed {
        page: vec![1, 2, 3],
    };
    let cloned = compressed.clone();
    assert_eq!(compressed.page, cloned.page);

    let page = create_test_page(1);
    let uncompressed = PageCacheEntryUncompressed::from_disk_page(page);
    let cloned_unc = uncompressed.clone();
    assert_eq!(uncompressed.page.len(), cloned_unc.page.len());
}
