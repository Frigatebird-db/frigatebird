use idk_uwu_ig::cache::page_cache::{
    PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::PageDirectory;
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

fn create_test_page(n: usize) -> Page {
    let mut page = Page::new();
    for i in 0..n {
        page.add_entry(Entry::new(&format!("data_{}", i)));
    }
    page
}

#[test]
fn page_locator_lookup_works() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));

    let descriptor = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let id = descriptor.id.clone();

    let locator = PageLocator::new(Arc::clone(&directory));
    let result = locator.lookup(&id);

    assert!(result.is_some());
    assert_eq!(result.unwrap().id, id);
}

#[test]
fn page_locator_latest_for_column() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    thread::sleep(Duration::from_millis(10));
    let desc2 = directory
        .register_page("col1", "test.db".to_string(), 1024)
        .unwrap();
    let id2 = desc2.id.clone();

    let locator = PageLocator::new(Arc::clone(&directory));
    let latest = locator.latest_for_column("col1");

    assert!(latest.is_some());
    assert_eq!(latest.unwrap().id, id2);
}

#[test]
fn page_locator_range_for_column() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    directory.register_page("col1", "test.db".to_string(), 1024);

    let locator = PageLocator::new(Arc::clone(&directory));
    let range = locator.range_for_column("col1", 0, 10, u64::MAX);

    assert!(range.len() >= 1);
}

#[test]
fn page_fetcher_get_cached_miss() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});

    let fetcher = PageFetcher::new(compressed_cache, page_io);
    let result = fetcher.get_cached("nonexistent");

    assert!(result.is_none());
}

#[test]
fn page_fetcher_get_cached_hit() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));

    {
        let mut cache = compressed_cache.write().unwrap();
        cache.add(
            "page1",
            PageCacheEntryCompressed {
                page: vec![1, 2, 3],
            },
        );
    }

    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(compressed_cache, page_io);
    let result = fetcher.get_cached("page1");

    assert!(result.is_some());
}

#[test]
fn page_fetcher_collect_cached_multiple() {
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));

    {
        let mut cache = compressed_cache.write().unwrap();
        cache.add(
            "page1",
            PageCacheEntryCompressed {
                page: vec![1, 2, 3],
            },
        );
        cache.add(
            "page2",
            PageCacheEntryCompressed {
                page: vec![4, 5, 6],
            },
        );
        cache.add(
            "page3",
            PageCacheEntryCompressed {
                page: vec![7, 8, 9],
            },
        );
    }

    let page_io = Arc::new(PageIO {});
    let fetcher = PageFetcher::new(compressed_cache, page_io);

    let order = vec!["page1".to_string(), "page3".to_string()];
    let results = fetcher.collect_cached(&order);

    assert_eq!(results.len(), 2);
}

#[test]
fn page_materializer_get_cached_miss() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let compressor = Arc::new(Compressor::new());

    let materializer = PageMaterializer::new(uncompressed_cache, compressor);
    let result = materializer.get_cached("nonexistent");

    assert!(result.is_none());
}

#[test]
fn page_materializer_get_cached_hit() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));

    {
        let mut cache = uncompressed_cache.write().unwrap();
        cache.add(
            "page1",
            PageCacheEntryUncompressed::from_disk_page(create_test_page(5)),
        );
    }

    let compressor = Arc::new(Compressor::new());
    let materializer = PageMaterializer::new(uncompressed_cache, compressor);
    let result = materializer.get_cached("page1");

    assert!(result.is_some());
}

#[test]
fn page_materializer_write_back() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let compressor = Arc::new(Compressor::new());

    let materializer = PageMaterializer::new(Arc::clone(&uncompressed_cache), compressor);

    let page = create_test_page(3);
    materializer.write_back("page1", PageCacheEntryUncompressed::from_disk_page(page));

    let cache = uncompressed_cache.read().unwrap();
    assert!(cache.has("page1"));
}

#[test]
fn page_materializer_materialize_one() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let compressor = Arc::new(Compressor::new());

    let materializer =
        PageMaterializer::new(Arc::clone(&uncompressed_cache), Arc::clone(&compressor));

    let page = create_test_page(5);
    let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(page));
    let compressed = Arc::new(compressor.compress(uncompressed));

    let result = materializer.materialize_one("page1", compressed);

    assert!(result.is_some());

    let cache = uncompressed_cache.read().unwrap();
    assert!(cache.has("page1"));
}

#[test]
fn page_materializer_materialize_many() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let compressor = Arc::new(Compressor::new());

    let materializer =
        PageMaterializer::new(Arc::clone(&uncompressed_cache), Arc::clone(&compressor));

    let mut items = vec![];
    for i in 0..5 {
        let page = create_test_page(i + 1);
        let uncompressed = Arc::new(PageCacheEntryUncompressed::from_disk_page(page));
        let compressed = Arc::new(compressor.compress(uncompressed));
        items.push((format!("page{}", i), compressed));
    }

    let results = materializer.materialize_many(items);
    assert_eq!(results.len(), 5);

    let cache = uncompressed_cache.read().unwrap();
    for i in 0..5 {
        assert!(cache.has(&format!("page{}", i)));
    }
}

#[test]
fn page_materializer_collect_cached() {
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));

    {
        let mut cache = uncompressed_cache.write().unwrap();
        for i in 0..3 {
            cache.add(
                &format!("page{}", i),
                PageCacheEntryUncompressed::from_disk_page(create_test_page(i + 1)),
            );
        }
    }

    let compressor = Arc::new(Compressor::new());
    let materializer = PageMaterializer::new(uncompressed_cache, compressor);

    let order = vec!["page0".to_string(), "page2".to_string()];
    let results = materializer.collect_cached(&order);

    assert_eq!(results.len(), 2);
}

#[test]
fn page_handler_new_creates_components() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
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

    let handler = PageHandler::new(locator, fetcher, materializer);

    let _addr = format!("{:p}", &handler);
}

#[test]
fn page_handler_locate_latest() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));
    directory.register_page("col1", "test.db".to_string(), 0);

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

    let handler = PageHandler::new(locator, fetcher, materializer);

    let result = handler.locate_latest("col1");
    assert!(result.is_some());
}

#[test]
fn page_handler_ensure_pages_cached_empty() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
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

    let handler = PageHandler::new(locator, fetcher, materializer);

    handler.ensure_pages_cached(&[]); // Should not panic
}

#[test]
fn page_handler_ensure_pages_cached_already_in_cache() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));

    let descriptor = directory
        .register_page("col1", "test.db".to_string(), 0)
        .unwrap();
    let id = descriptor.id.clone();

    {
        let mut cache = uncompressed_cache.write().unwrap();
        cache.add(
            &id,
            PageCacheEntryUncompressed::from_disk_page(create_test_page(5)),
        );
    }

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

    let handler = PageHandler::new(locator, fetcher, materializer);

    handler.ensure_pages_cached(&[id.clone()]);

    let cache = uncompressed_cache.read().unwrap();
    assert!(cache.has(&id));
}

#[test]
fn page_handler_get_pages_with_prefetch_zero_k() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
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

    let handler = PageHandler::new(locator, fetcher, materializer);

    let page_ids = vec!["page1".to_string(), "page2".to_string()];
    let results = handler.get_pages_with_prefetch(&page_ids, 0);

    assert_eq!(results.len(), 0);
}

#[test]
fn page_handler_get_pages_with_prefetch_empty() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
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

    let handler = PageHandler::new(locator, fetcher, materializer);

    let results = handler.get_pages_with_prefetch(&[], 5);
    assert_eq!(results.len(), 0);
}

#[test]
fn page_handler_write_back_uncompressed() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
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

    let handler = PageHandler::new(locator, fetcher, materializer);

    let page = create_test_page(10);
    handler.write_back_uncompressed("page1", PageCacheEntryUncompressed::from_disk_page(page));

    let cache = uncompressed_cache.read().unwrap();
    assert!(cache.has("page1"));
}

#[test]
fn page_handler_concurrent_access() {
    let store = Arc::new(RwLock::new(
        idk_uwu_ig::metadata_store::TableMetaStore::new(),
    ));
    let directory = Arc::new(PageDirectory::new(store));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let compressor = Arc::new(Compressor::new());

    for i in 0..10 {
        directory.register_page(&format!("col{}", i), "test.db".to_string(), i * 1024);
    }

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
    let mut handles = vec![];

    for i in 0..10 {
        let h = Arc::clone(&handler);
        let handle = thread::spawn(move || {
            let result = h.locate_latest(&format!("col{}", i));
            assert!(result.is_some());
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
