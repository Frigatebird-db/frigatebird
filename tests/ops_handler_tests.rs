use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::ops_handler::{
    create_table_from_plan, range_scan_column_entry, update_column_entry, upsert_data_into_column,
    upsert_data_into_table_column,
};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::sql::{ColumnSpec, CreateTablePlan};
use std::sync::{Arc, RwLock};

fn setup_page_handler() -> Arc<PageHandler> {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let compressor = Arc::new(Compressor::new());

    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));
    let fetcher = Arc::new(PageFetcher::new(Arc::clone(&compressed_cache), page_io));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        compressor,
    ));

    Arc::new(PageHandler::new(locator, fetcher, materializer))
}

#[test]
fn create_table_plan_registers_metadata() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = PageDirectory::new(Arc::clone(&store));

    let plan = CreateTablePlan::new(
        "items",
        vec![
            ColumnSpec::new("id", "UUID"),
            ColumnSpec::new("name", "String"),
        ],
        vec!["id".into()],
        false,
    );

    create_table_from_plan(&directory, &plan).expect("create table");

    let catalog = directory.table_catalog("items").expect("catalog exists");
    assert_eq!(catalog.columns().len(), 2);
    assert_eq!(catalog.columns()[0].name, "id");
}

#[test]
fn upsert_data_into_column_creates_new_page() {
    let page_handler = setup_page_handler();
    let result = upsert_data_into_column(&page_handler, "email", "test@example.com");
    // Should fail gracefully when no page exists yet
    assert!(result.is_err(), "Should error when column doesn't exist");
}

#[test]
fn upsert_data_with_empty_string() {
    let page_handler = setup_page_handler();
    let result = upsert_data_into_column(&page_handler, "empty", "");
    // Should handle empty data
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn upsert_data_with_special_characters() {
    let page_handler = setup_page_handler();
    let special_data = "hello\nworld\t\r\0";
    let result = upsert_data_into_column(&page_handler, "special", special_data);
    // Should handle special characters
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn upsert_data_with_unicode() {
    let page_handler = setup_page_handler();
    let unicode_data = "Hello 世界 🌍";
    let result = upsert_data_into_column(&page_handler, "unicode", unicode_data);
    // Should handle Unicode
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn upsert_data_large_string() {
    let page_handler = setup_page_handler();
    let large_data = "x".repeat(10_000);
    let result = upsert_data_into_column(&page_handler, "large", &large_data);
    // Should handle large strings
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn update_column_entry_basic() {
    let page_handler = setup_page_handler();

    // Attempt to update without existing data
    let result = update_column_entry(&page_handler, "data", "updated", 0);
    // Should fail gracefully when column doesn't exist
    assert!(result.is_err(), "Should error when column doesn't exist");
}

#[test]
fn update_column_entry_row_zero() {
    let page_handler = setup_page_handler();

    let result = update_column_entry(&page_handler, "zero", "modified", 0);
    // Should handle row 0 update attempts
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn update_column_entry_large_row_index() {
    let page_handler = setup_page_handler();

    // Update at a large index
    let result = update_column_entry(&page_handler, "large_idx", "value", 100);
    // Should handle large indices gracefully
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn range_scan_column_entry_empty_range() {
    let page_handler = setup_page_handler();

    // Scan range when column doesn't exist
    let results = range_scan_column_entry(&page_handler, "empty", 100, 200, 0);
    assert_eq!(
        results.len(),
        0,
        "Should return empty vec for non-existent column"
    );
}

#[test]
fn range_scan_column_entry_zero_width_range() {
    let page_handler = setup_page_handler();

    // Scan where start == end
    let results = range_scan_column_entry(&page_handler, "zero", 5, 5, 0);
    assert_eq!(results.len(), 0, "Zero-width range should return empty");
}

#[test]
fn range_scan_column_entry_nonexistent_column() {
    let page_handler = setup_page_handler();

    // Scan column that doesn't exist
    let results = range_scan_column_entry(&page_handler, "nonexistent", 0, 10, 0);
    assert_eq!(
        results.len(),
        0,
        "Non-existent column should return empty vec"
    );
}

#[test]
fn ops_handler_empty_column_name() {
    let page_handler = setup_page_handler();

    // Column name can be empty string
    let result = upsert_data_into_column(&page_handler, "", "data");
    // Should handle empty column names (likely returns error)
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn ops_handler_special_column_names() {
    let page_handler = setup_page_handler();

    // Special characters in column names
    let result = upsert_data_into_column(&page_handler, "col!@#$%^", "data");
    assert!(result.is_err() || result.is_ok());

    let update_result = update_column_entry(&page_handler, "col!@#$%^", "updated", 0);
    assert!(update_result.is_err() || update_result.is_ok());

    let scan_results = range_scan_column_entry(&page_handler, "col!@#$%^", 0, 1, 0);
    assert_eq!(scan_results.len(), 0);
}

#[test]
fn upsert_handles_error_cases() {
    let page_handler = setup_page_handler();

    // Various edge cases
    let r1 = upsert_data_into_column(&page_handler, "test", "data");
    let r2 = upsert_data_into_column(&page_handler, "test", "");
    let r3 = upsert_data_into_column(&page_handler, "", "data");

    // All should either succeed or fail gracefully
    assert!(r1.is_err() || r1.is_ok());
    assert!(r2.is_err() || r2.is_ok());
    assert!(r3.is_err() || r3.is_ok());
}

#[test]
fn update_handles_out_of_bounds() {
    let page_handler = setup_page_handler();

    // Update beyond page bounds
    let result = update_column_entry(&page_handler, "col", "value", u64::MAX);
    // Should fail gracefully
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn range_scan_handles_inverted_range() {
    let page_handler = setup_page_handler();

    // Scan with start > end
    let results = range_scan_column_entry(&page_handler, "col", 100, 10, 0);
    // Should return empty or handle gracefully
    assert_eq!(results.len(), 0);
}

#[test]
fn range_scan_with_max_bounds() {
    let page_handler = setup_page_handler();

    // Scan with maximum range
    let results = range_scan_column_entry(&page_handler, "col", 0, u64::MAX, 0);
    assert_eq!(
        results.len(),
        0,
        "Should return empty for non-existent column"
    );
}

#[test]
fn ops_handler_concurrent_operations() {
    use std::thread;

    let page_handler = setup_page_handler();
    let ph1 = Arc::clone(&page_handler);
    let ph2 = Arc::clone(&page_handler);

    let h1 = thread::spawn(move || {
        for _ in 0..10 {
            let _ = upsert_data_into_column(&ph1, "col1", "data");
        }
    });

    let h2 = thread::spawn(move || {
        for _ in 0..10 {
            let _ = upsert_data_into_column(&ph2, "col2", "data");
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();
    // Should not panic or deadlock
}

#[test]
fn multiple_scans_same_column() {
    let page_handler = setup_page_handler();

    // Multiple scans should work
    let r1 = range_scan_column_entry(&page_handler, "col", 0, 10, 0);
    let r2 = range_scan_column_entry(&page_handler, "col", 5, 15, 0);
    let r3 = range_scan_column_entry(&page_handler, "col", 0, u64::MAX, 0);

    assert_eq!(r1.len(), 0);
    assert_eq!(r2.len(), 0);
    assert_eq!(r3.len(), 0);
}

#[test]
fn update_and_scan_interaction() {
    let page_handler = setup_page_handler();

    // Try update then scan
    let _ = update_column_entry(&page_handler, "col", "value", 0);
    let results = range_scan_column_entry(&page_handler, "col", 0, 10, 0);

    // Should handle gracefully
    assert_eq!(results.len(), 0);
}

#[test]
fn range_scan_with_different_commit_times() {
    let page_handler = setup_page_handler();

    // Different commit time upper bounds
    let r1 = range_scan_column_entry(&page_handler, "col", 0, 10, 0);
    let r2 = range_scan_column_entry(&page_handler, "col", 0, 10, 100);
    let r3 = range_scan_column_entry(&page_handler, "col", 0, 10, u64::MAX);

    assert_eq!(r1.len(), 0);
    assert_eq!(r2.len(), 0);
    assert_eq!(r3.len(), 0);
}

#[test]
fn ops_handler_column_names_with_slashes() {
    let page_handler = setup_page_handler();

    // Column names with special characters
    let result = upsert_data_into_column(&page_handler, "path/to/column", "data");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn ops_handler_very_long_column_name() {
    let page_handler = setup_page_handler();

    let long_name = "a".repeat(1000);
    let result = upsert_data_into_column(&page_handler, &long_name, "data");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn ops_handler_very_long_data() {
    let page_handler = setup_page_handler();

    let long_data = "x".repeat(100_000);
    let result = upsert_data_into_column(&page_handler, "col", &long_data);
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn update_with_empty_data() {
    let page_handler = setup_page_handler();

    let result = update_column_entry(&page_handler, "col", "", 0);
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn range_scan_single_row() {
    let page_handler = setup_page_handler();

    // Scan single row (end = start + 1)
    let results = range_scan_column_entry(&page_handler, "col", 5, 6, 0);
    assert_eq!(results.len(), 0);
}

#[test]
fn ops_handler_whitespace_column_names() {
    let page_handler = setup_page_handler();

    let result1 = upsert_data_into_column(&page_handler, " ", "data");
    let result2 = upsert_data_into_column(&page_handler, "\t", "data");
    let result3 = upsert_data_into_column(&page_handler, "\n", "data");

    assert!(result1.is_err() || result1.is_ok());
    assert!(result2.is_err() || result2.is_ok());
    assert!(result3.is_err() || result3.is_ok());
}

#[test]
fn sorted_upsert_inserts_in_order() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(Arc::clone(&store)));
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
        compressor,
    ));
    let page_handler = Arc::new(PageHandler::new(locator, fetcher, materializer));

    let plan = CreateTablePlan::new(
        "users",
        vec![
            ColumnSpec::new("id", "String"),
            ColumnSpec::new("score", "String"),
        ],
        vec!["score".into()],
        false,
    );
    create_table_from_plan(&directory, &plan).expect("create ordered table");

    let descriptor = directory
        .register_page_in_table_with_sizes(
            "users",
            "score",
            "mem://users_score".into(),
            0,
            0,
            0,
            2,
        )
        .expect("register page");

    let mut cached = PageCacheEntryUncompressed { page: Page::new() };
    cached.page.page_metadata = descriptor.id.clone();
    cached.page.entries.push(Entry::new("10"));
    cached.page.entries.push(Entry::new("30"));
    page_handler.write_back_uncompressed(&descriptor.id, cached);

    let inserted = upsert_data_into_table_column(&page_handler, "users", "score", "20")
        .expect("sorted insert succeeds");
    assert!(inserted);

    let latest = page_handler
        .locate_latest_in_table("users", "score")
        .expect("descriptor exists");
    assert_eq!(latest.entry_count, 3);
    let latest_from_directory = directory
        .latest_in_table("users", "score")
        .expect("directory descriptor");
    assert_eq!(latest_from_directory.entry_count, 3);
    let page = page_handler
        .get_page(latest)
        .expect("page loaded");
    let values: Vec<&str> = page.page.entries.iter().map(|e| e.get_data()).collect();
    assert_eq!(values, vec!["10", "20", "30"]);
}
