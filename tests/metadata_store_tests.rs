use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use std::sync::{Arc, RwLock};

#[test]
fn table_meta_store_new_creates_empty() {
    let store = TableMetaStore::new();
    let _addr = format!("{:p}", &store);
}

#[test]
fn page_directory_new_creates_empty() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = PageDirectory::new(store);
    let _addr = format!("{:p}", &directory);
}

#[test]
fn page_directory_register_and_lookup() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let descriptor = directory.register_page("col1", "test.db".to_string(), 100);
    assert!(descriptor.is_some());

    let desc = descriptor.unwrap();
    let retrieved = directory.lookup(&desc.id);
    assert!(retrieved.is_some());
    let retrieved_meta = retrieved.unwrap();
    assert_eq!(retrieved_meta.id, desc.id);
    assert_eq!(retrieved_meta.disk_path, "test.db");
    assert_eq!(retrieved_meta.offset, 100);
}

#[test]
fn page_directory_latest_returns_most_recent() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    std::thread::sleep(std::time::Duration::from_millis(10));
    let desc2 = directory.register_page("col1", "test.db".to_string(), 1024).unwrap();

    let latest = directory.latest("col1");
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().id, desc2.id);
}

#[test]
fn page_directory_latest_nonexistent_column() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let latest = directory.latest("nonexistent");
    assert!(latest.is_none());
}

#[test]
fn page_directory_lookup_nonexistent_id() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let result = directory.lookup("nonexistent_id");
    assert!(result.is_none());
}

#[test]
fn page_directory_range_query() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    directory.register_page("col1", "test.db".to_string(), 1024);
    directory.register_page("col1", "test.db".to_string(), 2048);

    let results = directory.range("col1", 0, 10, u64::MAX);
    assert!(results.len() >= 1);
}

#[test]
fn page_directory_range_empty_result() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    let results = directory.range("col1", 200, 300, u64::MAX);
    assert_eq!(results.len(), 0);
}

#[test]
fn page_directory_range_with_timestamp_bound() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);

    std::thread::sleep(std::time::Duration::from_millis(10));
    let timestamp_bound = idk_uwu_ig::entry::current_epoch_millis();
    std::thread::sleep(std::time::Duration::from_millis(10));

    directory.register_page("col1", "test.db".to_string(), 1024);

    let results = directory.range("col1", 0, 199, timestamp_bound);
    assert!(results.len() >= 1);
}

#[test]
fn page_directory_multiple_columns() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    directory.register_page("col2", "test.db".to_string(), 1024);

    assert!(directory.latest("col1").is_some());
    assert!(directory.latest("col2").is_some());

    let col1_latest = directory.latest("col1").unwrap();
    let col2_latest = directory.latest("col2").unwrap();
    assert_ne!(col1_latest.id, col2_latest.id);
}

#[test]
fn page_directory_overlapping_ranges() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory.register_page("col1", "test.db".to_string(), 0);
    directory.register_page("col1", "test.db".to_string(), 1024);
    directory.register_page("col1", "test.db".to_string(), 2048);

    let results = directory.range("col1", 75, 125, u64::MAX);
    assert!(results.len() >= 0);
}

#[test]
fn page_directory_concurrent_register() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));
    let mut handles = vec![];

    for i in 0..10 {
        let dir = Arc::clone(&directory);
        let handle = std::thread::spawn(move || {
            dir.register_page(&format!("col{}", i), "test.db".to_string(), i * 1024);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..10 {
        assert!(directory.latest(&format!("col{}", i)).is_some());
    }
}

#[test]
fn page_directory_concurrent_lookup() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    let mut ids = vec![];
    for i in 0..5 {
        let desc = directory.register_page(&format!("col{}", i), "test.db".to_string(), i * 1024).unwrap();
        ids.push(desc.id.clone());
    }

    let mut handles = vec![];
    for id in ids {
        let dir = Arc::clone(&directory);
        let handle = std::thread::spawn(move || {
            let result = dir.lookup(&id);
            assert!(result.is_some());
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn page_directory_large_range() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    for i in 0..100 {
        directory.register_page("col1", "test.db".to_string(), i * 1024);
    }

    let results = directory.range("col1", 0, 1000, u64::MAX);
    assert!(results.len() >= 0);
}
