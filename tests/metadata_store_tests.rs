use idk_uwu_ig::metadata_store::{
    ColumnDefinition, ColumnStats, ColumnStatsKind, DEFAULT_TABLE, MetaJournal, MetaJournalEntry,
    MetaRecord, PageDirectory, PendingPage, ROWS_PER_PAGE_GROUP, TableDefinition, TableMetaStore,
};
use idk_uwu_ig::wal::{FsyncSchedule, ReadConsistency, Walrus};
use idk_uwu_ig::writer::executor::{DirectoryMetadataClient, MetadataClient, MetadataUpdate};
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use tempfile::TempDir;

static META_WAL_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static META_WAL_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct MetadataWalGuard {
    wal: Arc<Walrus>,
    _dir: TempDir,
    _lock: MutexGuard<'static, ()>,
}

impl MetadataWalGuard {
    fn new() -> Self {
        let lock = META_WAL_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let dir = TempDir::new().expect("create wal temp dir");
        let prev = env::var("WALRUS_DATA_DIR").ok();
        unsafe {
            env::set_var("WALRUS_DATA_DIR", dir.path());
        }
        let id = META_WAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let wal = Arc::new(
            Walrus::with_consistency_and_schedule_for_key(
                &format!("metadata-store-test-{id}"),
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("wal init failed for metadata tests"),
        );
        if let Some(value) = prev {
            unsafe {
                env::set_var("WALRUS_DATA_DIR", value);
            }
        } else {
            unsafe {
                env::remove_var("WALRUS_DATA_DIR");
            }
        }
        MetadataWalGuard {
            wal,
            _dir: dir,
            _lock: lock,
        }
    }

    fn wal(&self) -> Arc<Walrus> {
        Arc::clone(&self.wal)
    }
}

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
    let desc2 = directory
        .register_page("col1", "test.db".to_string(), 1024)
        .unwrap();

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
    assert!(!results.is_empty());
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
    assert!(!results.is_empty());
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
    assert!(!results.is_empty());
}

#[test]
fn register_batch_replaces_tail_and_updates_prefix() {
    let mut store = TableMetaStore::new();

    let first = vec![PendingPage {
        table: DEFAULT_TABLE.to_string(),
        column: "users".to_string(),
        descriptor_id: None,
        disk_path: "file0".to_string(),
        offset: 0,
        alloc_len: 256 * 1024,
        actual_len: 200 * 8,
        entry_count: 8,
        replace_last: false,
        stats: None,
    }];
    store.register_batch(&first);

    let second = vec![PendingPage {
        table: DEFAULT_TABLE.to_string(),
        column: "users".to_string(),
        descriptor_id: None,
        disk_path: "file0".to_string(),
        offset: 256 * 1024,
        alloc_len: 256 * 1024,
        actual_len: 200 * 4,
        entry_count: 4,
        replace_last: false,
        stats: None,
    }];
    store.register_batch(&second);

    let third = vec![PendingPage {
        table: DEFAULT_TABLE.to_string(),
        column: "users".to_string(),
        descriptor_id: None,
        disk_path: "file0".to_string(),
        offset: 512 * 1024,
        alloc_len: 64 * 1024,
        actual_len: 200 * 16,
        entry_count: 16,
        replace_last: true,
        stats: None,
    }];
    store.register_batch(&third);

    let slices = store.locate_range(DEFAULT_TABLE, "users", 0, ROWS_PER_PAGE_GROUP + 15);
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].descriptor.entry_count, 8);
    assert_eq!(slices[1].descriptor.entry_count, 16);

    let location = store
        .locate_row(DEFAULT_TABLE, "users", ROWS_PER_PAGE_GROUP + 4)
        .unwrap();
    assert_eq!(location.descriptor.entry_count, 16);
}

#[test]
fn update_latest_entry_count_adjusts_metadata() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(Arc::clone(&store)));

    let definition = TableDefinition::new(
        "metric",
        vec![ColumnDefinition::new("value", "String")],
        Vec::new(),
    );
    directory
        .register_table(definition)
        .expect("register table");

    let descriptor = directory
        .register_page_in_table_with_sizes(
            "metric",
            "value",
            "mem://metric_value".into(),
            0,
            0,
            0,
            2,
        )
        .expect("register page");
    assert_eq!(descriptor.entry_count, 2);

    directory
        .update_latest_entry_count("metric", "value", 4)
        .expect("update entry count");

    let latest = directory
        .latest_in_table("metric", "value")
        .expect("latest descriptor");
    assert_eq!(latest.entry_count, 4);
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
        let desc = directory
            .register_page(&format!("col{}", i), "test.db".to_string(), i * 1024)
            .unwrap();
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
fn register_table_records_columns_and_ordering() {
    let mut store = TableMetaStore::new();
    let definition = TableDefinition::new(
        "items",
        vec![
            ColumnDefinition::new("id", "UUID"),
            ColumnDefinition::new("name", "String"),
            ColumnDefinition::new("created_at", "DateTime"),
        ],
        vec!["id".into(), "created_at".into()],
    );
    store.register_table(definition).expect("register table");

    let catalog = store.table("items").expect("catalog exists");
    assert_eq!(catalog.columns().len(), 3);
    assert!(catalog.column("name").is_some());
    let key: Vec<String> = catalog
        .sort_key()
        .iter()
        .map(|col| col.name.clone())
        .collect();
    assert_eq!(key, vec!["id".to_string(), "created_at".to_string()]);
}

#[test]
fn add_columns_extends_existing_table() {
    let mut store = TableMetaStore::new();
    let definition = TableDefinition::new(
        "metrics",
        vec![ColumnDefinition::new("ts", "TIMESTAMP")],
        vec![],
    );
    store
        .register_table(definition)
        .expect("register base table");

    store
        .add_columns(
            "metrics",
            vec![
                ColumnDefinition::new("value", "Float64"),
                ColumnDefinition::new("tags", "String"),
            ],
        )
        .expect("extend table");

    assert!(store.column_defined("metrics", "ts"));
    assert!(store.column_defined("metrics", "value"));
    assert!(store.column_defined("metrics", "tags"));
}

#[test]
fn default_table_column_created_on_page_registration() {
    let mut store = TableMetaStore::new();
    let descriptor = store.register_page(
        DEFAULT_TABLE,
        "ephemeral_column",
        "ephemeral.db".to_string(),
        0,
        0,
        0,
        5,
    );
    assert!(descriptor.is_some());
    assert!(store.column_defined(DEFAULT_TABLE, "ephemeral_column"));
}

#[test]
fn page_directory_large_range() {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    for i in 0..100 {
        directory.register_page("col1", "test.db".to_string(), i * 1024);
    }

    let results = directory.range("col1", 0, 1000, u64::MAX);
    assert!(!results.is_empty());
}

#[test]
fn meta_journal_replay_publishes_pages() {
    let wal_guard = MetadataWalGuard::new();
    let wal = wal_guard.wal();
    let journal = MetaJournal::new(Arc::clone(&wal), 4);
    let directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));

    let record = MetaRecord::PublishPages {
        table: "logs".to_string(),
        entries: vec![MetaJournalEntry {
            column: "message".to_string(),
            descriptor_id: "0000000000000001".to_string(),
            disk_path: "fileA".to_string(),
            offset: 0,
            alloc_len: 4096,
            actual_len: 2048,
            entry_count: 5,
            replace_last: false,
            stats: None,
        }],
    };

    journal
        .append_commit("logs", &record)
        .expect("journal append");
    journal.replay_into(&directory).expect("journal replay");

    let descriptor = directory
        .latest_in_table("logs", "message")
        .expect("descriptor after replay");
    assert_eq!(descriptor.id, "0000000000000001");
    assert_eq!(descriptor.disk_path, "fileA");
    assert_eq!(descriptor.entry_count, 5);
}

#[test]
fn meta_journal_replay_handles_replacements_and_stats() {
    let wal_guard = MetadataWalGuard::new();
    let wal = wal_guard.wal();
    let journal = MetaJournal::new(Arc::clone(&wal), 4);
    let directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));

    let stats = Some(ColumnStats {
        min_value: Some("a".into()),
        max_value: Some("z".into()),
        null_count: 0,
        kind: ColumnStatsKind::Text,
    });

    let first = MetaRecord::PublishPages {
        table: "alpha".to_string(),
        entries: vec![
            MetaJournalEntry {
                column: "c1".to_string(),
                descriptor_id: "0000000000000001".to_string(),
                disk_path: "seg0".to_string(),
                offset: 0,
                alloc_len: 1024,
                actual_len: 512,
                entry_count: 4,
                replace_last: false,
                stats: stats.clone(),
            },
            MetaJournalEntry {
                column: "c2".to_string(),
                descriptor_id: "0000000000000002".to_string(),
                disk_path: "seg0".to_string(),
                offset: 1024,
                alloc_len: 1024,
                actual_len: 768,
                entry_count: 5,
                replace_last: false,
                stats: None,
            },
        ],
    };
    let beta = MetaRecord::PublishPages {
        table: "beta".to_string(),
        entries: vec![MetaJournalEntry {
            column: "payload".to_string(),
            descriptor_id: "0000000000000003".to_string(),
            disk_path: "seg1".to_string(),
            offset: 0,
            alloc_len: 2048,
            actual_len: 1024,
            entry_count: 7,
            replace_last: false,
            stats: None,
        }],
    };
    let replacement = MetaRecord::PublishPages {
        table: "alpha".to_string(),
        entries: vec![MetaJournalEntry {
            column: "c1".to_string(),
            descriptor_id: "0000000000000004".to_string(),
            disk_path: "seg2".to_string(),
            offset: 0,
            alloc_len: 512,
            actual_len: 256,
            entry_count: 9,
            replace_last: true,
            stats: None,
        }],
    };

    journal
        .append_commit("alpha", &first)
        .expect("append alpha");
    journal.append_commit("beta", &beta).expect("append beta");
    journal
        .append_commit("alpha", &replacement)
        .expect("append replacement");

    journal.replay_into(&directory).expect("journal replay");

    let alpha_c1 = directory.latest_in_table("alpha", "c1").expect("alpha.c1");
    assert_eq!(alpha_c1.id, "0000000000000004");
    assert_eq!(alpha_c1.entry_count, 9);
    assert!(alpha_c1.stats.is_none());

    let alpha_c2 = directory.latest_in_table("alpha", "c2").expect("alpha.c2");
    assert_eq!(alpha_c2.id, "0000000000000002");
    assert_eq!(alpha_c2.entry_count, 5);
    assert_eq!(alpha_c2.stats, None);

    let beta_payload = directory
        .latest_in_table("beta", "payload")
        .expect("beta payload");
    assert_eq!(beta_payload.id, "0000000000000003");
    assert_eq!(beta_payload.entry_count, 7);
}

#[test]
fn directory_metadata_client_commit_round_trips_via_journal() {
    let wal_guard = MetadataWalGuard::new();
    let wal = wal_guard.wal();
    let journal = Arc::new(MetaJournal::new(Arc::clone(&wal), 4));
    let directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));
    journal.replay_into(&directory).expect("initial replay");
    let metadata: Arc<dyn MetadataClient> = Arc::new(DirectoryMetadataClient::new(
        Arc::clone(&directory),
        Arc::clone(&journal),
    ));

    let updates = vec![MetadataUpdate {
        table: "metrics".to_string(),
        column: "value".to_string(),
        descriptor_id: None,
        disk_path: "fileB".to_string(),
        offset: 0,
        alloc_len: 2048,
        actual_len: 1024,
        entry_count: 10,
        replace_last: false,
        stats: None,
    }];

    let descriptors = metadata.commit("metrics", updates);
    assert_eq!(descriptors.len(), 1);
    let committed_id = descriptors[0].id.clone();

    let replay_directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));
    journal
        .replay_into(&replay_directory)
        .expect("replay into fresh directory");
    let replayed = replay_directory
        .latest_in_table("metrics", "value")
        .expect("replayed descriptor");
    assert_eq!(replayed.id, committed_id);
    assert_eq!(replayed.entry_count, 10);
}

#[test]
fn metadata_commit_crash_recovery_preserves_latest_state() {
    let wal_guard = MetadataWalGuard::new();
    let wal = wal_guard.wal();
    let journal = Arc::new(MetaJournal::new(Arc::clone(&wal), 4));
    let directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));
    journal.replay_into(&directory).expect("initial replay");
    let metadata: Arc<dyn MetadataClient> = Arc::new(DirectoryMetadataClient::new(
        Arc::clone(&directory),
        Arc::clone(&journal),
    ));

    let first_commit = vec![
        MetadataUpdate {
            table: "orders".to_string(),
            column: "id".to_string(),
            descriptor_id: None,
            disk_path: "segA".to_string(),
            offset: 0,
            alloc_len: 4096,
            actual_len: 1024,
            entry_count: 3,
            replace_last: false,
            stats: None,
        },
        MetadataUpdate {
            table: "orders".to_string(),
            column: "amount".to_string(),
            descriptor_id: None,
            disk_path: "segA".to_string(),
            offset: 4096,
            alloc_len: 4096,
            actual_len: 1024,
            entry_count: 3,
            replace_last: false,
            stats: None,
        },
    ];
    metadata.commit("orders", first_commit);

    let second_commit = vec![MetadataUpdate {
        table: "orders".to_string(),
        column: "amount".to_string(),
        descriptor_id: None,
        disk_path: "segB".to_string(),
        offset: 0,
        alloc_len: 4096,
        actual_len: 1024,
        entry_count: 6,
        replace_last: true,
        stats: None,
    }];
    metadata.commit("orders", second_commit);

    let third_commit = vec![MetadataUpdate {
        table: "customers".to_string(),
        column: "name".to_string(),
        descriptor_id: None,
        disk_path: "segC".to_string(),
        offset: 0,
        alloc_len: 2048,
        actual_len: 512,
        entry_count: 2,
        replace_last: false,
        stats: None,
    }];
    metadata.commit("customers", third_commit);

    // simulate crash by creating a brand new directory and replaying the journal
    let recovered_directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));
    journal
        .replay_into(&recovered_directory)
        .expect("replay after crash");

    let amount_descriptor = recovered_directory
        .latest_in_table("orders", "amount")
        .expect("orders.amount");
    assert_eq!(amount_descriptor.entry_count, 6);
    assert!(amount_descriptor.disk_path.contains("segB"));

    let id_descriptor = recovered_directory
        .latest_in_table("orders", "id")
        .expect("orders.id");
    assert_eq!(id_descriptor.entry_count, 3);

    let customer_descriptor = recovered_directory
        .latest_in_table("customers", "name")
        .expect("customers.name");
    assert_eq!(customer_descriptor.entry_count, 2);
}

#[test]
fn meta_journal_randomized_sequences_survive_replay() {
    let wal_guard = MetadataWalGuard::new();
    let wal = wal_guard.wal();
    let journal = MetaJournal::new(Arc::clone(&wal), 4);
    let mut expected: HashMap<(String, String), MetaJournalEntry> = HashMap::new();
    let mut seed: u64 = 0xfeedface_cafebabe;

    fn next_rand(seed: &mut u64) -> u64 {
        const A: u64 = 6364136223846793005;
        const C: u64 = 1;
        *seed = seed.wrapping_mul(A).wrapping_add(C);
        *seed
    }

    for step in 0..64u64 {
        let table = format!("table_{}", next_rand(&mut seed) % 3);
        let column = format!("column_{}", next_rand(&mut seed) % 5);
        let descriptor_id = format!("{:016x}", step + 1);
        let entry_count = next_rand(&mut seed) % 50 + 1;
        let actual_len = next_rand(&mut seed) % 3072 + 256;
        let offset = next_rand(&mut seed) % 1_000;
        let replace_last = (next_rand(&mut seed) & 1) == 0;
        let stats = if (next_rand(&mut seed) & 1) == 0 {
            Some(ColumnStats {
                min_value: Some(format!("min_{step}")),
                max_value: Some(format!("max_{step}")),
                null_count: (next_rand(&mut seed) % 3),
                kind: ColumnStatsKind::Text,
            })
        } else {
            None
        };

        let entry = MetaJournalEntry {
            column: column.clone(),
            descriptor_id: descriptor_id.clone(),
            disk_path: format!("seg_{}_{}", table, column),
            offset,
            alloc_len: 4096,
            actual_len,
            entry_count,
            replace_last,
            stats: stats.clone(),
        };

        let record = MetaRecord::PublishPages {
            table: table.clone(),
            entries: vec![entry.clone()],
        };

        journal
            .append_commit(&table, &record)
            .expect("journal append");
        expected.insert((table, column), entry);
    }

    let replay_directory = Arc::new(PageDirectory::new(Arc::new(RwLock::new(
        TableMetaStore::new(),
    ))));
    journal
        .replay_into(&replay_directory)
        .expect("replay randomized");

    for ((table, column), entry) in expected {
        let descriptor = replay_directory
            .latest_in_table(&table, &column)
            .unwrap_or_else(|| panic!("missing descriptor for {}.{}", table, column));
        assert_eq!(descriptor.id, entry.descriptor_id);
        assert_eq!(descriptor.entry_count, entry.entry_count);
        assert_eq!(descriptor.disk_path, entry.disk_path);
        assert_eq!(descriptor.offset, entry.offset);
        assert_eq!(descriptor.alloc_len, entry.alloc_len);
        assert_eq!(descriptor.actual_len, entry.actual_len);
        match (descriptor.stats.as_ref(), entry.stats.as_ref()) {
            (None, None) => {}
            (Some(lhs), Some(rhs)) => {
                assert_eq!(lhs.min_value, rhs.min_value);
                assert_eq!(lhs.max_value, rhs.max_value);
                assert_eq!(lhs.null_count, rhs.null_count);
                assert_eq!(lhs.kind, rhs.kind);
            }
            _ => panic!("stat mismatch for {}.{}", table, column),
        }
    }
}
