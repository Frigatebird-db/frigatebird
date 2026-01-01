use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{
    ColumnDefinition, PageDirectory, TableDefinition, TableMetaStore,
};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SelectResult, SqlExecutor, SqlExecutorWalOptions};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use tempfile::TempDir;

static SQL_WAL_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static SQL_WAL_COUNTER: AtomicUsize = AtomicUsize::new(0);

trait SelectResultExt {
    fn rows(&self) -> Vec<Vec<Option<String>>>;
}

impl SelectResultExt for SelectResult {
    fn rows(&self) -> Vec<Vec<Option<String>>> {
        self.row_iter().map(|row| row.to_vec()).collect()
    }
}

struct SqlWalGuard {
    namespace: String,
    storage_dir: String,
    prev_env: Option<String>,
    _wal_dir: TempDir,
    _storage_dir: TempDir,
    _lock: MutexGuard<'static, ()>,
}

impl SqlWalGuard {
    fn new() -> Self {
        let lock = SQL_WAL_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let wal_dir = TempDir::new().expect("create wal temp dir");
        let storage_dir = TempDir::new().expect("create storage temp dir");
        let prev = env::var("WALRUS_DATA_DIR").ok();
        unsafe {
            env::set_var("WALRUS_DATA_DIR", wal_dir.path());
        }
        let id = SQL_WAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let namespace = format!("sql-crash-test-{id}");
        let storage_path = storage_dir.path().to_string_lossy().to_string();
        SqlWalGuard {
            namespace,
            storage_dir: storage_path,
            prev_env: prev,
            _wal_dir: wal_dir,
            _storage_dir: storage_dir,
            _lock: lock,
        }
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn storage_dir(&self) -> &str {
        &self.storage_dir
    }
}

impl Drop for SqlWalGuard {
    fn drop(&mut self) {
        if let Some(prev) = &self.prev_env {
            unsafe {
                env::set_var("WALRUS_DATA_DIR", prev);
            }
        } else {
            unsafe {
                env::remove_var("WALRUS_DATA_DIR");
            }
        }
    }
}

fn build_page_components() -> (Arc<PageHandler>, Arc<PageDirectory>) {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(Arc::clone(&store)));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        Arc::new(Compressor::new()),
    ));
    let handler = Arc::new(PageHandler::new(locator, fetcher, materializer));
    (handler, directory)
}

fn new_executor(
    namespace: &str,
    storage_dir: &str,
    reset_namespace: bool,
    cleanup_on_drop: bool,
) -> (SqlExecutor, Arc<PageHandler>, Arc<PageDirectory>) {
    let (handler, directory) = build_page_components();
    let options = SqlExecutorWalOptions::new(namespace.to_string())
        .reset_namespace(reset_namespace)
        .cleanup_on_drop(cleanup_on_drop)
        .storage_dir(storage_dir);
    let executor =
        SqlExecutor::with_wal_options(Arc::clone(&handler), Arc::clone(&directory), true, options);
    (executor, handler, directory)
}

fn reapply_table_schema(
    directory: &Arc<PageDirectory>,
    table: &str,
    columns: &[(&str, &str)],
    sort_key: &[&str],
) {
    let defs: Vec<ColumnDefinition> = columns
        .iter()
        .map(|(name, ty)| ColumnDefinition::new(*name, *ty))
        .collect();
    let order: Vec<String> = sort_key.iter().map(|s| s.to_string()).collect();
    let definition = TableDefinition::new(table.to_string(), defs, order);
    let _ = directory.force_register_table(definition);
}

#[test]
fn sql_crash_simple_select_persists_rows() {
    let guard = SqlWalGuard::new();
    {
        let (executor, _, _) = new_executor(guard.namespace(), guard.storage_dir(), true, false);
        executor
            .execute("CREATE TABLE logs (id TEXT, message TEXT) ORDER BY id")
            .expect("create table");
        executor
            .execute("INSERT INTO logs (id, message) VALUES ('1', 'boot')")
            .expect("insert row 1");
        executor
            .execute("INSERT INTO logs (id, message) VALUES ('2', 'ready')")
            .expect("insert row 2");
        executor.flush_table("logs").expect("flush logs");
    } // drop without cleanup to simulate crash

    {
        let (executor, _, _directory) =
            new_executor(guard.namespace(), guard.storage_dir(), false, true);
        // Schema is now persisted to WAL, no need to reapply manually
        let result = executor
            .query("SELECT message FROM logs ORDER BY id")
            .expect("select after crash");
        let rows: Vec<_> = result
            .rows()
            .into_iter()
            .map(|row| row[0].clone().unwrap())
            .collect();
        assert_eq!(rows, vec!["boot".to_string(), "ready".to_string()]);
    }
}

#[test]
fn sql_crash_multi_table_state_is_preserved() {
    let guard = SqlWalGuard::new();
    {
        let (executor, _, _) = new_executor(guard.namespace(), guard.storage_dir(), true, false);
        executor
            .execute("CREATE TABLE accounts (id TEXT, balance TEXT) ORDER BY id")
            .expect("create accounts");
        executor
            .execute("CREATE TABLE audit (id TEXT, entry TEXT) ORDER BY id")
            .expect("create audit");
        executor
            .execute("INSERT INTO accounts (id, balance) VALUES ('a', '10')")
            .expect("insert account a");
        executor
            .execute("INSERT INTO accounts (id, balance) VALUES ('b', '25')")
            .expect("insert account b");
        executor
            .execute("UPDATE accounts SET balance = '15' WHERE id = 'a'")
            .expect("update a");
        executor
            .execute("INSERT INTO audit (id, entry) VALUES ('1', 'adjust a'), ('2', 'freeze b')")
            .expect("insert audit");
        executor
            .execute("DELETE FROM accounts WHERE id = 'b'")
            .expect("delete b");
        executor.flush_table("accounts").expect("flush accounts");
        executor.flush_table("audit").expect("flush audit");
    }

    {
        let (executor, _, _directory) =
            new_executor(guard.namespace(), guard.storage_dir(), false, true);
        // Schema is now persisted to WAL, no need to reapply manually
        let accounts = executor
            .query("SELECT id, balance FROM accounts ORDER BY id")
            .expect("select accounts");
        let rows: Vec<_> = accounts
            .rows()
            .into_iter()
            .map(|row| (row[0].clone().unwrap(), row[1].clone().unwrap()))
            .collect();
        assert_eq!(rows, vec![("a".into(), "15".into())]);

        let audit = executor
            .query("SELECT COUNT(*) FROM audit")
            .expect("select audit");
        assert_eq!(audit.rows()[0][0].as_deref(), Some("2"));
    }
}

#[test]
fn sql_crash_multiple_rounds_accumulate_rows() {
    let guard = SqlWalGuard::new();
    {
        let (executor, _, _) = new_executor(guard.namespace(), guard.storage_dir(), true, false);
        executor
            .execute("CREATE TABLE kv (k TEXT, v TEXT) ORDER BY k")
            .expect("create kv");
        executor
            .execute("INSERT INTO kv (k, v) VALUES ('k1', 'v1'), ('k2', 'v2')")
            .expect("insert first batch");
        executor.flush_table("kv").expect("flush first batch");
    }

    {
        let (executor, _, _directory) =
            new_executor(guard.namespace(), guard.storage_dir(), false, false);
        // Schema is now persisted to WAL, no need to reapply manually
        executor
            .execute("INSERT INTO kv (k, v) VALUES ('k3', 'v3')")
            .expect("insert second batch");
        executor
            .execute("UPDATE kv SET v = 'v2b' WHERE k = 'k2'")
            .expect("update second record");
        executor.flush_table("kv").expect("flush second batch");
        let check = executor
            .query("SELECT k, v FROM kv ORDER BY k")
            .expect("check kv before crash");
        eprintln!("kv before crash: {:?}", check.rows());
    }

    {
        let (executor, _, _directory) =
            new_executor(guard.namespace(), guard.storage_dir(), false, true);
        // Schema is now persisted to WAL, no need to reapply manually
        let result = executor
            .query("SELECT k, v FROM kv ORDER BY k")
            .expect("select kv");
        let rows: Vec<_> = result
            .rows()
            .into_iter()
            .map(|row| (row[0].clone().unwrap(), row[1].clone().unwrap()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("k1".into(), "v1".into()),
                ("k2".into(), "v2b".into()),
                ("k3".into(), "v3".into()),
            ]
        );
    }
}
