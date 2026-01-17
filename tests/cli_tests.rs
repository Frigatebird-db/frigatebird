use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Mutex};
use once_cell::sync::Lazy;

use frigatebird::cache::page_cache::PageCache;
use frigatebird::helpers::compressor::Compressor;
use frigatebird::metadata_store::{PageDirectory, TableMetaStore};
use frigatebird::page_handler::page_io::PageIO;
use frigatebird::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use frigatebird::sql::executor::{SqlExecutor, SqlExecutorWalOptions};
use rand::Rng;

fn setup_executor() -> SqlExecutor {
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
    SqlExecutor::new(handler, directory)
}

#[test]
fn cli_executor_initializes() {
    let _executor = setup_executor();
}

#[test]
fn cli_create_table() {
    let executor = setup_executor();
    let result = executor.execute("CREATE TABLE users (id TEXT, name TEXT, age INT) ORDER BY id");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());
}

#[test]
fn cli_create_table_and_insert() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE products (id TEXT, name TEXT, price FLOAT) ORDER BY id")
        .expect("CREATE TABLE failed");

    let result =
        executor.execute("INSERT INTO products (id, name, price) VALUES ('1', 'Widget', '9.99')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result.err());
}

#[test]
fn cli_create_insert_select() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE items (id TEXT, value INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO items (id, value) VALUES ('1', '100')")
        .expect("INSERT failed");

    executor
        .execute("INSERT INTO items (id, value) VALUES ('1', '200')")
        .expect("INSERT failed");

    let result = executor
        .query("SELECT value FROM items WHERE id = '1'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 2);
}

#[test]
fn cli_select_with_aggregation() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE numbers (id TEXT, n INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    for i in 1..=5 {
        executor
            .execute(&format!(
                "INSERT INTO numbers (id, n) VALUES ('1', '{}')",
                i * 10
            ))
            .expect("INSERT failed");
    }

    let result = executor
        .query("SELECT SUM(n), AVG(n), COUNT(*) FROM numbers WHERE id = '1'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 1);

    let row: Vec<_> = result.row_iter().next().unwrap().to_vec();
    assert_eq!(row[0], Some("150".to_string())); // SUM: 10+20+30+40+50
    assert_eq!(row[1], Some("30".to_string())); // AVG: 150/5
    assert_eq!(row[2], Some("5".to_string())); // COUNT
}

#[test]
fn cli_select_with_order_by() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE scores (id TEXT, player TEXT, score INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO scores (id, player, score) VALUES ('1', 'Alice', '100')")
        .unwrap();
    executor
        .execute("INSERT INTO scores (id, player, score) VALUES ('1', 'Bob', '250')")
        .unwrap();
    executor
        .execute("INSERT INTO scores (id, player, score) VALUES ('1', 'Charlie', '175')")
        .unwrap();

    let result = executor
        .query("SELECT player, score FROM scores WHERE id = '1' ORDER BY score DESC")
        .expect("SELECT failed");

    let rows: Vec<_> = result.row_iter().map(|r| r.to_vec()).collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Some("Bob".to_string()));
    assert_eq!(rows[1][0], Some("Charlie".to_string()));
    assert_eq!(rows[2][0], Some("Alice".to_string()));
}

#[test]
fn cli_select_with_limit() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE logs (id TEXT, msg TEXT) ORDER BY id")
        .expect("CREATE TABLE failed");

    for i in 1..=10 {
        executor
            .execute(&format!(
                "INSERT INTO logs (id, msg) VALUES ('1', 'message {}')",
                i
            ))
            .unwrap();
    }

    let result = executor
        .query("SELECT msg FROM logs WHERE id = '1' LIMIT 3")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 3);
}

#[test]
fn cli_select_display_format() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE display_test (id TEXT, name TEXT, value INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO display_test (id, name, value) VALUES ('1', 'test', '42')")
        .unwrap();

    let result = executor
        .query("SELECT name, value FROM display_test WHERE id = '1'")
        .expect("SELECT failed");

    let formatted = format!("{}", result);
    assert!(formatted.contains("name"));
    assert!(formatted.contains("value"));
    assert!(formatted.contains("test"));
    assert!(formatted.contains("42"));
    assert!(formatted.contains("(1 rows)"));
}

#[test]
fn cli_error_on_invalid_sql() {
    let executor = setup_executor();

    let result = executor.query("THIS IS NOT VALID SQL");
    assert!(result.is_err());
}

#[test]
fn cli_error_on_missing_table() {
    let executor = setup_executor();

    let result = executor.query("SELECT * FROM nonexistent WHERE id = '1'");
    assert!(result.is_err());
}

#[test]
fn cli_multiple_tables() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE table_a (id TEXT, val INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("CREATE TABLE table_b (id TEXT, val INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO table_a (id, val) VALUES ('1', '10')")
        .unwrap();
    executor
        .execute("INSERT INTO table_b (id, val) VALUES ('1', '20')")
        .unwrap();

    let result_a = executor
        .query("SELECT val FROM table_a WHERE id = '1'")
        .expect("SELECT from table_a failed");

    let result_b = executor
        .query("SELECT val FROM table_b WHERE id = '1'")
        .expect("SELECT from table_b failed");

    let val_a: Vec<_> = result_a.row_iter().map(|r| r.to_vec()).collect();
    let val_b: Vec<_> = result_b.row_iter().map(|r| r.to_vec()).collect();

    assert_eq!(val_a[0][0], Some("10".to_string()));
    assert_eq!(val_b[0][0], Some("20".to_string()));
}

#[test]
fn cli_null_handling() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE nullable (id TEXT, name TEXT, score INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO nullable (id, name, score) VALUES ('1', NULL, '100')")
        .unwrap();

    executor
        .execute("INSERT INTO nullable (id, name, score) VALUES ('1', 'present', NULL)")
        .unwrap();

    let result = executor
        .query("SELECT name, score FROM nullable WHERE id = '1'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 2);

    let formatted = format!("{}", result);
    assert!(formatted.contains("NULL"));
}

#[test]
fn cli_float_operations() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE floats (id TEXT, val FLOAT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO floats (id, val) VALUES ('1', '3.14159')")
        .unwrap();
    executor
        .execute("INSERT INTO floats (id, val) VALUES ('1', '2.71828')")
        .unwrap();

    let result = executor
        .query("SELECT val FROM floats WHERE id = '1' ORDER BY val")
        .expect("SELECT failed");

    let rows: Vec<_> = result.row_iter().map(|r| r.to_vec()).collect();
    assert_eq!(rows.len(), 2);

    let first_val: f64 = rows[0][0].as_ref().unwrap().parse().unwrap();
    let second_val: f64 = rows[1][0].as_ref().unwrap().parse().unwrap();
    assert!(first_val < second_val);
}

#[test]
fn cli_boolean_values() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE flags (id TEXT, active BOOL) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO flags (id, active) VALUES ('1', 'true')")
        .unwrap();
    executor
        .execute("INSERT INTO flags (id, active) VALUES ('1', 'false')")
        .unwrap();

    let result = executor
        .query("SELECT active FROM flags WHERE id = '1'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 2);
}

#[test]
fn cli_where_clause_filtering() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE filtered (id TEXT, category TEXT, amount INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO filtered (id, category, amount) VALUES ('1', 'A', '10')")
        .unwrap();
    executor
        .execute("INSERT INTO filtered (id, category, amount) VALUES ('1', 'B', '20')")
        .unwrap();
    executor
        .execute("INSERT INTO filtered (id, category, amount) VALUES ('1', 'A', '30')")
        .unwrap();
    executor
        .execute("INSERT INTO filtered (id, category, amount) VALUES ('1', 'B', '40')")
        .unwrap();

    let result = executor
        .query("SELECT amount FROM filtered WHERE id = '1' AND category = 'A'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 2);

    let amounts: Vec<i64> = result
        .row_iter()
        .map(|r| r.to_vec()[0].as_ref().unwrap().parse().unwrap())
        .collect();

    assert!(amounts.contains(&10));
    assert!(amounts.contains(&30));
}

#[test]
fn cli_group_by() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE sales (id TEXT, region TEXT, amount INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO sales (id, region, amount) VALUES ('1', 'North', '100')")
        .unwrap();
    executor
        .execute("INSERT INTO sales (id, region, amount) VALUES ('1', 'North', '150')")
        .unwrap();
    executor
        .execute("INSERT INTO sales (id, region, amount) VALUES ('1', 'South', '200')")
        .unwrap();
    executor
        .execute("INSERT INTO sales (id, region, amount) VALUES ('1', 'South', '250')")
        .unwrap();

    let result = executor
        .query(
            "SELECT region, SUM(amount) FROM sales WHERE id = '1' GROUP BY region ORDER BY region",
        )
        .expect("SELECT failed");

    let rows: Vec<_> = result.row_iter().map(|r| r.to_vec()).collect();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0][0], Some("North".to_string()));
    assert_eq!(rows[0][1], Some("250".to_string())); // 100 + 150

    assert_eq!(rows[1][0], Some("South".to_string()));
    assert_eq!(rows[1][1], Some("450".to_string())); // 200 + 250
}

#[test]
fn cli_empty_result() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE empty_test (id TEXT, val INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    let result = executor
        .query("SELECT val FROM empty_test WHERE id = 'nonexistent'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 0);
    assert!(result.is_empty());

    let formatted = format!("{}", result);
    assert!(formatted.contains("(0 rows)"));
}

#[test]
fn cli_update_rows() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE updatable (id TEXT, status TEXT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO updatable (id, status) VALUES ('1', 'pending')")
        .unwrap();
    executor
        .execute("INSERT INTO updatable (id, status) VALUES ('1', 'pending')")
        .unwrap();

    executor
        .execute("UPDATE updatable SET status = 'done' WHERE id = '1'")
        .unwrap();

    let result = executor
        .query("SELECT status FROM updatable WHERE id = '1'")
        .expect("SELECT failed");

    let statuses: Vec<_> = result.row_iter().map(|r| r.to_vec()[0].clone()).collect();

    assert!(statuses.iter().all(|s| s == &Some("done".to_string())));
}

#[test]
fn cli_delete_rows() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE deletable (id TEXT, val INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("INSERT INTO deletable (id, val) VALUES ('1', '10')")
        .unwrap();
    executor
        .execute("INSERT INTO deletable (id, val) VALUES ('1', '20')")
        .unwrap();
    executor
        .execute("INSERT INTO deletable (id, val) VALUES ('2', '30')")
        .unwrap();

    executor
        .execute("DELETE FROM deletable WHERE id = '1'")
        .unwrap();

    let result = executor
        .query("SELECT val FROM deletable WHERE id = '1'")
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 0);

    let result2 = executor
        .query("SELECT val FROM deletable WHERE id = '2'")
        .expect("SELECT failed");

    assert_eq!(result2.row_count(), 1);
}

#[test]
fn cli_table_names_empty() {
    let executor = setup_executor();
    let tables = executor.table_names();
    assert!(tables.is_empty());
}

#[test]
fn cli_table_names_after_create() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE alpha (id TEXT, val INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    executor
        .execute("CREATE TABLE beta (id TEXT, name TEXT) ORDER BY id")
        .expect("CREATE TABLE failed");

    let tables = executor.table_names();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"alpha".to_string()));
    assert!(tables.contains(&"beta".to_string()));
}

#[test]
fn cli_get_table_catalog() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE catalog_test (id TEXT, name TEXT, score INT) ORDER BY id")
        .expect("CREATE TABLE failed");

    let catalog = executor.get_table_catalog("catalog_test");
    assert!(catalog.is_some());

    let catalog = catalog.unwrap();
    let columns = catalog.columns();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[1].name, "name");
    assert_eq!(columns[2].name, "score");
}

#[test]
fn cli_get_table_catalog_missing() {
    let executor = setup_executor();
    let catalog = executor.get_table_catalog("nonexistent");
    assert!(catalog.is_none());
}

#[test]
fn cli_shorthand_dt_lists_tables() {
    let executor = setup_executor();
    assert!(executor.table_names().is_empty());

    executor
        .execute("CREATE TABLE orders (id TEXT, total INT) ORDER BY id")
        .unwrap();
    executor
        .execute("CREATE TABLE customers (id TEXT, name TEXT) ORDER BY id")
        .unwrap();
    executor
        .execute("CREATE TABLE products (id TEXT, price INT) ORDER BY id")
        .unwrap();

    let tables = executor.table_names();
    assert_eq!(tables.len(), 3);
    assert!(tables.contains(&"orders".to_string()));
    assert!(tables.contains(&"customers".to_string()));
    assert!(tables.contains(&"products".to_string()));
}

#[test]
fn cli_shorthand_d_describes_table() {
    let executor = setup_executor();
    executor
        .execute("CREATE TABLE employees (id TEXT, name TEXT, salary INT, active BOOL) ORDER BY id")
        .unwrap();

    let catalog = executor
        .get_table_catalog("employees")
        .expect("table should exist");
    let columns = catalog.columns();
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[1].name, "name");
    assert_eq!(columns[2].name, "salary");
    assert_eq!(columns[3].name, "active");
    use frigatebird::sql::DataType;
    assert_eq!(columns[0].data_type, DataType::String);
    assert_eq!(columns[1].data_type, DataType::String);
    assert_eq!(columns[2].data_type, DataType::Int64);
    assert_eq!(columns[3].data_type, DataType::Boolean);
}

#[test]
fn cli_shorthand_d_missing_table() {
    let executor = setup_executor();
    assert!(executor.get_table_catalog("does_not_exist").is_none());
}

#[test]
fn cli_execute_create_table() {
    let executor = setup_executor();
    let result = executor.execute("CREATE TABLE test_exec (id TEXT, val INT) ORDER BY id");
    assert!(result.is_ok());
    assert!(executor.table_names().contains(&"test_exec".to_string()));
}

#[test]
fn cli_execute_insert() {
    let executor = setup_executor();
    executor
        .execute("CREATE TABLE insert_test (id TEXT, val INT) ORDER BY id")
        .unwrap();

    let result = executor.execute("INSERT INTO insert_test (id, val) VALUES ('1', '42')");
    assert!(result.is_ok());

    let rows = executor
        .query("SELECT val FROM insert_test WHERE id = '1'")
        .unwrap();
    assert_eq!(rows.row_count(), 1);
}

#[test]
fn cli_execute_update() {
    let executor = setup_executor();
    executor
        .execute("CREATE TABLE update_test (id TEXT, status TEXT) ORDER BY id")
        .unwrap();
    executor
        .execute("INSERT INTO update_test (id, status) VALUES ('1', 'old')")
        .unwrap();

    let result = executor.execute("UPDATE update_test SET status = 'new' WHERE id = '1'");
    assert!(result.is_ok());

    let rows = executor
        .query("SELECT status FROM update_test WHERE id = '1'")
        .unwrap();
    let status = rows.row_iter().next().unwrap().value_as_string(0);
    assert_eq!(status, Some("new".to_string()));
}

#[test]
fn cli_execute_delete() {
    let executor = setup_executor();
    executor
        .execute("CREATE TABLE delete_test (id TEXT, val INT) ORDER BY id")
        .unwrap();
    executor
        .execute("INSERT INTO delete_test (id, val) VALUES ('1', '100')")
        .unwrap();
    executor
        .execute("INSERT INTO delete_test (id, val) VALUES ('1', '200')")
        .unwrap();

    let result = executor.execute("DELETE FROM delete_test WHERE id = '1'");
    assert!(result.is_ok());

    let rows = executor
        .query("SELECT val FROM delete_test WHERE id = '1'")
        .unwrap();
    assert_eq!(rows.row_count(), 0);
}

#[test]
fn cli_query_rejects_non_select() {
    let executor = setup_executor();
    let result = executor.query("CREATE TABLE should_fail (id TEXT) ORDER BY id");
    assert!(result.is_err());
}

#[test]
fn cli_execute_vs_query() {
    let executor = setup_executor();
    executor
        .execute("CREATE TABLE exec_query (id TEXT, val INT) ORDER BY id")
        .unwrap();
    executor
        .execute("INSERT INTO exec_query (id, val) VALUES ('1', '42')")
        .unwrap();

    let result = executor.query("SELECT val FROM exec_query WHERE id = '1'");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().row_count(), 1);

    let result = executor.query("INSERT INTO exec_query (id, val) VALUES ('1', '99')");
    assert!(result.is_err());
}

#[test]
fn cli_schema_persistence() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE persist_test (id TEXT, a INT, b TEXT, c BOOL) ORDER BY id")
        .unwrap();

    executor
        .execute("INSERT INTO persist_test (id, a, b, c) VALUES ('1', '10', 'hello', 'true')")
        .unwrap();

    let catalog = executor.get_table_catalog("persist_test").unwrap();
    assert_eq!(catalog.columns().len(), 4);

    let rows = executor
        .query("SELECT a, b, c FROM persist_test WHERE id = '1'")
        .unwrap();
    assert_eq!(rows.row_count(), 1);
}

#[test]
fn cli_multiple_independent_tables() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE t1 (id TEXT, x INT) ORDER BY id")
        .unwrap();
    executor
        .execute("CREATE TABLE t2 (id TEXT, y TEXT) ORDER BY id")
        .unwrap();
    executor
        .execute("CREATE TABLE t3 (id TEXT, z BOOL) ORDER BY id")
        .unwrap();

    executor
        .execute("INSERT INTO t1 (id, x) VALUES ('1', '100')")
        .unwrap();
    executor
        .execute("INSERT INTO t2 (id, y) VALUES ('1', 'hello')")
        .unwrap();
    executor
        .execute("INSERT INTO t3 (id, z) VALUES ('1', 'true')")
        .unwrap();

    assert_eq!(executor.get_table_catalog("t1").unwrap().columns().len(), 2);
    assert_eq!(executor.get_table_catalog("t2").unwrap().columns().len(), 2);
    assert_eq!(executor.get_table_catalog("t3").unwrap().columns().len(), 2);

    let r1 = executor.query("SELECT x FROM t1 WHERE id = '1'").unwrap();
    let r2 = executor.query("SELECT y FROM t2 WHERE id = '1'").unwrap();
    let r3 = executor.query("SELECT z FROM t3 WHERE id = '1'").unwrap();

    assert_eq!(
        r1.row_iter().next().unwrap().value_as_string(0),
        Some("100".to_string())
    );
    assert_eq!(
        r2.row_iter().next().unwrap().value_as_string(0),
        Some("hello".to_string())
    );
    assert_eq!(
        r3.row_iter().next().unwrap().value_as_string(0),
        Some("true".to_string())
    );
}

#[test]
fn cli_all_data_types() {
    let executor = setup_executor();

    executor
        .execute("CREATE TABLE all_types (id TEXT, t TEXT, i INT, f FLOAT, b BOOL) ORDER BY id")
        .unwrap();

    executor
        .execute(
            "INSERT INTO all_types (id, t, i, f, b) VALUES ('1', 'text', '42', '3.14', 'true')",
        )
        .unwrap();

    let result = executor
        .query("SELECT t, i, f, b FROM all_types WHERE id = '1'")
        .unwrap();

    let row = result.row_iter().next().unwrap();
    assert_eq!(row.value_as_string(0), Some("text".to_string()));
    assert_eq!(row.value_as_string(1), Some("42".to_string()));
    assert!(row.value_as_string(2).unwrap().starts_with("3.14"));
    assert_eq!(row.value_as_string(3), Some("true".to_string()));
}

static PERSIST_TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);
static SERIAL_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn setup_persistent_executor(namespace: &str) -> SqlExecutor {
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

    let options = SqlExecutorWalOptions::new(namespace)
        .storage_dir("./test_storage")
        .cleanup_on_drop(false)
        .reset_namespace(false);

    SqlExecutor::with_wal_options(handler, directory, true, options)
}

fn unique_namespace(prefix: &str) -> String {
    let id = PERSIST_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut rng = rand::thread_rng();
    let suffix: u32 = rng.r#gen();
    format!("{prefix}-{id}-{suffix}")
}

fn cleanup_namespace(namespace: &str) {
    let options = SqlExecutorWalOptions::new(namespace)
        .storage_dir("./test_storage")
        .cleanup_on_drop(true)
        .reset_namespace(true);

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

    drop(SqlExecutor::with_wal_options(handler, directory, true, options));
}

#[test]
fn persist_table_schema_survives_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-schema");

    {
        let executor = setup_persistent_executor(&ns);
        executor
            .execute("CREATE TABLE users (id TEXT, name TEXT, age INT) ORDER BY id")
            .expect("CREATE TABLE failed");

        let tables = executor.table_names();
        assert!(tables.contains(&"users".to_string()));
    }

    {
        let executor = setup_persistent_executor(&ns);
        let tables = executor.table_names();
        assert!(
            tables.contains(&"users".to_string()),
            "table 'users' should persist after restart"
        );

        let catalog = executor
            .get_table_catalog("users")
            .expect("catalog should exist");
        assert_eq!(catalog.columns().len(), 3);
        assert_eq!(catalog.columns()[0].name, "id");
        assert_eq!(catalog.columns()[1].name, "name");
        assert_eq!(catalog.columns()[2].name, "age");
    }

    cleanup_namespace(&ns);
}

#[test]
fn persist_data_survives_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-data");

    {
        let executor = setup_persistent_executor(&ns);
        executor
            .execute("CREATE TABLE items (id TEXT, value INT) ORDER BY id")
            .expect("CREATE TABLE failed");

        executor
            .execute("INSERT INTO items (id, value) VALUES ('1', '100')")
            .expect("INSERT failed");
        executor
            .execute("INSERT INTO items (id, value) VALUES ('1', '200')")
            .expect("INSERT failed");
        executor
            .execute("INSERT INTO items (id, value) VALUES ('1', '300')")
            .expect("INSERT failed");

        let result = executor
            .query("SELECT value FROM items WHERE id = '1'")
            .expect("SELECT failed");
        assert_eq!(result.row_count(), 3);
    }

    {
        let executor = setup_persistent_executor(&ns);
        let result = executor
            .query("SELECT value FROM items WHERE id = '1'")
            .expect("SELECT failed after restart");

        assert_eq!(
            result.row_count(),
            3,
            "all 3 rows should persist after restart"
        );

        let values: Vec<i64> = result
            .row_iter()
            .map(|r| r.value_as_string(0).unwrap().parse().unwrap())
            .collect();
        assert!(values.contains(&100));
        assert!(values.contains(&200));
        assert!(values.contains(&300));
    }

    cleanup_namespace(&ns);
}

#[test]
fn persist_multiple_tables_survive_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-multi");

    {
        let executor = setup_persistent_executor(&ns);

        executor
            .execute("CREATE TABLE orders (id TEXT, total INT) ORDER BY id")
            .unwrap();
        executor
            .execute("CREATE TABLE customers (id TEXT, name TEXT) ORDER BY id")
            .unwrap();

        executor
            .execute("INSERT INTO orders (id, total) VALUES ('1', '500')")
            .unwrap();
        executor
            .execute("INSERT INTO customers (id, name) VALUES ('1', 'Alice')")
            .unwrap();
    }

    {
        let executor = setup_persistent_executor(&ns);

        let tables = executor.table_names();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"customers".to_string()));

        let orders = executor
            .query("SELECT total FROM orders WHERE id = '1'")
            .unwrap();
        assert_eq!(orders.row_count(), 1);
        assert_eq!(
            orders.row_iter().next().unwrap().value_as_string(0),
            Some("500".to_string())
        );

        let customers = executor
            .query("SELECT name FROM customers WHERE id = '1'")
            .unwrap();
        assert_eq!(customers.row_count(), 1);
        assert_eq!(
            customers.row_iter().next().unwrap().value_as_string(0),
            Some("Alice".to_string())
        );
    }

    cleanup_namespace(&ns);
}

#[test]
fn persist_updates_survive_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-update");

    {
        let executor = setup_persistent_executor(&ns);
        executor
            .execute("CREATE TABLE status (id TEXT, state TEXT) ORDER BY id")
            .unwrap();

        executor
            .execute("INSERT INTO status (id, state) VALUES ('1', 'pending')")
            .unwrap();

        executor
            .execute("UPDATE status SET state = 'completed' WHERE id = '1'")
            .unwrap();

        let result = executor
            .query("SELECT state FROM status WHERE id = '1'")
            .unwrap();
        assert_eq!(
            result.row_iter().next().unwrap().value_as_string(0),
            Some("completed".to_string())
        );
    }

    {
        let executor = setup_persistent_executor(&ns);
        let result = executor
            .query("SELECT state FROM status WHERE id = '1'")
            .expect("SELECT failed after restart");

        assert_eq!(
            result.row_iter().next().unwrap().value_as_string(0),
            Some("completed".to_string()),
            "updated value should persist after restart"
        );
    }

    cleanup_namespace(&ns);
}

#[test]
fn persist_deletes_survive_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-delete");

    {
        let executor = setup_persistent_executor(&ns);
        executor
            .execute("CREATE TABLE logs (id TEXT, msg TEXT) ORDER BY id")
            .unwrap();

        executor
            .execute("INSERT INTO logs (id, msg) VALUES ('1', 'keep')")
            .unwrap();
        executor
            .execute("INSERT INTO logs (id, msg) VALUES ('2', 'delete')")
            .unwrap();

        executor.execute("DELETE FROM logs WHERE id = '2'").unwrap();

        let result = executor.query("SELECT msg FROM logs WHERE id = '1'").unwrap();
        assert_eq!(result.row_count(), 1);

        let result = executor.query("SELECT msg FROM logs WHERE id = '2'").unwrap();
        assert_eq!(result.row_count(), 0);
    }

    {
        let executor = setup_persistent_executor(&ns);

        let result = executor
            .query("SELECT msg FROM logs WHERE id = '1'")
            .expect("SELECT failed");
        assert_eq!(result.row_count(), 1, "kept row should persist");

        let result = executor
            .query("SELECT msg FROM logs WHERE id = '2'")
            .expect("SELECT failed");
        assert_eq!(result.row_count(), 0, "deleted row should stay deleted");
    }

    cleanup_namespace(&ns);
}

#[test]
fn persist_large_dataset_survives_restart() {
    let _guard = SERIAL_TEST_LOCK.lock().unwrap();
    let ns = unique_namespace("persist-large");

    {
        let executor = setup_persistent_executor(&ns);
        executor
            .execute("CREATE TABLE numbers (id TEXT, n INT) ORDER BY id")
            .unwrap();

        for i in 0..100 {
            executor
                .execute(&format!(
                    "INSERT INTO numbers (id, n) VALUES ('1', '{}')",
                    i
                ))
                .unwrap();
        }

        let result = executor
            .query("SELECT COUNT(*) FROM numbers WHERE id = '1'")
            .unwrap();
        assert_eq!(
            result.row_iter().next().unwrap().value_as_string(0),
            Some("100".to_string())
        );
    }

    {
        let executor = setup_persistent_executor(&ns);
        let result = executor
            .query("SELECT COUNT(*) FROM numbers WHERE id = '1'")
            .expect("SELECT failed");

        assert_eq!(
            result.row_iter().next().unwrap().value_as_string(0),
            Some("100".to_string()),
            "all 100 rows should persist"
        );

        let sum_result = executor
            .query("SELECT SUM(n) FROM numbers WHERE id = '1'")
            .unwrap();
        assert_eq!(
            sum_result.row_iter().next().unwrap().value_as_string(0),
            Some("4950".to_string()) // 0+1+2+...+99
        );
    }

    cleanup_namespace(&ns);
}
