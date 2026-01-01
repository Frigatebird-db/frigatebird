use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SelectResult, SqlExecutor};
use std::sync::{Arc, RwLock};

trait ResultRowsExt {
    fn rows(&self) -> Vec<Vec<Option<String>>>;
}

impl ResultRowsExt for SelectResult {
    fn rows(&self) -> Vec<Vec<Option<String>>> {
        self.row_iter().map(|row| row.to_vec()).collect()
    }
}

fn setup_executor() -> (SqlExecutor, Arc<PageHandler>, Arc<PageDirectory>) {
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
    let executor =
        SqlExecutor::new_with_writer_mode(Arc::clone(&handler), Arc::clone(&directory), false);
    (executor, handler, directory)
}

#[test]
fn test_e2e_create_insert_update_delete_full_lifecycle() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE products (id TEXT, name TEXT, price TEXT, stock TEXT) ORDER BY id")
        .expect("create table");

    // Insert initial data
    executor
        .execute(
            "INSERT INTO products (id, name, price, stock) VALUES ('1', 'Laptop', '999.99', '10')",
        )
        .expect("insert");

    // Verify insert
    let result = executor
        .query("SELECT name, price, stock FROM products WHERE id = '1'")
        .expect("select after insert");
    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows()[0],
        vec![
            Some("Laptop".to_string()),
            Some("999.99".to_string()),
            Some("10".to_string())
        ]
    );

    // Update price and stock
    executor
        .execute("UPDATE products SET price = '899.99', stock = '15' WHERE id = '1'")
        .expect("update");

    // Verify update
    let result = executor
        .query("SELECT name, price, stock FROM products WHERE id = '1'")
        .expect("select after update");
    assert_eq!(
        result.rows()[0],
        vec![
            Some("Laptop".to_string()),
            Some("899.99".to_string()),
            Some("15".to_string())
        ]
    );

    // Delete the product
    executor
        .execute("DELETE FROM products WHERE id = '1'")
        .expect("delete");

    // Verify deletion
    let result = executor
        .query("SELECT COUNT(*) FROM products WHERE id = '1'")
        .expect("count after delete");
    assert_eq!(result.rows()[0], vec![Some("0".to_string())]);
}

#[test]
fn test_e2e_index_refine_with_non_sort_predicate() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE tasks (id TEXT, status TEXT, payload TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO tasks (id, status, payload) VALUES ('1', 'keep', 'alpha')")
        .expect("insert keep");
    executor
        .execute("INSERT INTO tasks (id, status, payload) VALUES ('1', 'drop', 'beta')")
        .expect("insert drop");

    let result = executor
        .query("SELECT payload FROM tasks WHERE id = '1' AND status = 'keep'")
        .expect("select with refine");
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0], vec![Some("alpha".to_string())]);

    executor
        .execute("UPDATE tasks SET payload = 'updated' WHERE id = '1' AND status = 'keep'")
        .expect("update with refine");
    let result = executor
        .query("SELECT payload FROM tasks WHERE id = '1' AND status = 'keep'")
        .expect("select after update");
    assert_eq!(result.rows()[0], vec![Some("updated".to_string())]);

    executor
        .execute("DELETE FROM tasks WHERE id = '1' AND status = 'drop'")
        .expect("delete with refine");
    let result = executor
        .query("SELECT status FROM tasks WHERE id = '1'")
        .expect("select after delete");
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0], vec![Some("keep".to_string())]);
}

#[test]
fn test_e2e_math_functions_comprehensive() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE numbers (id TEXT, value TEXT) ORDER BY id")
        .expect("create table");

    // Insert test data
    executor
        .execute("INSERT INTO numbers (id, value) VALUES ('1', '-42.7')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO numbers (id, value) VALUES ('1', '3.14159')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO numbers (id, value) VALUES ('1', '10')")
        .expect("insert 3");
    executor
        .execute("INSERT INTO numbers (id, value) VALUES ('1', '100')")
        .expect("insert 4");

    // Test ABS
    let result = executor
        .query("SELECT AVG(ABS(value)) FROM numbers WHERE id = '1'")
        .expect("abs");
    // |-42.7| + |3.14159| + |10| + |100| = 42.7 + 3.14159 + 10 + 100 = 155.84159, avg = 38.96
    assert!(result.rows()[0][0].as_ref().unwrap().starts_with("38.96"));

    // Test ROUND
    let result = executor
        .query("SELECT ROUND(AVG(value), 2) FROM numbers WHERE id = '1'")
        .expect("round");
    // (-42.7 + 3.14159 + 10 + 100) / 4 ≈ 17.6
    assert!(result.rows()[0][0].as_ref().unwrap().starts_with("17.6"));

    // Test that CEIL and FLOOR work (tested individually elsewhere, here just verify avg is reasonable)
    let result = executor
        .query("SELECT AVG(value) FROM numbers WHERE id = '1'")
        .expect("avg for bounds check");
    // Average should be around 17.6
    let avg_val = result.rows()[0][0]
        .as_ref()
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!(avg_val > 17.0 && avg_val < 18.0);

    // Test POWER - sum of squares
    let result = executor
        .query("SELECT ROUND(SUM(POWER(value, 2)), 2) FROM numbers WHERE id = '1'")
        .expect("power");
    // (-42.7)^2 + (3.14159)^2 + 10^2 + 100^2 = 1823.29 + 9.87 + 100 + 10000 = 11933.16
    assert!(result.rows()[0][0].as_ref().unwrap().starts_with("11933"));
}

#[test]
fn test_e2e_aggregate_functions_complete() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE sales (id TEXT, amount TEXT) ORDER BY id")
        .expect("create table");

    // Insert data: 100, 150, 200, 50
    executor
        .execute("INSERT INTO sales (id, amount) VALUES ('1', '100')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO sales (id, amount) VALUES ('1', '150')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO sales (id, amount) VALUES ('1', '200')")
        .expect("insert 3");
    executor
        .execute("INSERT INTO sales (id, amount) VALUES ('1', '50')")
        .expect("insert 4");

    // Test all aggregate functions at once
    let result = executor
        .query("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales WHERE id = '1'")
        .expect("all aggregates");
    assert_eq!(
        result.rows()[0],
        vec![
            Some("4".to_string()),
            Some("500".to_string()),
            Some("125".to_string()),
            Some("50".to_string()),
            Some("200".to_string())
        ]
    );

    // Test VARIANCE and STDDEV
    // Values: 100, 150, 200, 50
    // Mean: 125
    // Variance: ((100-125)^2 + (150-125)^2 + (200-125)^2 + (50-125)^2) / 4
    //         = (625 + 625 + 5625 + 5625) / 4 = 12500 / 4 = 3125
    let result = executor
        .query("SELECT VARIANCE(amount), ROUND(STDDEV(amount), 2) FROM sales WHERE id = '1'")
        .expect("variance/stddev");
    assert_eq!(result.rows()[0][0], Some("3125".to_string()));
    assert_eq!(result.rows()[0][1], Some("55.9".to_string()));

    // Test PERCENTILE_CONT
    let result = executor
        .query("SELECT percentile_cont(0.5, amount) FROM sales WHERE id = '1'")
        .expect("median");
    assert_eq!(result.rows()[0], vec![Some("125".to_string())]);

    let result = executor
        .query("SELECT percentile_cont(0.25, amount) FROM sales WHERE id = '1'")
        .expect("25th percentile");
    assert_eq!(result.rows()[0], vec![Some("87.5".to_string())]);
}

#[test]
fn test_e2e_width_bucket_function() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE scores (id TEXT, score TEXT) ORDER BY id")
        .expect("create table");

    // Insert data
    executor
        .execute("INSERT INTO scores (id, score) VALUES ('1', '15')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO scores (id, score) VALUES ('1', '25')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO scores (id, score) VALUES ('1', '85')")
        .expect("insert 3");
    executor
        .execute("INSERT INTO scores (id, score) VALUES ('1', '105')")
        .expect("insert 4");

    // Test WIDTH_BUCKET with AVG
    // 15 -> bucket 1, 25 -> bucket 2, 85 -> bucket 5, 105 -> bucket 6
    // AVG = (1+2+5+6)/4 = 14/4 = 3.5
    let result = executor
        .query("SELECT AVG(width_bucket(score, 0, 100, 5)) FROM scores WHERE id = '1'")
        .expect("avg width_bucket");
    assert_eq!(result.rows()[0], vec![Some("3.5".to_string())]);
}

#[test]
fn test_e2e_null_handling() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE data (id TEXT, value TEXT) ORDER BY id")
        .expect("create table");

    let null_marker = "\u{0001}";

    // Insert data with nulls
    executor
        .execute("INSERT INTO data (id, value) VALUES ('1', '10')")
        .expect("insert 1");
    executor
        .execute(&format!(
            "INSERT INTO data (id, value) VALUES ('1', '{}')",
            null_marker
        ))
        .expect("insert 2");
    executor
        .execute("INSERT INTO data (id, value) VALUES ('1', '30')")
        .expect("insert 3");

    // Test COUNT(*) includes nulls
    let result = executor
        .query("SELECT COUNT(*) FROM data WHERE id = '1'")
        .expect("count all");
    assert_eq!(result.rows()[0], vec![Some("3".to_string())]);

    // Test COUNT(value) excludes nulls
    let result = executor
        .query("SELECT COUNT(value) FROM data WHERE id = '1'")
        .expect("count non-null");
    assert_eq!(result.rows()[0], vec![Some("2".to_string())]);

    // Test SUM excludes nulls
    let result = executor
        .query("SELECT SUM(value) FROM data WHERE id = '1'")
        .expect("sum");
    assert_eq!(result.rows()[0], vec![Some("40".to_string())]);

    // Test AVG excludes nulls (40/2 = 20)
    let result = executor
        .query("SELECT AVG(value) FROM data WHERE id = '1'")
        .expect("avg");
    assert_eq!(result.rows()[0], vec![Some("20".to_string())]);
}

#[test]
fn test_e2e_exp_ln_log_functions() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE math (id TEXT, value TEXT) ORDER BY id")
        .expect("create table");

    // Insert data
    executor
        .execute("INSERT INTO math (id, value) VALUES ('1', '1')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO math (id, value) VALUES ('1', '2')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO math (id, value) VALUES ('1', '10')")
        .expect("insert 3");
    executor
        .execute("INSERT INTO math (id, value) VALUES ('1', '100')")
        .expect("insert 4");

    // Test EXP - average of e^value
    let result = executor
        .query("SELECT ROUND(AVG(EXP(value)), 2) FROM math WHERE id = '1'")
        .expect("exp");
    // e^1 + e^2 + e^10 + e^100 is huge, so this should be a large number
    assert!(
        result.rows()[0][0]
            .as_ref()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 1000.0
    );

    // Test LN - average of ln(value)
    let result = executor
        .query("SELECT ROUND(AVG(LN(value)), 3) FROM math WHERE id = '1'")
        .expect("ln");
    // ln(1)=0, ln(2)≈0.693, ln(10)≈2.303, ln(100)≈4.605
    // avg ≈ 1.9
    assert!(result.rows()[0][0].as_ref().unwrap().starts_with("1.9"));

    // Test LOG base 10
    let result = executor
        .query("SELECT AVG(LOG(10, value)) FROM math WHERE id = '1'")
        .expect("log");
    // log10(1)=0, log10(2)≈0.301, log10(10)=1, log10(100)=2
    // avg ≈ 0.825
    assert!(result.rows()[0][0].as_ref().unwrap().starts_with("0.825"));
}

#[test]
fn test_e2e_combined_functions_and_case() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE values (id TEXT, amount TEXT) ORDER BY id")
        .expect("create table");

    // Insert data
    executor
        .execute("INSERT INTO values (id, amount) VALUES ('1', '10')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO values (id, amount) VALUES ('1', '20')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO values (id, amount) VALUES ('1', '30')")
        .expect("insert 3");
    executor
        .execute("INSERT INTO values (id, amount) VALUES ('1', '40')")
        .expect("insert 4");

    // Test CASE with aggregation
    let result = executor
        .query("SELECT SUM(CASE WHEN amount > 20 THEN 1 ELSE 0 END) FROM values WHERE id = '1'")
        .expect("case sum");
    assert_eq!(result.rows()[0], vec![Some("2".to_string())]);

    // Test combined math functions
    let result = executor
        .query("SELECT ROUND(AVG(POWER(amount, 2)), 2) FROM values WHERE id = '1'")
        .expect("avg of squares");
    // (100 + 400 + 900 + 1600) / 4 = 3000 / 4 = 750
    assert_eq!(result.rows()[0], vec![Some("750".to_string())]);
}

#[test]
fn test_e2e_multiple_inserts_updates_deletes() {
    let (executor, _, _) = setup_executor();

    // Create table
    executor
        .execute("CREATE TABLE inventory (id TEXT, item TEXT, quantity TEXT) ORDER BY id")
        .expect("create table");

    // Insert 5 items for product id='1'
    for i in 1..=5 {
        executor
            .execute(&format!(
                "INSERT INTO inventory (id, item, quantity) VALUES ('1', 'Item{}', '{}')",
                i,
                i * 10
            ))
            .expect("insert");
    }

    // Verify count
    let result = executor
        .query("SELECT COUNT(*) FROM inventory WHERE id = '1'")
        .expect("count");
    assert_eq!(result.rows()[0], vec![Some("5".to_string())]);

    // Verify sum
    let result = executor
        .query("SELECT SUM(quantity) FROM inventory WHERE id = '1'")
        .expect("sum");
    // 10 + 20 + 30 + 40 + 50 = 150
    assert_eq!(result.rows()[0], vec![Some("150".to_string())]);

    // Update all quantities (double them)
    for i in 1..=5 {
        executor
            .execute(&format!(
                "UPDATE inventory SET quantity = '{}' WHERE id = '1' AND item = 'Item{}'",
                i * 20,
                i
            ))
            .expect("update");
    }

    // Verify new sum
    let result = executor
        .query("SELECT SUM(quantity) FROM inventory WHERE id = '1'")
        .expect("sum after update");
    // 20 + 40 + 60 + 80 + 100 = 300
    assert_eq!(result.rows()[0], vec![Some("300".to_string())]);

    // Delete half the items
    for i in 1..=2 {
        executor
            .execute(&format!(
                "DELETE FROM inventory WHERE id = '1' AND item = 'Item{}'",
                i
            ))
            .expect("delete");
    }

    // Verify count decreased
    let result = executor
        .query("SELECT COUNT(*) FROM inventory WHERE id = '1'")
        .expect("count after delete");
    assert_eq!(result.rows()[0], vec![Some("3".to_string())]);

    // Verify sum
    let result = executor
        .query("SELECT SUM(quantity) FROM inventory WHERE id = '1'")
        .expect("sum after delete");
    // 60 + 80 + 100 = 240
    assert_eq!(result.rows()[0], vec![Some("240".to_string())]);
}

#[test]
fn test_select_on_non_sort_column_filters() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE tasks (id TEXT, status TEXT, payload TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO tasks (id, status, payload) VALUES ('1', 'done', 'alpha')")
        .expect("insert done");
    executor
        .execute("INSERT INTO tasks (id, status, payload) VALUES ('2', 'pending', 'beta')")
        .expect("insert pending");
    executor
        .execute("INSERT INTO tasks (id, status, payload) VALUES ('3', 'done', 'gamma')")
        .expect("insert done second");

    let result = executor
        .query("SELECT COUNT(*) FROM tasks WHERE status = 'done'")
        .expect("count done");
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0], vec![Some("2".to_string())]);

    let result = executor
        .query("SELECT COUNT(*) FROM tasks WHERE status = 'pending'")
        .expect("count pending");
    assert_eq!(result.rows()[0], vec![Some("1".to_string())]);

    let result = executor
        .query("SELECT COUNT(*) FROM tasks WHERE status = 'done' OR status = 'pending'")
        .expect("count combined");
    assert_eq!(result.rows()[0], vec![Some("3".to_string())]);
}

#[test]
fn test_select_without_where_and_order_by() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE jobs (id TEXT, status TEXT, payload TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO jobs (id, status, payload) VALUES ('1', 'open', 'alpha')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO jobs (id, status, payload) VALUES ('2', 'pending', 'beta')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO jobs (id, status, payload) VALUES ('3', 'done', 'gamma')")
        .expect("insert 3");

    let result = executor
        .query("SELECT COUNT(*) FROM jobs")
        .expect("select without where");
    assert_eq!(result.rows()[0], vec![Some("3".to_string())]);

    let result = executor
        .query("SELECT id FROM jobs ORDER BY id")
        .expect("select order by sort key");
    assert_eq!(result.row_count(), 3);
    assert_eq!(result.rows()[0], vec![Some("1".to_string())]);
    assert_eq!(result.rows()[1], vec![Some("2".to_string())]);
    assert_eq!(result.rows()[2], vec![Some("3".to_string())]);

    let result = executor
        .query("SELECT id, status FROM jobs ORDER BY id DESC")
        .expect("select order by desc");
    assert_eq!(result.row_count(), 3);
    assert_eq!(
        result.rows()[0],
        vec![Some("3".to_string()), Some("done".to_string())]
    );
    assert_eq!(
        result.rows()[1],
        vec![Some("2".to_string()), Some("pending".to_string())]
    );
    assert_eq!(
        result.rows()[2],
        vec![Some("1".to_string()), Some("open".to_string())]
    );
}

#[test]
fn test_update_and_delete_with_non_sort_filters() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE tickets (id TEXT, status TEXT, payload TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO tickets (id, status, payload) VALUES ('1', 'open', 'a')")
        .expect("insert 1");
    executor
        .execute("INSERT INTO tickets (id, status, payload) VALUES ('2', 'pending', 'b')")
        .expect("insert 2");
    executor
        .execute("INSERT INTO tickets (id, status, payload) VALUES ('3', 'done', 'c')")
        .expect("insert 3");

    executor
        .execute("UPDATE tickets SET payload = 'updated' WHERE status = 'pending'")
        .expect("update by non-sort predicate");

    let result = executor
        .query("SELECT payload FROM tickets WHERE id = '2'")
        .expect("select updated row");
    assert_eq!(result.rows()[0], vec![Some("updated".to_string())]);

    executor
        .execute("DELETE FROM tickets WHERE status = 'open' OR status = 'done'")
        .expect("delete via non-sort predicate");

    let result = executor
        .query("SELECT COUNT(*) FROM tickets")
        .expect("count remaining");
    assert_eq!(result.rows()[0], vec![Some("1".to_string())]);

    let result = executor
        .query("SELECT id FROM tickets")
        .expect("select remaining row");
    assert_eq!(result.rows(), vec![vec![Some("2".to_string())]]);
}

#[test]
fn test_update_and_delete_without_where() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE bulk (id TEXT, status TEXT) ORDER BY id")
        .expect("create table");

    for i in 0..5 {
        executor
            .execute(&format!(
                "INSERT INTO bulk (id, status) VALUES ('{}', 'open')",
                i
            ))
            .expect("insert row");
    }

    executor
        .execute("UPDATE bulk SET status = 'closed'")
        .expect("update all rows");

    let result = executor
        .query("SELECT COUNT(*) FROM bulk WHERE status = 'closed'")
        .expect("count closed");
    assert_eq!(result.rows()[0], vec![Some("5".to_string())]);

    executor
        .execute("DELETE FROM bulk")
        .expect("delete all rows");

    let result = executor
        .query("SELECT COUNT(*) FROM bulk")
        .expect("count after delete");
    assert_eq!(result.rows()[0], vec![Some("0".to_string())]);
}

#[test]
fn test_insert_default_keyword() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE defaults (id TEXT, status TEXT, payload TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO defaults (id, status, payload) VALUES ('1', DEFAULT, 'alpha')")
        .expect("insert default");

    let result = executor
        .query("SELECT status FROM defaults WHERE id = '1'")
        .expect("select status");
    assert_eq!(result.rows()[0], vec![None]);
}

#[test]
fn test_select_distinct_rows() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE distincts (id TEXT, status TEXT) ORDER BY id")
        .expect("create table");

    executor
        .execute("INSERT INTO distincts (id, status) VALUES ('1', 'a')")
        .expect("insert a1");
    executor
        .execute("INSERT INTO distincts (id, status) VALUES ('2', 'a')")
        .expect("insert a2");
    executor
        .execute("INSERT INTO distincts (id, status) VALUES ('3', 'b')")
        .expect("insert b");

    let result = executor
        .query("SELECT DISTINCT status FROM distincts")
        .expect("distinct");
    let mut values: Vec<_> = result
        .rows()
        .into_iter()
        .map(|row| row[0].clone().unwrap())
        .collect();
    values.sort();
    assert_eq!(values, vec!["a".to_string(), "b".to_string()]);

    let result = executor
        .query("SELECT DISTINCT status FROM distincts LIMIT 1")
        .expect("distinct limit");
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_update_delete_with_sort_key_prefix_predicates() {
    let (executor, _, _) = setup_executor();

    executor
        .execute(
            "CREATE TABLE users (region TEXT, user_id TEXT, name TEXT) ORDER BY (region, user_id)",
        )
        .expect("create users");

    let seed_rows = vec![
        ("US", "1", "Alice"),
        ("US", "2", "Bob"),
        ("US", "3", "Carol"),
        ("EU", "1", "Eve"),
        ("EU", "2", "Mallory"),
    ];
    for (region, user_id, name) in seed_rows {
        executor
            .execute(&format!(
                "INSERT INTO users (region, user_id, name) VALUES ('{region}', '{user_id}', '{name}')"
            ))
            .expect("seed insert");
    }

    executor
        .execute("UPDATE users SET name = 'Anon' WHERE user_id = '2'")
        .expect("update without leading column");
    let result = executor
        .query("SELECT name FROM users WHERE region = 'US' AND user_id = '2'")
        .expect("select updated us row");
    assert_eq!(result.rows()[0], vec![Some("Anon".to_string())]);
    let result = executor
        .query("SELECT name FROM users WHERE region = 'EU' AND user_id = '2'")
        .expect("select updated eu row");
    assert_eq!(result.rows()[0], vec![Some("Anon".to_string())]);

    executor
        .execute("UPDATE users SET name = 'Priority' WHERE region = 'US' AND user_id > '1'")
        .expect("update range");
    let result = executor
        .query("SELECT user_id, name FROM users WHERE region = 'US' ORDER BY user_id")
        .expect("select us rows after range update");
    assert_eq!(
        result.rows(),
        vec![
            vec![Some("1".to_string()), Some("Alice".to_string())],
            vec![Some("2".to_string()), Some("Priority".to_string())],
            vec![Some("3".to_string()), Some("Priority".to_string())],
        ]
    );

    executor
        .execute("UPDATE users SET name = 'Special' WHERE user_id = '1' OR user_id = '3'")
        .expect("update with or");
    let result = executor
        .query(
            "SELECT region, user_id, name FROM users WHERE user_id IN ('1','3') ORDER BY region, user_id",
        )
        .expect("select rows after OR update");
    assert_eq!(
        result.rows(),
        vec![
            vec![
                Some("EU".to_string()),
                Some("1".to_string()),
                Some("Special".to_string())
            ],
            vec![
                Some("US".to_string()),
                Some("1".to_string()),
                Some("Special".to_string())
            ],
            vec![
                Some("US".to_string()),
                Some("3".to_string()),
                Some("Special".to_string())
            ],
        ]
    );

    executor
        .execute("DELETE FROM users WHERE user_id = '3'")
        .expect("delete without leading column");
    let result = executor
        .query("SELECT COUNT(*) FROM users WHERE user_id = '3'")
        .expect("count after delete");
    assert_eq!(result.rows()[0], vec![Some("0".to_string())]);

    executor
        .execute("DELETE FROM users WHERE region = 'EU' AND user_id >= '2'")
        .expect("delete range");
    let result = executor
        .query("SELECT COUNT(*) FROM users WHERE region = 'EU'")
        .expect("count eu rows");
    assert_eq!(result.rows()[0], vec![Some("1".to_string())]);

    executor
        .execute("DELETE FROM users WHERE user_id = '1' OR user_id = '2'")
        .expect("delete with or");
    let result = executor
        .query("SELECT COUNT(*) FROM users")
        .expect("count all rows");
    assert_eq!(result.rows()[0], vec![Some("0".to_string())]);
}

#[test]
fn test_select_with_sort_key_prefix_predicates() {
    let (executor, _, _) = setup_executor();

    executor
        .execute(
            "CREATE TABLE users (region TEXT, user_id TEXT, name TEXT) ORDER BY (region, user_id)",
        )
        .expect("create users");

    let seed_rows = vec![
        ("US", "1", "Alice"),
        ("US", "2", "Bob"),
        ("US", "3", "Carol"),
        ("EU", "1", "Eve"),
        ("EU", "2", "Mallory"),
    ];
    for (region, user_id, name) in seed_rows {
        executor
            .execute(&format!(
                "INSERT INTO users (region, user_id, name) VALUES ('{region}', '{user_id}', '{name}')"
            ))
            .expect("seed insert");
    }

    let result = executor
        .query(
            "SELECT region, user_id, name FROM users WHERE user_id = '2' ORDER BY region, user_id",
        )
        .expect("select missing leading predicate");
    assert_eq!(
        result.rows(),
        vec![
            vec![
                Some("EU".to_string()),
                Some("2".to_string()),
                Some("Mallory".to_string())
            ],
            vec![
                Some("US".to_string()),
                Some("2".to_string()),
                Some("Bob".to_string())
            ]
        ]
    );

    let result = executor
        .query(
            "SELECT user_id, name FROM users WHERE region = 'US' AND user_id >= '2' ORDER BY user_id",
        )
        .expect("select with range");
    assert_eq!(
        result.rows(),
        vec![
            vec![Some("2".to_string()), Some("Bob".to_string())],
            vec![Some("3".to_string()), Some("Carol".to_string())],
        ]
    );

    let result = executor
        .query(
            "SELECT region, user_id FROM users WHERE user_id = '1' OR user_id = '3' ORDER BY region, user_id",
        )
        .expect("select with or");
    assert_eq!(
        result.rows(),
        vec![
            vec![Some("EU".to_string()), Some("1".to_string())],
            vec![Some("US".to_string()), Some("1".to_string())],
            vec![Some("US".to_string()), Some("3".to_string())],
        ]
    );
}

#[test]
fn test_case_expression_prevents_divide_by_zero() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE metrics (id BIGINT, price DOUBLE, discount DOUBLE) ORDER BY id")
        .expect("create table");
    executor
        .execute("INSERT INTO metrics (id, price, discount) VALUES (1, 2793.41, 0)")
        .expect("insert row");

    let result = executor
        .query(
            "SELECT CASE WHEN discount = 0 THEN NULL ELSE price / discount END AS safe FROM metrics WHERE id = '1'",
        )
        .expect("query");
    assert_eq!(result.rows(), vec![vec![None]]);
}

#[test]
fn test_case_expression_with_multi_column_order_by() {
    let (executor, _, _) = setup_executor();

    executor
        .execute(
            "CREATE TABLE metrics2 (id BIGINT, created_at TEXT, price DOUBLE, discount DOUBLE) ORDER BY (id, created_at)",
        )
        .expect("create table");
    executor
        .execute(
            "INSERT INTO metrics2 (id, created_at, price, discount) VALUES (1, '2024-01-01 00:00:00', 2793.41, 0)",
        )
        .expect("insert row");
    executor
        .execute(
            "INSERT INTO metrics2 (id, created_at, price, discount) VALUES (2, '2024-01-02 00:00:00', 100.0, 5.0)",
        )
        .expect("insert row 2");

    let result = executor
        .query(
            "SELECT id, CASE WHEN discount = 0 THEN NULL ELSE price / discount END AS safe FROM metrics2 WHERE id <= 2 ORDER BY id",
        )
        .expect("query");
    assert_eq!(
        result.rows(),
        vec![
            vec![Some("1".to_string()), None],
            vec![Some("2".to_string()), Some("20".to_string())]
        ]
    );
}

#[test]
fn test_case_expression_inside_aggregate() {
    let (executor, _, _) = setup_executor();

    executor
        .execute(
            "CREATE TABLE agg_case (id BIGINT, active BOOLEAN, price DOUBLE, net_amount DOUBLE) ORDER BY id",
        )
        .expect("create table");
    executor
        .execute("INSERT INTO agg_case (id, active, price, net_amount) VALUES (1, true, 600, 500)")
        .expect("insert row 1");
    executor
        .execute("INSERT INTO agg_case (id, active, price, net_amount) VALUES (2, false, 300, 250)")
        .expect("insert row 2");

    let result = executor
        .query(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN active THEN 1 ELSE 0 END) AS active_count, SUM(CASE WHEN price > 500 THEN net_amount ELSE 0 END) AS high_price_revenue FROM agg_case",
        )
        .expect("aggregate case query");
    assert_eq!(
        result.rows(),
        vec![vec![
            Some("2".to_string()),
            Some("1".to_string()),
            Some("500".to_string())
        ]]
    );
}
