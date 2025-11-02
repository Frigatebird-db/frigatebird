use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryUncompressed};
use idk_uwu_ig::entry::Entry;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::ops_handler::{
    create_table_from_plan, insert_sorted_row, upsert_data_into_table_column,
};
use idk_uwu_ig::page::Page;
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SqlExecutionError, SqlExecutor};
use idk_uwu_ig::sql::{ColumnSpec, CreateTablePlan};
use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

fn build_table_with_rows(
    table: &str,
    columns_with_data: &[(&str, Vec<&str>)],
    order_by: Vec<String>,
) -> (
    Arc<PageHandler>,
    Arc<PageDirectory>,
    Arc<RwLock<TableMetaStore>>,
) {
    assert!(
        !columns_with_data.is_empty(),
        "test helper requires at least one column"
    );

    let expected_len = columns_with_data[0].1.len();
    for (name, values) in columns_with_data {
        assert_eq!(values.len(), expected_len, "column {name} length mismatch");
    }

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

    let column_specs: Vec<ColumnSpec> = columns_with_data
        .iter()
        .map(|(name, _)| ColumnSpec::new(*name, "String"))
        .collect();

    let plan = CreateTablePlan::new(table.to_string(), column_specs, order_by, false);
    create_table_from_plan(&directory, &plan).expect("create table with order by");

    for (name, values) in columns_with_data {
        let descriptor = directory
            .register_page_in_table_with_sizes(
                table,
                name,
                format!("mem://{table}_{name}"),
                0,
                0,
                0,
                values.len() as u64,
            )
            .expect("register column page");

        let mut cached = PageCacheEntryUncompressed { page: Page::new() };
        cached.page.page_metadata = descriptor.id.clone();
        for value in values {
            cached.page.entries.push(Entry::new(value));
        }
        handler.write_back_uncompressed(&descriptor.id, cached);
        handler
            .update_entry_count_in_table(table, name, values.len() as u64)
            .expect("sync entry count");
    }

    (handler, directory, store)
}

fn build_sql_executor() -> (
    SqlExecutor,
    Arc<PageHandler>,
    Arc<PageDirectory>,
    Arc<RwLock<TableMetaStore>>,
) {
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
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    (executor, handler, directory, store)
}

fn collect_column_values(
    handler: &PageHandler,
    directory: &PageDirectory,
    table: &str,
    column: &str,
) -> Vec<String> {
    let descriptor = directory
        .latest_in_table(table, column)
        .expect("latest descriptor");
    let page = handler
        .get_page(descriptor.clone())
        .expect("load page for collection");
    page.page
        .entries
        .iter()
        .map(|entry| entry.get_data().to_string())
        .collect()
}

fn collect_table_rows(
    handler: &PageHandler,
    directory: &PageDirectory,
    table: &str,
    column_names: &[&str],
) -> Vec<Vec<String>> {
    if column_names.is_empty() {
        return Vec::new();
    }

    let row_count = directory
        .latest_in_table(table, column_names[0])
        .map(|descriptor| descriptor.entry_count as usize)
        .unwrap_or(0);

    let mut rows = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let mut row = Vec::with_capacity(column_names.len());
        for column in column_names {
            let entry = handler
                .read_entry_at(table, column, row_idx as u64)
                .expect("entry must exist");
            row.push(entry.get_data().to_string());
        }
        rows.push(row);
    }
    rows
}

fn assert_metadata_for_columns(
    store: &Arc<RwLock<TableMetaStore>>,
    table: &str,
    columns: &[&str],
    expected_len: usize,
) {
    let guard = store.read().expect("metadata read lock");
    for column in columns {
        if expected_len == 0 {
            assert!(
                guard.locate_range(table, column, 0, 0).is_empty(),
                "expected empty metadata for {table}.{column}"
            );
            continue;
        }

        let end = (expected_len - 1) as u64;
        let slices = guard.locate_range(table, column, 0, end);
        assert_eq!(
            slices.len(),
            1,
            "expected single slice for {table}.{column}"
        );
        let slice = &slices[0];
        assert_eq!(slice.start_row_offset, 0);
        assert_eq!(slice.end_row_offset, expected_len as u64);

        let location = guard
            .locate_row(table, column, end)
            .expect("row location must exist");
        assert_eq!(location.page_row_index, end);
        assert_eq!(location.descriptor.entry_count, expected_len as u64);
    }
}

#[test]
fn sql_executor_query_returns_projected_rows() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[
            ("id", vec!["1", "2", "3"]),
            ("name", vec!["Alice", "Bob", "Cara"]),
        ],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT id, name FROM users WHERE id = 2")
        .expect("simple select query");
    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        result.rows,
        vec![vec![Some("2".to_string()), Some("Bob".to_string())]]
    );

    let alias_result = executor
        .query("SELECT name AS nickname FROM users WHERE id = 3")
        .expect("alias projection");
    assert_eq!(alias_result.columns, vec!["nickname".to_string()]);
    assert_eq!(alias_result.rows, vec![vec![Some("Cara".to_string())]]);

    let wildcard_result = executor
        .query("SELECT u.* FROM users u WHERE id = 1")
        .expect("wildcard projection");
    assert_eq!(
        wildcard_result.columns,
        vec!["id".to_string(), "name".to_string()]
    );
    assert_eq!(
        wildcard_result.rows,
        vec![vec![Some("1".to_string()), Some("Alice".to_string())]]
    );
}

#[test]
fn sql_executor_query_limit_offset_respected() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[
            ("id", vec!["1", "1", "1"]),
            ("name", vec!["Alice", "Alicia", "Alison"]),
        ],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT name FROM users WHERE id = 1 LIMIT 1 OFFSET 1")
        .expect("limit/offset query");
    assert_eq!(result.columns, vec!["name".to_string()]);
    assert_eq!(result.rows, vec![vec![Some("Alicia".to_string())]]);
}

#[test]
fn sql_executor_query_requires_sort_predicate() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[("id", vec!["1", "2"]), ("name", vec!["Alice", "Bob"])],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT name FROM users WHERE name = 'Alice'")
        .expect("missing sort predicate now scans");
    assert_eq!(result.columns, vec!["name".to_string()]);
    assert_eq!(result.rows, vec![vec![Some("Alice".to_string())]]);
}

#[test]
fn sql_executor_query_rejects_expressions() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[("id", vec!["1"]), ("name", vec!["Alice"])],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let err = executor
        .query("SELECT id + 1 FROM users WHERE id = 1")
        .expect_err("expression projection must error");
    assert!(matches!(
        err,
        SqlExecutionError::Unsupported(message)
            if message.contains("only simple column projections are supported")
    ));
}

#[test]
fn sql_executor_query_supports_comparison_predicates() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[
            ("id", vec!["1", "1", "1"]),
            ("age", vec!["18", "30", "45"]),
            ("name", vec!["Alice", "Beatrice", "Cara"]),
        ],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT name FROM users WHERE id = 1 AND age >= 25 AND age < 50")
        .expect("comparison predicates");
    assert_eq!(result.columns, vec!["name".to_string()]);
    assert_eq!(
        result.rows,
        vec![
            vec![Some("Beatrice".to_string())],
            vec![Some("Cara".to_string())]
        ]
    );
}

#[test]
fn sql_executor_query_supports_like_predicates() {
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[
            ("id", vec!["1", "1", "1"]),
            ("name", vec!["Alfred", "Mallory", "Allison"]),
        ],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT name FROM users WHERE id = 1 AND name LIKE 'Al%'")
        .expect("LIKE predicate");
    assert_eq!(result.columns, vec!["name".to_string()]);
    assert_eq!(
        result.rows,
        vec![
            vec![Some("Alfred".to_string())],
            vec![Some("Allison".to_string())]
        ]
    );

    let ilike = executor
        .query("SELECT name FROM users WHERE id = 1 AND name ILIKE 'al%'")
        .expect("ILIKE predicate");
    assert_eq!(ilike.rows.len(), 2);
}

#[test]
fn sql_executor_query_handles_nulls() {
    let null_marker = "\u{0001}";
    let (handler, directory, _store) = build_table_with_rows(
        "users",
        &[("id", vec!["1", "1"]), ("name", vec![null_marker, "Bob"])],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let result = executor
        .query("SELECT name FROM users WHERE id = 1")
        .expect("null handling");
    assert_eq!(result.columns, vec!["name".to_string()]);
    assert_eq!(result.rows, vec![vec![None], vec![Some("Bob".to_string())]]);
}

#[test]
fn sql_executor_numeric_aggregates() {
    let null_marker = "\u{0001}";
    let (handler, directory, _store) = build_table_with_rows(
        "numbers",
        &[
            ("id", vec!["1", "1", "1", "1", "1"]),
            ("value", vec!["10", "20", null_marker, "30", "40"]),
        ],
        vec!["id".to_string()],
    );
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));

    let counts = executor
        .query("SELECT COUNT(*), COUNT(value), COUNT(DISTINCT value) FROM numbers WHERE id = 1")
        .expect("count aggregates");
    assert_eq!(
        counts.columns,
        vec![
            "COUNT(*)".to_string(),
            "COUNT(value)".to_string(),
            "COUNT(DISTINCT value)".to_string()
        ]
    );
    assert_eq!(
        counts.rows,
        vec![vec![
            Some("5".to_string()),
            Some("4".to_string()),
            Some("4".to_string())
        ]]
    );

    let sums = executor
        .query("SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM numbers WHERE id = 1")
        .expect("sum aggregates");
    assert_eq!(
        sums.rows,
        vec![vec![
            Some("100".to_string()),
            Some("25".to_string()),
            Some("10".to_string()),
            Some("40".to_string()),
        ]]
    );

    let variance = executor
        .query("SELECT VARIANCE(value), STDDEV(value) FROM numbers WHERE id = 1")
        .expect("variance/stddev");
    assert_eq!(
        variance.rows,
        vec![vec![Some("125".to_string()), Some("11.18034".to_string())]]
    );

    let percentile = executor
        .query("SELECT percentile_cont(0.5, value) FROM numbers WHERE id = 1")
        .expect("percentile");
    assert_eq!(percentile.rows, vec![vec![Some("25".to_string())]]);

    let filtered = executor
        .query("SELECT SUM(CASE WHEN value > 20 THEN 1 ELSE 0 END) FROM numbers WHERE id = 1")
        .expect("filtered sum");
    println!("filtered rows: {:?}", filtered.rows);
    assert_eq!(filtered.rows, vec![vec![Some("2".to_string())]]);

    let count_filtered = executor
        .query("SELECT COUNT(*) FROM numbers WHERE id = 1 AND value > 20")
        .expect("count filtered");
    println!("count filtered: {:?}", count_filtered.rows);
    assert_eq!(count_filtered.rows, vec![vec![Some("2".to_string())]]);

    let null_count = executor
        .query("SELECT COUNT(*) - COUNT(value) FROM numbers WHERE id = 1")
        .expect("null count");
    assert_eq!(null_count.rows, vec![vec![Some("1".to_string())]]);

    let rounded = executor
        .query("SELECT ROUND(AVG(value), 1) FROM numbers WHERE id = 1")
        .expect("rounded avg");
    assert_eq!(rounded.rows, vec![vec![Some("25".to_string())]]);

    let width_bucket_avg = executor
        .query("SELECT AVG(width_bucket(value, 0, 50, 5)) FROM numbers WHERE id = 1")
        .expect("width bucket");
    assert_eq!(width_bucket_avg.rows, vec![vec![Some("3.5".to_string())]]);

    let empty = executor
        .query("SELECT COUNT(*) FROM numbers WHERE id = 2")
        .expect("empty aggregate");
    assert_eq!(empty.rows, vec![vec![Some("0".to_string())]]);
}

fn assert_rows_sorted(rows: &[Vec<String>], sort_indices: &[usize]) {
    if rows.len() <= 1 {
        return;
    }

    for window in rows.windows(2) {
        let left = &window[0];
        let right = &window[1];
        let mut decided = false;
        for &idx in sort_indices {
            let cmp = compare_strs(&left[idx], &right[idx]);
            if cmp == Ordering::Greater {
                panic!(
                    "rows not sorted: left={:?}, right={:?}, sort_indices={sort_indices:?}",
                    left, right
                );
            } else if cmp == Ordering::Less {
                decided = true;
                break;
            }
        }
        if !decided {
            // All sort keys equal; order is stable so nothing to assert.
        }
    }
}

fn compare_strs(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(l), Ok(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}

#[test]
fn sql_executor_end_to_end_sorted_insert() {
    let (executor, handler, directory, store) = build_sql_executor();

    executor
        .execute("CREATE TABLE accounts (id TEXT, score TEXT) ORDER BY score")
        .expect("create table succeeds");

    executor
        .execute("INSERT INTO accounts (id, score) VALUES ('u1', '50'), ('u2', '20'), ('u3', '30')")
        .expect("batched insert succeeds");

    executor
        .execute("INSERT INTO accounts (id, score) VALUES ('u4', '25')")
        .expect("single insert succeeds");

    executor
        .execute("UPDATE accounts SET id = 'u2b' WHERE score = '20'")
        .expect("update non-sort column succeeds");

    executor
        .execute("UPDATE accounts SET score = '60' WHERE score = '25'")
        .expect("update sort column succeeds");

    executor
        .execute("DELETE FROM accounts WHERE score = '30'")
        .expect("delete succeeds");

    let column_names = ["id", "score"];
    let rows = collect_table_rows(&handler, &directory, "accounts", &column_names);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "accounts", &column_names, rows.len());

    assert_eq!(
        rows,
        vec![
            vec!["u2b".to_string(), "20".to_string()],
            vec!["u1".to_string(), "50".to_string()],
            vec!["u4".to_string(), "60".to_string()],
        ]
    );
}

#[test]
fn order_by_numeric_random_inserts_preserve_order_and_metadata() {
    let columns = vec![("score", vec!["10", "30"])];
    let (handler, directory, store) =
        build_table_with_rows("numbers", &columns, vec!["score".into()]);

    let inserts = ["25", "5", "40", "30", "-1", "17", "100"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "numbers", "score", value).expect("sorted insert");
        let values = collect_column_values(&handler, &directory, "numbers", "score");
        assert_rows_sorted(
            &values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(),
            &[0],
        );
        assert_metadata_for_columns(&store, "numbers", &["score"], values.len());
        let descriptor = directory
            .latest_in_table("numbers", "score")
            .expect("descriptor");
        assert_eq!(descriptor.entry_count as usize, values.len());
    }
}

#[test]
fn order_by_lexicographic_inserts_keep_sorted_strings() {
    let columns = vec![("word", vec!["delta", "omega"])];
    let (handler, directory, store) = build_table_with_rows("words", &columns, vec!["word".into()]);

    let inserts = ["alpha", "gamma", "beta", "epsilon", "eta", "alpha"];
    for value in inserts {
        upsert_data_into_table_column(&handler, "words", "word", value).expect("sorted insert");
    }

    let values = collect_column_values(&handler, &directory, "words", "word");
    assert_rows_sorted(
        &values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(),
        &[0],
    );
    assert_metadata_for_columns(&store, "words", &["word"], values.len());
    let descriptor = directory
        .latest_in_table("words", "word")
        .expect("descriptor");
    assert_eq!(descriptor.entry_count as usize, values.len());
}

#[test]
fn order_by_numeric_duplicates_update_prefix_counts() {
    let columns = vec![("value", vec!["0"])];
    let (handler, directory, store) =
        build_table_with_rows("metrics", &columns, vec!["value".into()]);

    let inserts = ["0", "0", "1", "-5", "3", "2", "2", "-5", "4", "3", "3", "1"];

    for (idx, value) in inserts.iter().enumerate() {
        upsert_data_into_table_column(&handler, "metrics", "value", value).expect("sorted insert");
        let values = collect_column_values(&handler, &directory, "metrics", "value");
        assert_rows_sorted(
            &values.iter().map(|v| vec![v.clone()]).collect::<Vec<_>>(),
            &[0],
        );
        assert_metadata_for_columns(&store, "metrics", &["value"], values.len());
        let descriptor = directory
            .latest_in_table("metrics", "value")
            .expect("descriptor");
        assert_eq!(
            descriptor.entry_count as usize,
            values.len(),
            "entry count mismatch after insert {idx} ({value})"
        );
    }
}

#[test]
fn composite_order_by_inserts_are_sorted() {
    let columns = vec![
        ("country", vec!["CA", "US", "US"]),
        ("city", vec!["Toronto", "Austin", "Seattle"]),
        ("population", vec!["2.7", "1.0", "0.75"]),
    ];

    let (handler, directory, store) =
        build_table_with_rows("cities", &columns, vec!["country".into(), "city".into()]);

    let inserts = vec![
        vec![
            ("country", "US"),
            ("city", "Boston"),
            ("population", "0.65"),
        ],
        vec![
            ("country", "CA"),
            ("city", "Calgary"),
            ("population", "1.2"),
        ],
        vec![
            ("country", "US"),
            ("city", "Atlanta"),
            ("population", "0.5"),
        ],
        vec![
            ("country", "CA"),
            ("city", "Halifax"),
            ("population", "0.4"),
        ],
    ];

    for row in inserts {
        insert_sorted_row(&handler, "cities", &row).expect("sorted row insert");
    }

    let column_names = ["country", "city", "population"];
    let rows = collect_table_rows(&handler, &directory, "cities", &column_names);
    assert_rows_sorted(&rows, &[0, 1]);
    assert_metadata_for_columns(&store, "cities", &column_names, rows.len());
}

#[test]
fn composite_order_by_numeric_then_lexicographic() {
    let columns = vec![
        ("score", vec!["1", "2", "2"]),
        ("label", vec!["alpha", "beta", "delta"]),
        ("note", vec!["A", "B", "D"]),
    ];

    let (handler, directory, store) =
        build_table_with_rows("rankings", &columns, vec!["score".into(), "label".into()]);

    let inserts = vec![
        vec![("score", "1"), ("label", "aardvark"), ("note", "AA")],
        vec![("score", "3"), ("label", "gamma"), ("note", "G")],
        vec![("score", "2"), ("label", "charlie"), ("note", "C")],
        vec![("score", "2"), ("label", "beta"), ("note", "B2")],
        vec![("score", "0"), ("label", "omega"), ("note", "Z")],
    ];

    for row in inserts {
        insert_sorted_row(&handler, "rankings", &row).expect("sorted row insert");
    }

    let column_names = ["score", "label", "note"];
    let rows = collect_table_rows(&handler, &directory, "rankings", &column_names);
    assert_rows_sorted(&rows, &[0, 1]);
    assert_metadata_for_columns(&store, "rankings", &column_names, rows.len());
}

#[test]
fn long_end_to_end_single_column_order_by_with_updates_and_deletes() {
    let (executor, handler, directory, store) = build_sql_executor();

    // Step 1: Create table with single-column ORDER BY
    executor
        .execute("CREATE TABLE products (id TEXT, price TEXT) ORDER BY price")
        .expect("create table");

    // Step 2: Insert initial data
    executor
        .execute(
            "INSERT INTO products (id, price) VALUES ('p1', '100'), ('p2', '50'), ('p3', '75')",
        )
        .expect("initial insert");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 3);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 3);
    assert_eq!(
        rows,
        vec![vec!["p2", "50"], vec!["p3", "75"], vec!["p1", "100"],]
    );

    // Step 3: Insert more items (various positions)
    executor
        .execute("INSERT INTO products (id, price) VALUES ('p4', '25')")
        .expect("insert at beginning");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 4);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[0], vec!["p4", "25"]);

    executor
        .execute("INSERT INTO products (id, price) VALUES ('p5', '200')")
        .expect("insert at end");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 5);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[4], vec!["p5", "200"]);

    executor
        .execute("INSERT INTO products (id, price) VALUES ('p6', '60')")
        .expect("insert in middle");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 6);

    // Step 4: Update non-ORDER BY column (should be in-place)
    executor
        .execute("UPDATE products SET id = 'p2_updated' WHERE price = '50'")
        .expect("update non-sort column");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[1], vec!["p2_updated", "50"]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 6);

    // Step 5: Update ORDER BY column (should delete+reinsert)
    executor
        .execute("UPDATE products SET price = '5' WHERE price = '25'")
        .expect("update sort column to smaller value");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[0], vec!["p4", "5"]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 6);

    executor
        .execute("UPDATE products SET price = '300' WHERE price = '200'")
        .expect("update sort column to larger value");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[5], vec!["p5", "300"]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 6);

    // Step 6: Delete from various positions
    executor
        .execute("DELETE FROM products WHERE price = '5'")
        .expect("delete first item");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 5);
    assert_rows_sorted(&rows, &[1]);
    assert_eq!(rows[0], vec!["p2_updated", "50"]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 5);

    executor
        .execute("DELETE FROM products WHERE price = '300'")
        .expect("delete last item");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 4);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 4);

    executor
        .execute("DELETE FROM products WHERE price = '75'")
        .expect("delete middle item");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 3);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 3);

    // Step 7: Insert duplicates
    executor
        .execute("INSERT INTO products (id, price) VALUES ('p7', '50')")
        .expect("insert duplicate price");

    let rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_eq!(rows.len(), 4);
    assert_rows_sorted(&rows, &[1]);
    assert_metadata_for_columns(&store, "products", &["id", "price"], 4);

    // Final verification
    let final_rows = collect_table_rows(&handler, &directory, "products", &["id", "price"]);
    assert_rows_sorted(&final_rows, &[1]);
    for (i, row) in final_rows.iter().enumerate() {
        if i > 0 {
            let prev_price: f64 = final_rows[i - 1][1].parse().expect("numeric price");
            let curr_price: f64 = row[1].parse().expect("numeric price");
            assert!(prev_price <= curr_price, "prices not sorted at index {i}");
        }
    }
}

#[test]
fn long_end_to_end_multi_column_order_by_complex_operations() {
    let (executor, handler, directory, store) = build_sql_executor();

    // Step 1: Create table with multi-column ORDER BY
    executor
        .execute("CREATE TABLE orders (order_id TEXT, customer TEXT, region TEXT, amount TEXT) ORDER BY (region, amount)")
        .expect("create table");

    // Step 2: Insert initial dataset
    executor
        .execute(
            "INSERT INTO orders (order_id, customer, region, amount) VALUES \
            ('o1', 'Alice', 'US', '100'), \
            ('o2', 'Bob', 'EU', '200'), \
            ('o3', 'Charlie', 'US', '50'), \
            ('o4', 'David', 'APAC', '150'), \
            ('o5', 'Eve', 'EU', '75')",
        )
        .expect("initial batch insert");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 5);
    assert_rows_sorted(&rows, &[2, 3]); // region index=2, amount index=3
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        5,
    );

    // Verify initial order: APAC < EU < US, and within each region by amount
    assert_eq!(rows[0][2], "APAC"); // David
    assert_eq!(rows[1][2], "EU"); // Eve (75)
    assert_eq!(rows[2][2], "EU"); // Bob (200)
    assert_eq!(rows[3][2], "US"); // Charlie (50)
    assert_eq!(rows[4][2], "US"); // Alice (100)

    // Step 3: Insert items that go to different positions
    executor
        .execute("INSERT INTO orders (order_id, customer, region, amount) VALUES ('o6', 'Frank', 'APAC', '50')")
        .expect("insert APAC (first region)");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[2, 3]);
    assert_eq!(rows[0][0], "o6"); // Frank, APAC, 50 - smallest in APAC
    assert_eq!(rows[1][0], "o4"); // David, APAC, 150

    executor
        .execute("INSERT INTO orders (order_id, customer, region, amount) VALUES ('o7', 'Grace', 'US', '60')")
        .expect("insert US middle");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 7);
    assert_rows_sorted(&rows, &[2, 3]);
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        7,
    );

    // Step 4: Update non-ORDER BY columns (must provide all ORDER BY columns in WHERE)
    executor
        .execute(
            "UPDATE orders SET customer = 'Alice_Updated' WHERE region = 'US' AND amount = '100'",
        )
        .expect("update non-sort column");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_rows_sorted(&rows, &[2, 3]);
    let alice_row = rows.iter().find(|r| r[0] == "o1").expect("find alice");
    assert_eq!(alice_row[1], "Alice_Updated");
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        7,
    );

    // Step 5: Update ORDER BY column (region) - should reposition
    executor
        .execute("UPDATE orders SET region = 'EU' WHERE region = 'APAC' AND amount = '150'")
        .expect("update region - changes sort position");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 7);
    assert_rows_sorted(&rows, &[2, 3]);

    // David (o4) should now be in EU section with amount 150
    let david_row = rows.iter().find(|r| r[0] == "o4").expect("find david");
    assert_eq!(david_row[2], "EU");
    assert_eq!(david_row[3], "150");

    // Only Frank should be in APAC now
    let apac_rows: Vec<_> = rows.iter().filter(|r| r[2] == "APAC").collect();
    assert_eq!(apac_rows.len(), 1);
    assert_eq!(apac_rows[0][0], "o6"); // Frank
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        7,
    );

    // Step 6: Update both ORDER BY columns
    executor
        .execute("UPDATE orders SET region = 'APAC', amount = '300' WHERE region = 'EU' AND amount = '200'")
        .expect("update both sort columns");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 7);
    assert_rows_sorted(&rows, &[2, 3]);

    let bob_row = rows.iter().find(|r| r[0] == "o2").expect("find bob");
    assert_eq!(bob_row[2], "APAC");
    assert_eq!(bob_row[3], "300");

    // Bob should be last in APAC (highest amount)
    let apac_rows: Vec<_> = rows.iter().filter(|r| r[2] == "APAC").collect();
    assert_eq!(apac_rows.len(), 2);
    assert_eq!(apac_rows[1][0], "o2"); // Bob last in APAC
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        7,
    );

    // Step 7: Delete items from different regions (must provide all ORDER BY columns)
    executor
        .execute("DELETE FROM orders WHERE region = 'APAC' AND amount = '50'")
        .expect("delete from APAC");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[2, 3]);
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        6,
    );

    executor
        .execute("DELETE FROM orders WHERE region = 'EU' AND amount = '75'")
        .expect("delete from EU");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 5);
    assert_rows_sorted(&rows, &[2, 3]);
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        5,
    );

    // Step 8: Insert more items with duplicates
    executor
        .execute(
            "INSERT INTO orders (order_id, customer, region, amount) VALUES \
            ('o8', 'Henry', 'US', '50'), \
            ('o9', 'Iris', 'US', '50'), \
            ('o10', 'Jack', 'EU', '150')",
        )
        .expect("insert duplicates");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "orders",
        &["order_id", "customer", "region", "amount"],
    );
    assert_eq!(rows.len(), 8);
    assert_rows_sorted(&rows, &[2, 3]);
    assert_metadata_for_columns(
        &store,
        "orders",
        &["order_id", "customer", "region", "amount"],
        8,
    );

    // Final verification: check region grouping and amount ordering within regions
    let mut last_region = "";
    let mut last_amount_in_region = -1.0;

    for row in &rows {
        let region = &row[2];
        let amount: f64 = row[3].parse().expect("numeric amount");

        if region != last_region {
            last_region = region;
            last_amount_in_region = -1.0;
        }

        assert!(
            amount >= last_amount_in_region,
            "amounts not sorted within region {region}: {amount} < {last_amount_in_region}"
        );
        last_amount_in_region = amount;
    }
}

#[test]
fn long_end_to_end_empty_strings_and_mixed_types() {
    let (executor, handler, directory, store) = build_sql_executor();

    executor
        .execute("CREATE TABLE mixed (id TEXT, val1 TEXT, val2 TEXT) ORDER BY (val1, val2)")
        .expect("create table");

    // Insert with some empty strings
    executor
        .execute("INSERT INTO mixed (id, val1, val2) VALUES ('r1', '10', 'apple')")
        .expect("insert");

    executor
        .execute("INSERT INTO mixed (id, val1, val2) VALUES ('r2', '10', 'banana')")
        .expect("insert");

    executor
        .execute("INSERT INTO mixed (id, val1) VALUES ('r3', '5')")
        .expect("insert with missing val2 - defaults to empty");

    let rows = collect_table_rows(&handler, &directory, "mixed", &["id", "val1", "val2"]);
    assert_eq!(rows.len(), 3);
    assert_rows_sorted(&rows, &[1, 2]);

    // r3 should be first (val1=5 < 10)
    assert_eq!(rows[0][0], "r3");
    assert_eq!(rows[0][2], ""); // empty val2

    // r1 and r2 both have val1=10, sorted by val2
    assert_eq!(rows[1][0], "r1"); // apple < banana
    assert_eq!(rows[2][0], "r2");

    // Insert item with empty val1 (should sort first)
    executor
        .execute("INSERT INTO mixed (id, val2) VALUES ('r4', 'zebra')")
        .expect("insert with empty val1");

    let rows = collect_table_rows(&handler, &directory, "mixed", &["id", "val1", "val2"]);
    assert_eq!(rows.len(), 4);
    assert_rows_sorted(&rows, &[1, 2]);
    assert_eq!(rows[0][0], "r4"); // empty val1 sorts first
    assert_eq!(rows[0][1], "");
    assert_metadata_for_columns(&store, "mixed", &["id", "val1", "val2"], 4);

    // Insert numeric-looking strings
    executor
        .execute("INSERT INTO mixed (id, val1, val2) VALUES ('r5', '2', 'delta')")
        .expect("insert");

    executor
        .execute("INSERT INTO mixed (id, val1, val2) VALUES ('r6', '100', 'echo')")
        .expect("insert");

    let rows = collect_table_rows(&handler, &directory, "mixed", &["id", "val1", "val2"]);
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1, 2]);

    // Verify numeric comparison works: 2 < 5 < 10 < 100
    let numeric_rows: Vec<_> = rows.iter().filter(|r| !r[1].is_empty()).collect();
    let vals: Vec<f64> = numeric_rows.iter().map(|r| r[1].parse().unwrap()).collect();
    assert_eq!(vals, vec![2.0, 5.0, 10.0, 10.0, 100.0]);

    // Update to create more empty scenarios (must provide all ORDER BY columns in WHERE)
    executor
        .execute("UPDATE mixed SET val2 = '' WHERE val1 = '10' AND val2 = 'apple'")
        .expect("clear val2");

    let rows = collect_table_rows(&handler, &directory, "mixed", &["id", "val1", "val2"]);
    assert_rows_sorted(&rows, &[1, 2]);
    assert_metadata_for_columns(&store, "mixed", &["id", "val1", "val2"], 6);

    // Delete items with empty values (must provide all ORDER BY columns)
    executor
        .execute("DELETE FROM mixed WHERE val1 = '' AND val2 = 'zebra'")
        .expect("delete empty val1");

    let rows = collect_table_rows(&handler, &directory, "mixed", &["id", "val1", "val2"]);
    assert_eq!(rows.len(), 5);
    assert_rows_sorted(&rows, &[1, 2]);
    assert_metadata_for_columns(&store, "mixed", &["id", "val1", "val2"], 5);
}

#[test]
fn long_end_to_end_stress_many_inserts_updates_deletes() {
    let (executor, handler, directory, store) = build_sql_executor();

    executor
        .execute("CREATE TABLE stress (id TEXT, value TEXT) ORDER BY value")
        .expect("create table");

    // Insert 50 items
    for i in 0..50 {
        let value = (i * 2) % 100; // Create some duplicates
        executor
            .execute(&format!(
                "INSERT INTO stress (id, value) VALUES ('id{i}', '{value}')"
            ))
            .expect(&format!("insert {i}"));

        if i % 10 == 9 {
            // Verify order every 10 inserts
            let rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
            assert_rows_sorted(&rows, &[1]);
            assert_metadata_for_columns(&store, "stress", &["id", "value"], rows.len());
        }
    }

    let rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
    assert_eq!(rows.len(), 50);
    assert_rows_sorted(&rows, &[1]);

    // Update 20 items to change their sort position (must provide ORDER BY column in WHERE)
    for i in 0..20 {
        let new_value = 1000 + i; // Move to end
        let old_value = (i * 2) % 100; // Original value calculation
        executor
            .execute(&format!(
                "UPDATE stress SET value = '{new_value}' WHERE value = '{old_value}'"
            ))
            .expect(&format!("update {i}"));

        if i % 5 == 4 {
            let rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
            assert_rows_sorted(&rows, &[1]);
            assert_metadata_for_columns(&store, "stress", &["id", "value"], 50);
        }
    }

    let rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
    assert_eq!(rows.len(), 50);
    assert_rows_sorted(&rows, &[1]);

    // Delete 30 items (must provide ORDER BY column in WHERE)
    for i in 10..40 {
        let value = if i < 20 {
            1000 + i // These were updated earlier
        } else {
            (i * 2) % 100 // Original value
        };
        executor
            .execute(&format!("DELETE FROM stress WHERE value = '{value}'"))
            .expect(&format!("delete {i}"));

        if i % 5 == 4 {
            let rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
            assert_rows_sorted(&rows, &[1]);
            assert_metadata_for_columns(&store, "stress", &["id", "value"], rows.len());
        }
    }

    let final_rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
    assert_eq!(final_rows.len(), 20);
    assert_rows_sorted(&final_rows, &[1]);
    assert_metadata_for_columns(&store, "stress", &["id", "value"], 20);

    // Insert more items in the middle
    for i in 0..10 {
        executor
            .execute(&format!(
                "INSERT INTO stress (id, value) VALUES ('new{i}', '{}')",
                i * 5
            ))
            .expect(&format!("new insert {i}"));
    }

    let final_rows = collect_table_rows(&handler, &directory, "stress", &["id", "value"]);
    assert_eq!(final_rows.len(), 30);
    assert_rows_sorted(&final_rows, &[1]);
    assert_metadata_for_columns(&store, "stress", &["id", "value"], 30);
}

#[test]
fn long_end_to_end_three_column_order_by() {
    let (executor, handler, directory, store) = build_sql_executor();

    executor
        .execute("CREATE TABLE events (id TEXT, year TEXT, month TEXT, day TEXT, event TEXT) ORDER BY (year, month, day)")
        .expect("create table");

    // Insert events in various orders
    let events = vec![
        ("e1", "2024", "03", "15", "Spring"),
        ("e2", "2023", "12", "25", "Christmas"),
        ("e3", "2024", "01", "01", "New Year"),
        ("e4", "2024", "03", "10", "Early March"),
        ("e5", "2023", "12", "31", "NYE"),
        ("e6", "2024", "03", "10", "Also Early March"),
    ];

    for (id, year, month, day, event) in events {
        executor
            .execute(&format!(
                "INSERT INTO events (id, year, month, day, event) VALUES ('{id}', '{year}', '{month}', '{day}', '{event}')"
            ))
            .expect("insert event");
    }

    let rows = collect_table_rows(
        &handler,
        &directory,
        "events",
        &["id", "year", "month", "day", "event"],
    );
    assert_eq!(rows.len(), 6);
    assert_rows_sorted(&rows, &[1, 2, 3]); // year, month, day
    assert_metadata_for_columns(
        &store,
        "events",
        &["id", "year", "month", "day", "event"],
        6,
    );

    // Verify sort order
    assert_eq!(rows[0][0], "e2"); // 2023-12-25
    assert_eq!(rows[1][0], "e5"); // 2023-12-31
    assert_eq!(rows[2][0], "e3"); // 2024-01-01
    // e4 and e6 both 2024-03-10 (stable order)
    assert_eq!(rows[5][0], "e1"); // 2024-03-15

    // Update year - moves to different year group (must provide all ORDER BY columns in WHERE)
    executor
        .execute(
            "UPDATE events SET year = '2025' WHERE year = '2024' AND month = '03' AND day = '15'",
        )
        .expect("update year");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "events",
        &["id", "year", "month", "day", "event"],
    );
    assert_rows_sorted(&rows, &[1, 2, 3]);
    assert_eq!(rows[5][0], "e1"); // Now last (2025)

    // Update month within same year (must provide all ORDER BY columns in WHERE)
    executor
        .execute(
            "UPDATE events SET month = '02' WHERE year = '2024' AND month = '01' AND day = '01'",
        )
        .expect("update month");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "events",
        &["id", "year", "month", "day", "event"],
    );
    assert_rows_sorted(&rows, &[1, 2, 3]);
    assert_metadata_for_columns(
        &store,
        "events",
        &["id", "year", "month", "day", "event"],
        6,
    );

    // Delete and verify (must provide all ORDER BY columns - delete each 2023 event individually)
    executor
        .execute("DELETE FROM events WHERE year = '2023' AND month = '12' AND day = '25'")
        .expect("delete 2023 event 1");
    executor
        .execute("DELETE FROM events WHERE year = '2023' AND month = '12' AND day = '31'")
        .expect("delete 2023 event 2");

    let rows = collect_table_rows(
        &handler,
        &directory,
        "events",
        &["id", "year", "month", "day", "event"],
    );
    assert_eq!(rows.len(), 4);
    assert_rows_sorted(&rows, &[1, 2, 3]);
    assert_metadata_for_columns(
        &store,
        "events",
        &["id", "year", "month", "day", "event"],
        4,
    );

    // All remaining should be 2024 or 2025
    for row in &rows {
        let year: i32 = row[1].parse().unwrap();
        assert!(year >= 2024);
    }
}
