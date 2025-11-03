use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::SqlExecutor;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

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
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));
    (executor, handler, directory)
}

#[derive(Clone)]
struct MetricRow {
    user_id: String,
    ts_numeric: i32,
    ts_text: String,
    category: String,
    region: String,
    value: i64,
    score: i64,
}

#[test]
fn test_sql_feature_matrix_with_large_dataset() {
    let (executor, _, _) = setup_executor();

    executor
        .execute("CREATE TABLE metrics (user_id TEXT, ts TEXT, category TEXT, value TEXT, region TEXT, score TEXT) ORDER BY user_id")
        .expect("create metrics table");

    let categories = ["alpha", "beta", "gamma", "delta"];
    let regions = ["north", "south", "east", "west"];

    let mut rows: Vec<MetricRow> = Vec::with_capacity(500);

    for i in 0..500 {
        let user_id = format!("user{:02}", i % 37);
        let ts_numeric = 10_000 + i as i32;
        let ts_text = format!("{:05}", ts_numeric);
        let category = categories[(i as usize) % categories.len()].to_string();
        let region = regions[(i as usize) % regions.len()].to_string();
        let value = (i as i64 * 3) + ((i % 5) as i64) + 10;
        let score = (i % 7) as i64;

        let insert = format!(
            "INSERT INTO metrics (user_id, ts, category, value, region, score) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')",
            user_id, ts_text, category, value, region, score
        );
        executor.execute(&insert).expect("insert metric row");

        rows.push(MetricRow {
            user_id,
            ts_numeric,
            ts_text,
            category,
            region,
            value,
            score,
        });
    }

    // Sanity check that all rows landed.
    let count_result = executor
        .query("SELECT COUNT(*) FROM metrics")
        .expect("count metrics rows");
    assert_eq!(count_result.rows[0][0], Some("500".to_string()));

    // ----- Arbitrary ORDER BY with Top-K -----
    let mut expected_order = rows.clone();
    expected_order.sort_by(|lhs, rhs| {
        let lhs_key = lhs.value - lhs.score;
        let rhs_key = rhs.value - rhs.score;
        rhs_key
            .cmp(&lhs_key)
            .then_with(|| lhs.user_id.cmp(&rhs.user_id))
            .then_with(|| lhs.ts_text.cmp(&rhs.ts_text))
    });
    let expected_top_five: Vec<(String, String)> = expected_order
        .iter()
        .take(5)
        .map(|row| (row.user_id.clone(), row.ts_text.clone()))
        .collect();

    let order_query =
        "SELECT user_id, ts FROM metrics ORDER BY (value - score) DESC, user_id, ts LIMIT 5";
    let order_result = executor
        .query(order_query)
        .expect("order by expression query");
    assert_eq!(order_result.rows.len(), expected_top_five.len());
    for (row, expected) in order_result.rows.iter().zip(expected_top_five.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        assert_eq!(row[1].as_ref().unwrap(), &expected.1);
    }

    // ----- General GROUP BY + HAVING -----
    #[derive(Default)]
    struct GroupAcc {
        sum_value: i64,
        sum_score: i64,
        count: usize,
    }
    let mut group_map: BTreeMap<(String, String), GroupAcc> = BTreeMap::new();
    for row in &rows {
        let key = (row.category.clone(), row.region.clone());
        let entry = group_map.entry(key).or_default();
        entry.sum_value += row.value;
        entry.sum_score += row.score;
        entry.count += 1;
    }

    let mut expected_groups: Vec<(String, String, i64, i64, f64)> = Vec::new();
    for ((category, region), acc) in group_map {
        if acc.sum_value > 1_000 {
            let avg_score = acc.sum_score as f64 / acc.count as f64;
            expected_groups.push((category, region, acc.sum_value, acc.count as i64, avg_score));
        }
    }

    let group_query = "SELECT category, region, ROUND(SUM(value), 2) AS total_value, COUNT(*) AS cnt, ROUND(AVG(score), 2) AS avg_score \
                       FROM metrics \
                       GROUP BY category, region \
                       HAVING SUM(value) > 1000 \
                       ORDER BY category, region";
    let group_result = executor
        .query(group_query)
        .expect("group by + having query");
    assert_eq!(group_result.rows.len(), expected_groups.len());
    for (row, expected) in group_result.rows.iter().zip(expected_groups.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        assert_eq!(row[1].as_ref().unwrap(), &expected.1);
        let total_value: i64 = row[2].as_ref().unwrap().parse().unwrap();
        assert_eq!(total_value, expected.2);
        let count: i64 = row[3].as_ref().unwrap().parse().unwrap();
        assert_eq!(count, expected.3);
        let avg_score: f64 = row[4].as_ref().unwrap().parse().unwrap();
        assert!(
            (avg_score - expected.4).abs() < 1e-6,
            "avg_score mismatch: got {avg_score}, expected {}",
            expected.4
        );
    }

    // ----- Window functions -----
    let mut alpha_rows: Vec<&MetricRow> =
        rows.iter().filter(|row| row.category == "alpha").collect();
    alpha_rows.sort_by_key(|row| row.ts_numeric);

    let window_limit = 25;
    let mut running_sum = 0_i64;
    let mut expected_window: Vec<(String, i64, i64)> = Vec::new();
    for (idx, row) in alpha_rows.iter().take(window_limit).enumerate() {
        running_sum += row.score;
        expected_window.push((row.ts_text.clone(), (idx + 1) as i64, running_sum));
    }

    let window_query = format!(
        "SELECT ts, ROW_NUMBER() OVER (PARTITION BY category ORDER BY ts) AS rn, \
                SUM(score) OVER (PARTITION BY category ORDER BY ts ROWS UNBOUNDED PRECEDING) AS running \
         FROM metrics \
         WHERE category = 'alpha' \
         ORDER BY ts \
         LIMIT {window_limit}"
    );
    let window_result = executor
        .query(&window_query)
        .expect("window function query");
    assert_eq!(window_result.rows.len(), expected_window.len());
    for (row, expected) in window_result.rows.iter().zip(expected_window.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        let rn: i64 = row[1].as_ref().unwrap().parse().unwrap();
        assert_eq!(rn, expected.1);
        let running: i64 = row[2].as_ref().unwrap().parse().unwrap();
        assert_eq!(running, expected.2);
    }

    // Spot-check that window results align with raw dataset for later positions (edge behaviour).
    if let Some(last_expected) = expected_window.last() {
        let last_ts = &last_expected.0;
        let confirm_query = format!(
            "SELECT SUM(score) OVER (PARTITION BY category ORDER BY ts ROWS UNBOUNDED PRECEDING) \
             FROM metrics WHERE category = 'alpha' AND ts = '{last_ts}'"
        );
        let confirm_result = executor.query(&confirm_query).expect("confirm running sum");
        assert_eq!(confirm_result.rows.len(), 1);
        let running: i64 = confirm_result.rows[0][0].as_ref().unwrap().parse().unwrap();
        assert_eq!(running, last_expected.2);
    }
}
