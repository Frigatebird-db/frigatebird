use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SelectResult, SqlExecutor};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
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

#[derive(Clone)]
struct MetricRow {
    user_id: String,
    ts_numeric: i32,
    ts_text: String,
    category: String,
    region: String,
    value: i64,
    score: Option<i64>,
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
        let ts_numeric = 10_000 + i;
        let ts_text = format!("{:05}", ts_numeric);
        let category = categories[(i as usize) % categories.len()].to_string();
        let region = regions[(i as usize) % regions.len()].to_string();
        let value = (i as i64 * 3) + ((i % 5) as i64) + 10;
        let score = if i % 11 == 0 {
            None
        } else {
            Some((i % 7) as i64)
        };

        let score_sql = match score {
            Some(s) => format!("'{}'", s),
            None => "NULL".to_string(),
        };

        let insert = format!(
            "INSERT INTO metrics (user_id, ts, category, value, region, score) VALUES ('{}', '{}', '{}', '{}', '{}', {})",
            user_id, ts_text, category, value, region, score_sql
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
    assert_eq!(count_result.rows()[0][0], Some("500".to_string()));

    // ----- Arbitrary ORDER BY with Top-K -----
    let mut expected_order = rows.clone();
    expected_order.sort_by(|lhs, rhs| {
        let lhs_key = lhs.score.map(|s| lhs.value - s);
        let rhs_key = rhs.score.map(|s| rhs.value - s);
        match (lhs_key, rhs_key) {
            (Some(lhs_val), Some(rhs_val)) => rhs_val
                .cmp(&lhs_val)
                .then_with(|| lhs.user_id.cmp(&rhs.user_id))
                .then_with(|| lhs.ts_text.cmp(&rhs.ts_text)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => lhs
                .user_id
                .cmp(&rhs.user_id)
                .then_with(|| lhs.ts_text.cmp(&rhs.ts_text)),
        }
    });
    let expected_top_five: Vec<(String, String)> = expected_order
        .iter()
        .take(5)
        .map(|row| (row.user_id.clone(), row.ts_text.clone()))
        .collect();

    let order_query = "SELECT user_id, ts FROM metrics \
                       ORDER BY CASE WHEN score IS NULL THEN NULL ELSE (value - score) END \
                       DESC NULLS LAST, user_id, ts LIMIT 5";
    let order_result = executor
        .query(order_query)
        .expect("order by expression query");
    assert_eq!(order_result.row_count(), expected_top_five.len());
    for (row, expected) in order_result.rows().iter().zip(expected_top_five.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        assert_eq!(row[1].as_ref().unwrap(), &expected.1);
    }

    // ----- General GROUP BY + HAVING -----
    #[derive(Default)]
    struct GroupAcc {
        sum_value: i64,
        sum_score: i64,
        score_count: usize,
        count: usize,
    }
    let mut group_map: BTreeMap<(String, String), GroupAcc> = BTreeMap::new();
    for row in &rows {
        let key = (row.category.clone(), row.region.clone());
        let entry = group_map.entry(key).or_default();
        entry.sum_value += row.value;
        if let Some(score) = row.score {
            entry.sum_score += score;
            entry.score_count += 1;
        }
        entry.count += 1;
    }

    let mut expected_groups: Vec<(String, String, i64, i64, Option<f64>)> = Vec::new();
    for ((category, region), acc) in group_map {
        if acc.sum_value > 1_000 {
            let avg_score = if acc.score_count > 0 {
                Some(acc.sum_score as f64 / acc.score_count as f64)
            } else {
                None
            };
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
    assert_eq!(group_result.row_count(), expected_groups.len());
    for (row, expected) in group_result.rows().iter().zip(expected_groups.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        assert_eq!(row[1].as_ref().unwrap(), &expected.1);
        let total_value: i64 = row[2].as_ref().unwrap().parse().unwrap();
        assert_eq!(total_value, expected.2);
        let count: i64 = row[3].as_ref().unwrap().parse().unwrap();
        assert_eq!(count, expected.3);
        match expected.4 {
            Some(expected_avg) => {
                let avg_score: f64 = row[4].as_ref().unwrap().parse().unwrap();
                assert!(
                    (avg_score - expected_avg).abs() < 1e-6,
                    "avg_score mismatch: got {avg_score}, expected {expected_avg}"
                );
            }
            None => {
                assert!(
                    row[4].is_none(),
                    "expected NULL avg_score for group but got {:?}",
                    row[4]
                );
            }
        }
    }

    // ----- Window functions -----
    let mut alpha_rows: Vec<&MetricRow> =
        rows.iter().filter(|row| row.category == "alpha").collect();
    alpha_rows.sort_by_key(|row| row.ts_numeric);

    let window_limit = 25;
    let mut running_sum = 0_i64;
    let mut seen_non_null = false;
    let mut expected_window: Vec<(String, i64, Option<i64>)> = Vec::new();
    for (idx, row) in alpha_rows.iter().take(window_limit).enumerate() {
        if let Some(score) = row.score {
            running_sum += score;
            seen_non_null = true;
            expected_window.push((row.ts_text.clone(), (idx + 1) as i64, Some(running_sum)));
        } else if seen_non_null {
            expected_window.push((row.ts_text.clone(), (idx + 1) as i64, Some(running_sum)));
        } else {
            expected_window.push((row.ts_text.clone(), (idx + 1) as i64, None));
        }
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
    assert_eq!(window_result.row_count(), expected_window.len());
    for (row, expected) in window_result.rows().iter().zip(expected_window.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        let rn: i64 = row[1].as_ref().unwrap().parse().unwrap();
        assert_eq!(rn, expected.1);
        match expected.2 {
            Some(expected_running) => {
                let running: i64 = row[2].as_ref().unwrap().parse().unwrap();
                assert_eq!(running, expected_running);
            }
            None => {
                assert!(
                    row[2].is_none(),
                    "expected NULL running sum but got {:?}",
                    row[2]
                );
            }
        }
    }

    // Note: The previous spot-check was flawed. Window functions operate on the result set
    // AFTER the WHERE clause is applied. When filtering to a single timestamp, the window
    // function only sees that one row, not all preceding rows. The main verification loop
    // above (lines 231-247) already validates all window function results correctly.

    // ----- QUALIFY with ROW_NUMBER -----
    let mut beta_rows: Vec<&MetricRow> = rows.iter().filter(|row| row.category == "beta").collect();
    beta_rows.sort_by(|lhs, rhs| {
        rhs.value
            .cmp(&lhs.value)
            .then_with(|| lhs.ts_text.cmp(&rhs.ts_text))
    });
    let mut expected_beta: Vec<(String, String)> = beta_rows
        .iter()
        .take(3)
        .map(|row| (row.user_id.clone(), row.ts_text.clone()))
        .collect();
    expected_beta.sort_by(|lhs, rhs| lhs.1.cmp(&rhs.1));

    let qualify_query = "SELECT user_id, ts FROM metrics \
                         WHERE category = 'beta' \
                         QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) <= 3 \
                         ORDER BY ts";
    let qualify_result = executor.query(qualify_query).expect("qualify query");
    assert_eq!(qualify_result.row_count(), expected_beta.len());
    for (row, expected) in qualify_result.rows().iter().zip(expected_beta.iter()) {
        assert_eq!(row[0].as_ref().unwrap(), &expected.0);
        assert_eq!(row[1].as_ref().unwrap(), &expected.1);
    }

    // ----- RANK and DENSE_RANK -----
    let mut gamma_rows_ts: Vec<&MetricRow> =
        rows.iter().filter(|row| row.category == "gamma").collect();
    gamma_rows_ts.sort_by_key(|row| row.ts_numeric);

    let mut gamma_by_value = gamma_rows_ts.clone();
    gamma_by_value.sort_by(|lhs, rhs| {
        rhs.value
            .cmp(&lhs.value)
            .then_with(|| lhs.ts_text.cmp(&rhs.ts_text))
    });
    let mut rank_map: HashMap<String, (i64, i64)> = HashMap::new();
    let mut current_rank = 1_i64;
    let mut dense_rank = 1_i64;
    for (idx, row) in gamma_by_value.iter().enumerate() {
        if idx > 0 && gamma_by_value[idx - 1].value != row.value {
            current_rank = (idx + 1) as i64;
            dense_rank += 1;
        }
        rank_map.insert(row.ts_text.clone(), (current_rank, dense_rank));
    }

    let rank_query = "SELECT ts, RANK() OVER (PARTITION BY category ORDER BY value DESC) AS rnk, \
                            DENSE_RANK() OVER (PARTITION BY category ORDER BY value DESC) AS drnk \
                      FROM metrics WHERE category = 'gamma' ORDER BY ts LIMIT 8";
    let rank_result = executor.query(rank_query).expect("rank window query");
    assert_eq!(rank_result.row_count(), 8);
    for (row, metric) in rank_result.rows().iter().zip(gamma_rows_ts.iter().take(8)) {
        let (expected_rank, expected_dense) = rank_map.get(&metric.ts_text).unwrap();
        assert_eq!(row[1].as_ref().unwrap(), &expected_rank.to_string());
        assert_eq!(row[2].as_ref().unwrap(), &expected_dense.to_string());
    }

    // ----- LAG / LEAD -----
    let mut alpha_rows_ts: Vec<&MetricRow> =
        rows.iter().filter(|row| row.category == "alpha").collect();
    alpha_rows_ts.sort_by_key(|row| row.ts_numeric);

    let lag_lead_query = "SELECT ts, value, \
                                 LAG(value) OVER (PARTITION BY category ORDER BY ts) AS prev_value, \
                                 LEAD(value, 2, 'missing') OVER (PARTITION BY category ORDER BY ts) AS next_value \
                          FROM metrics WHERE category = 'alpha' ORDER BY ts LIMIT 6";
    let lag_lead_result = executor
        .query(lag_lead_query)
        .expect("lag/lead window query");
    assert_eq!(lag_lead_result.row_count(), 6);
    for (idx, row) in lag_lead_result.rows().iter().enumerate() {
        let current = alpha_rows_ts[idx];
        let expected_prev = if idx > 0 {
            Some(alpha_rows_ts[idx - 1].value.to_string())
        } else {
            None
        };
        if let Some(prev) = expected_prev {
            assert_eq!(row[2].as_ref().unwrap(), &prev);
        } else {
            assert!(row[2].is_none());
        }

        let next_index = idx + 2;
        let expected_next = if next_index < alpha_rows_ts.len() {
            alpha_rows_ts[next_index].value.to_string()
        } else {
            "missing".to_string()
        };
        assert_eq!(row[3].as_ref().unwrap(), &expected_next);
        assert_eq!(row[0].as_ref().unwrap(), &current.ts_text);
    }

    // ----- Sliding SUM with bounded frame -----
    let sliding_query = "SELECT ts, \
        SUM(score) OVER (PARTITION BY category ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running \
        FROM metrics WHERE category = 'alpha' ORDER BY ts LIMIT 8";
    let sliding_result = executor
        .query(sliding_query)
        .expect("sliding sum window query");
    assert_eq!(sliding_result.row_count(), 8);
    for (idx, row) in sliding_result.rows().iter().enumerate() {
        let current = alpha_rows_ts[idx];
        let start = idx.saturating_sub(2);
        let mut sum: i64 = 0;
        let mut non_null = 0;
        for candidate in &alpha_rows_ts[start..=idx] {
            if let Some(score) = candidate.score {
                sum += score;
                non_null += 1;
            }
        }
        if non_null > 0 {
            assert_eq!(row[1].as_ref().unwrap(), &sum.to_string());
        } else {
            assert!(row[1].is_none());
        }
        assert_eq!(row[0].as_ref().unwrap(), &current.ts_text);
    }
}
