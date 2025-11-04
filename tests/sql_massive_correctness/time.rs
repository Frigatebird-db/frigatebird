use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn temporal_functions_align() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            tenant, \
            DATE_TRUNC('day', created_at) AS day_bucket, \
            TIME_BUCKET(3600, created_at) AS hour_bucket, \
            COUNT(*) AS rows_seen, \
            SUM(quantity) AS qty_sum \
         FROM {table} \
         WHERE tenant IN ('alpha', 'beta') \
         GROUP BY 1, 2, 3 \
         ORDER BY 1, 2, 3 \
         LIMIT 100",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            tenant, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('day', CAST(created_at AS TIMESTAMP))) AS day_bucket, \
            strftime('%Y-%m-%d %H:%M:%S', TIME_BUCKET(INTERVAL '1 hour', CAST(created_at AS TIMESTAMP))) AS hour_bucket, \
            COUNT(*) AS rows_seen, \
            SUM(quantity) AS qty_sum \
         FROM {table} \
         WHERE tenant IN ('alpha', 'beta') \
         GROUP BY 1, 2, 3 \
         ORDER BY 1, 2, 3 \
         LIMIT 100",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn time_bucket_handles_numeric_inputs() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            tenant, \
            TIME_BUCKET(100.0, quantity) AS quantity_bucket, \
            COUNT(*) AS samples \
         FROM {table} \
         WHERE tenant IN ('gamma', 'delta') \
         GROUP BY 1, 2 \
         ORDER BY 1, 2"
    );

    let duckdb_sql = format!(
        "SELECT \
            tenant, \
            FLOOR(quantity / 100.0) * 100.0 AS quantity_bucket, \
            COUNT(*) AS samples \
         FROM {table} \
         WHERE tenant IN ('gamma', 'delta') \
         GROUP BY 1, 2 \
         ORDER BY 1, 2",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}
