use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn date_trunc_second() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            DATE_TRUNC('second', created_at) AS trunc_second \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('second', CAST(created_at AS TIMESTAMP))) AS trunc_second \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn date_trunc_minute() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            DATE_TRUNC('minute', created_at) AS trunc_minute \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('minute', CAST(created_at AS TIMESTAMP))) AS trunc_minute \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn date_trunc_hour() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            DATE_TRUNC('hour', created_at) AS trunc_hour \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('hour', CAST(created_at AS TIMESTAMP))) AS trunc_hour \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn date_trunc_day() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            DATE_TRUNC('day', created_at) AS trunc_day \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('day', CAST(created_at AS TIMESTAMP))) AS trunc_day \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn date_trunc_week() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            DATE_TRUNC('week', created_at) AS trunc_week \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', DATE_TRUNC('week', CAST(created_at AS TIMESTAMP))) AS trunc_week \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn time_bucket_with_origin() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            created_at, \
            TIME_BUCKET(3600, created_at, '2020-01-01 00:00:00') AS hour_bucket_with_origin \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            created_at, \
            strftime('%Y-%m-%d %H:%M:%S', TIME_BUCKET(INTERVAL '1 hour', CAST(created_at AS TIMESTAMP), TIMESTAMP '2020-01-01 00:00:00')) AS hour_bucket_with_origin \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn time_bucket_numeric_with_origin() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            quantity, \
            TIME_BUCKET(100.0, quantity, 50.0) AS qty_bucket_origin \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let duckdb_sql = format!(
        "SELECT \
            id, \
            quantity, \
            FLOOR((quantity - 50.0) / 100.0) * 100.0 + 50.0 AS qty_bucket_origin \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}
