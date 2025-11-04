use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn mathematical_functions_basic() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            ABS(discount - 0.25) AS abs_diff, \
            ROUND(price, 2) AS rounded_price, \
            CEIL(quantity / 10.0) AS ceil_qty, \
            FLOOR(quantity / 10.0) AS floor_qty \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn exponential_and_logarithmic_functions() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            EXP(discount) AS exp_disc, \
            LN(price) AS ln_price, \
            POWER(discount, 2) AS disc_squared \
         FROM {table} \
         WHERE id <= 100 AND price > 0 AND quantity > 0 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-5;
    options.float_rel_tol = 1e-5;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
#[ignore = "BUG: LOG function treats LOG as LN instead of LOG10 - returns 5.509 vs expected 2.393"]
fn logarithm_base_10_function() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            LOG(quantity) AS log10_qty, \
            LN(quantity) AS ln_qty \
         FROM {table} \
         WHERE id <= 100 AND quantity > 0 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    // BUG FOUND: Implementation treats LOG as natural logarithm (LN) instead of LOG10
    // Expected: LOG = LOG10, but getting LOG = LN
    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-5;
    options.float_rel_tol = 1e-5;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn power_function_variations() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            POWER(2, 3) AS const_power, \
            POWER(price, 0.5) AS sqrt_price, \
            POWER(quantity, 1.5) AS qty_power \
         FROM {table} \
         WHERE id <= 100 AND price > 0 AND quantity > 0 \
         ORDER BY id \
         LIMIT 40",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-5;
    options.float_rel_tol = 1e-5;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
#[ignore = "BUG: WIDTH_BUCKET has off-by-one error - returns bucket 11 for max value instead of bucket 10"]
fn width_bucket_function() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            WIDTH_BUCKET(price, 0, 1000, 10) AS price_bucket, \
            quantity, \
            WIDTH_BUCKET(quantity, 0, 5000, 20) AS qty_bucket \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    // DuckDB doesn't have WIDTH_BUCKET, so provide equivalent formula
    // WIDTH_BUCKET(expr, min, max, num_buckets) should return values from 1 to num_buckets
    let duckdb_sql = format!(
        "SELECT \
            id, \
            price, \
            CAST(LEAST(FLOOR((price - 0) / ((1000 - 0) / 10.0)), 10 - 1) + 1 AS BIGINT) AS price_bucket, \
            quantity, \
            CAST(LEAST(FLOOR((quantity - 0) / ((5000 - 0) / 20.0)), 20 - 1) + 1 AS BIGINT) AS qty_bucket \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    // BUG FOUND: Implementation returns bucket 11 when max value should be in bucket 10
    let mut options = QueryOptions::default();
    options.duckdb_sql = Some(&duckdb_sql);
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn combined_scalar_functions() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            ROUND(ABS(discount - 0.5), 3) AS normalized_disc, \
            CEIL(LOG(price)) AS log_ceil, \
            FLOOR(quantity / POWER(2, 3)) AS scaled_qty \
         FROM {table} \
         WHERE id <= 100 AND price > 0 \
         ORDER BY id \
         LIMIT 40",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-4;
    options.float_rel_tol = 1e-4;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn scalar_functions_in_aggregates() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            COUNT(*) AS total, \
            SUM(ROUND(price, 0)) AS sum_rounded_price, \
            AVG(ABS(discount - 0.5)) AS avg_abs_disc, \
            MAX(CEIL(quantity / 100.0)) AS max_ceil_qty \
         FROM {table} \
         WHERE id <= 500",
        table = table
    );

    let mut options = QueryOptions::default();
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-4;
    options.float_rel_tol = 1e-4;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn scalar_functions_in_where_clause() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, price, discount \
         FROM {table} \
         WHERE ROUND(price, -2) = 100 \
            OR ABS(discount) > 0.3 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn round_with_different_precisions() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            ROUND(price, 0) AS round_0, \
            ROUND(price, 1) AS round_1, \
            ROUND(price, 2) AS round_2, \
            ROUND(price, -1) AS round_neg1 \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 30",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-6;
    assert_query_matches(&executor, &fixture, &sql, options);
}
