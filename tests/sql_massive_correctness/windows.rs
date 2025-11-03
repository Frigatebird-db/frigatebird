use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let fixture = MassiveFixture::install(&harness.executor);
    (harness, fixture)
}

#[test]
fn window_functions_match_reference() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            tenant, \
            region, \
            created_at, \
            quantity, \
            price, \
            ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY quantity DESC) AS rn, \
            RANK() OVER (PARTITION BY tenant ORDER BY quantity DESC) AS rk, \
            DENSE_RANK() OVER (PARTITION BY tenant ORDER BY quantity DESC) AS drk, \
            LAG(quantity, 1, 0) OVER (PARTITION BY tenant ORDER BY created_at) AS lag_qty, \
            LEAD(quantity, 1, 0) OVER (PARTITION BY tenant ORDER BY created_at) AS lead_qty, \
            SUM(quantity) OVER (PARTITION BY tenant ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty, \
            SUM(price) OVER (PARTITION BY tenant ORDER BY created_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS window_price, \
            FIRST_VALUE(description) OVER (PARTITION BY tenant ORDER BY created_at) AS first_desc, \
            LAST_VALUE(description) OVER (PARTITION BY tenant ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_desc \
         FROM {table} \
         WHERE tenant IN ('alpha', 'beta') \
           AND created_at >= '2021-01-01 00:00:00' \
         QUALIFY rn <= 5 \
         ORDER BY tenant, rn",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn range_based_frames_are_consistent() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            region, \
            quantity, \
            SUM(quantity) OVER (PARTITION BY region ORDER BY quantity RANGE BETWEEN 500 PRECEDING AND CURRENT ROW) AS range_sum, \
            AVG(price) OVER (PARTITION BY region ORDER BY quantity RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW) AS range_avg \
         FROM {table} \
         WHERE region IN ('americas', 'emea') \
         ORDER BY region, quantity \
         LIMIT 200",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}
