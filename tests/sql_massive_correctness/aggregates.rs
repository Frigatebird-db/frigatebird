use super::*;

struct Case {
    sql: String,
    duckdb_sql: Option<String>,
    order_matters: bool,
}

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn aggregate_projections_align() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let cases = vec![
        Case {
            sql: format!(
                "SELECT \
                    COUNT(*) AS total_rows, \
                    SUM(quantity) AS qty_sum, \
                    AVG(price) AS avg_price, \
                    MIN(discount) AS min_disc, \
                    MAX(discount) AS max_disc \
                 FROM {table}",
                table = table
            ),
            duckdb_sql: None,
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT \
                    tenant, \
                    SUM(net_amount) AS total_revenue, \
                    AVG(discount) FILTER (WHERE active) AS avg_active_discount, \
                    COUNT(*) FILTER (WHERE NOT active) AS churned \
                 FROM {table} \
                 GROUP BY tenant \
                 ORDER BY tenant",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT \
                    region, \
                    segment, \
                    SUM(quantity) AS qty, \
                    VAR_POP(quantity) AS var_pop, \
                    STDDEV_SAMP(price) AS stddev_price \
                 FROM {table} \
                 GROUP BY region, segment \
                 HAVING SUM(quantity) > 10_000 \
                 ORDER BY region, segment",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT \
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY net_amount) AS q1, \
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY net_amount) AS median, \
                    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY net_amount) FILTER (WHERE active) AS active_p90, \
                    APPROX_QUANTILE(quantity, 0.95) AS approx_qty_p95 \
                 FROM {table}",
                table = table
            ),
            duckdb_sql: None,
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT \
                    sumIf(net_amount, active) AS active_revenue, \
                    avgIf(price, quantity > 2000) AS high_qty_avg_price, \
                    countIf(nullable_text IS NULL) AS missing_notes \
                 FROM {table}",
                table = table
            ),
            duckdb_sql: Some(format!(
                "SELECT \
                    SUM(CASE WHEN active THEN net_amount ELSE NULL END) AS active_revenue, \
                    AVG(CASE WHEN quantity > 2000 THEN price ELSE NULL END) AS high_qty_avg_price, \
                    SUM(CASE WHEN nullable_text IS NULL THEN 1 ELSE 0 END) AS missing_notes \
                 FROM {table}",
                table = table
            )),
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT \
                    DATE_TRUNC('day', created_at) AS day_bucket, \
                    tenant, \
                    COUNT(*) AS rows_seen, \
                    SUM(quantity) AS qty_sum \
                 FROM {table} \
                 GROUP BY 1, 2 \
                 ORDER BY 1, 2 \
                 LIMIT 200",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT \
                    region, \
                    COUNT(*) AS total, \
                    COUNT(DISTINCT tenant) AS tenant_count \
                 FROM {table} \
                 GROUP BY region \
                 ORDER BY region",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
    ];

    for case in &cases {
        let mut options = QueryOptions::default();
        options.duckdb_sql = case.duckdb_sql.as_deref();
        options.order_matters = case.order_matters;
        assert_query_matches(&executor, &fixture, &case.sql, options);
    }
}
