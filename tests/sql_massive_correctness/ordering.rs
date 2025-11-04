use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn ordering_variants_remain_stable() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let queries = vec![
        format!(
            "SELECT nullable_number, nullable_text, id \
             FROM {table} \
             ORDER BY nullable_number DESC NULLS LAST, \
                      nullable_text ASC NULLS FIRST, \
                      id ASC \
             LIMIT 120 \
             OFFSET 30",
            table = table
        ),
        format!(
            "SELECT tenant, region, discount \
             FROM {table} \
             WHERE tenant IN ('alpha', 'delta') \
             ORDER BY tenant ASC, discount DESC NULLS LAST, id ASC \
             LIMIT 200",
            table = table
        ),
        format!(
            "SELECT region, segment, AVG(net_amount) AS avg_net \
             FROM {table} \
             GROUP BY region, segment \
             ORDER BY avg_net DESC, region ASC",
            table = table
        ),
        format!(
            "SELECT DISTINCT tenant, segment \
             FROM {table} \
             ORDER BY tenant, segment \
             LIMIT 40",
            table = table
        ),
    ];

    for sql in &queries {
        let mut options = QueryOptions::default();
        options.order_matters = true;
        assert_query_matches(&executor, &fixture, sql, options);
    }
}
