use super::*;

struct Case {
    sql: String,
    duckdb_sql: Option<String>,
    order_matters: bool,
}

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let fixture = MassiveFixture::install(&harness.executor);
    (harness, fixture)
}

#[test]
fn predicate_matrix_is_consistent() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let cases = vec![
        Case {
            sql: format!(
                "SELECT id, tenant, quantity \
                 FROM {table} \
                 WHERE tenant = 'alpha' \
                   AND quantity BETWEEN 100 AND 4000 \
                   AND created_at >= '2021-01-01 00:00:00' \
                 ORDER BY quantity DESC, id \
                 LIMIT 50",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT id, region, active \
                 FROM {table} \
                 WHERE (region = 'emea' OR region = 'apac') \
                   AND (active OR discount > 0.2) \
                   AND net_amount > 5000 \
                 ORDER BY region, id \
                 LIMIT 60",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT COUNT(*) \
                 FROM {table} \
                 WHERE tenant IN ('beta', 'gamma') \
                   AND quantity NOT BETWEEN 200 AND 300 \
                   AND price > 50 \
                   AND (nullable_number IS NULL OR nullable_number > 10)",
                table = table
            ),
            duckdb_sql: None,
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT DISTINCT tenant \
                 FROM {table} \
                 WHERE description LIKE '%consumer%' \
                   AND description NOT LIKE 'omega:%' \
                 ORDER BY tenant",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT COUNT(*) \
                 FROM {table} \
                 WHERE description ILIKE '%A__-%' \
                   AND nullable_text IS NOT NULL",
                table = table
            ),
            duckdb_sql: None,
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT COUNT(*) \
                 FROM {table} \
                 WHERE description RLIKE '.*-[A-Z]{{2}}$'",
                table = table
            ),
            duckdb_sql: Some(format!(
                "SELECT COUNT(*) FROM {table} WHERE description REGEXP '.*-[A-Z]{{2}}$'",
                table = table
            )),
            order_matters: false,
        },
        Case {
            sql: format!(
                "SELECT id, tenant, region \
                 FROM {table} \
                 WHERE active = FALSE \
                    OR nullable_text = '' \
                 ORDER BY id OFFSET 100 LIMIT 40",
                table = table
            ),
            duckdb_sql: None,
            order_matters: true,
        },
        Case {
            sql: format!(
                "SELECT id \
                 FROM {table} \
                 WHERE (tenant = 'delta' AND region <> 'emea') \
                    OR (segment = 'startup' AND price BETWEEN 75 AND 400) \
                 ORDER BY id \
                 LIMIT 120",
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
