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
                "SELECT COUNT(*) FROM {table} WHERE regexp_matches(description, '.*-[A-Z]{{2}}$')",
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
        println!("running case: {}", case.sql.replace('\n', " "));
        assert_query_matches(&executor, &fixture, &case.sql, options);
    }
}

#[test]
#[ignore]
fn debug_predicate_case() {
    let harness = setup_executor();
    let ExecutorHarness { executor, .. } = harness;
    let config = MassiveFixtureConfig { row_count: 1_000 };
    let fixture = MassiveFixture::install_with_config(&executor, config);
    let table = fixture.table_name();
    let sql = format!(
        "SELECT id, region, active \
         FROM {table} \
         WHERE (region = 'emea' OR region = 'apac') \
           AND (active OR discount > 0.2) \
           AND net_amount > 5000 \
         ORDER BY region, id \
         LIMIT 60",
        table = table
    );

    let ours = executor.query(&sql).expect("satori query");
    let mut stmt = fixture.duckdb().prepare(&sql).expect("duckdb prepare");
    let mut duck_iter = stmt.query([]).expect("duckdb query");
    let mut duck_rows = Vec::new();
    while let Some(row) = duck_iter.next().expect("duckdb row") {
        let id: i64 = row.get(0).expect("duck id");
        let region: String = row.get(1).expect("duck region");
        let active: bool = row.get(2).expect("duck active");
        duck_rows.push((id, region, active));
    }

    println!("ours rows count {}", ours.rows.len());
    println!("duck rows count {}", duck_rows.len());
    for idx in 0..ours.rows.len().min(duck_rows.len()).min(10) {
        println!(
            "row {idx} ours={:?} duck={:?}",
            ours.rows[idx], duck_rows[idx]
        );
    }
}
