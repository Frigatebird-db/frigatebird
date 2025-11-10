use super::*;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng, rngs::StdRng};

struct RandomQuery {
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
fn randomized_query_sampling() {
    run_random_suite(256);
}

#[test]
#[ignore]
fn randomized_query_stress() {
    run_random_suite(5_000);
}

fn run_random_suite(iterations: usize) {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name().to_string();
    let row_count = fixture.row_count() as i64;
    let mut rng = StdRng::seed_from_u64(0xD15C_0DE5);

    let mut queries = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        queries.push(generate_random_query(&table, row_count, &mut rng));
    }

    for (idx, case) in queries.iter().enumerate() {
        eprintln!("Random case #{idx}: {}", case.sql);
        let mut options = QueryOptions::default();
        options.duckdb_sql = case.duckdb_sql.as_deref();
        options.order_matters = case.order_matters;
        options.skip_if_unsupported = true;
        assert_query_matches(&executor, &fixture, &case.sql, options);
    }
}

#[test]
fn debug_distinct_nullable_text_case() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let sql = "SELECT DISTINCT nullable_text, segment FROM massive_correctness WHERE quantity BETWEEN 2873 AND 3411 ORDER BY quantity DESC, id ASC";
    let ours = executor.query(sql).expect("satori query failed");
    println!("ours rows:\n{}", ours);
    let conn = fixture.duckdb();
    let mut stmt = conn.prepare(sql).expect("prep duckdb");
    let mut rows = stmt.query([]).expect("duckdb query");
    let column_count = rows.as_ref().expect("statement reference").column_count();
    let mut duck_rows = Vec::new();
    use duckdb::types::ValueRef;
    while let Some(row) = rows.next().expect("duckdb step") {
        let mut values = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let value = row.get_ref(idx).expect("duckdb value");
            let cell = match value {
                ValueRef::Null => None,
                ValueRef::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => Some(format!("{value:?}")),
            };
            values.push(cell);
        }
        duck_rows.push(values);
    }
    println!("duck rows: {:?}", duck_rows);
}

#[test]
#[ignore]
fn debug_window_case() {
    let harness = setup_executor();
    let mut config = MassiveFixtureConfig::default();
    config.row_count = 2_000;
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    let ExecutorHarness { executor, .. } = harness;
    let sql = "SELECT tenant, quantity, quantity, price, ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY created_at) AS rn, SUM(quantity) OVER (PARTITION BY tenant ORDER BY quantity ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty, LAG(price, 1, 0) OVER (PARTITION BY tenant ORDER BY quantity) AS lag_price, LEAD(net_amount, 1, 0) OVER (PARTITION BY tenant ORDER BY quantity) AS lead_net FROM massive_correctness WHERE tenant = 'beta' AND segment = 'consumer' AND nullable_number IS NOT NULL QUALIFY rn <= 8 ORDER BY tenant, rn";
    let ours = executor.query(sql).expect("satori query failed");
    println!("ours rows:\n{}", ours);
    if let Some(batch) = ours.batches.first() {
        println!("raw running_qty column:");
        if let Some(column) = batch.columns.get(&5) {
            for row_idx in 0..batch.num_rows {
                println!(
                    "row {row_idx}: {:?}",
                    column.value_as_string(row_idx).unwrap_or_default()
                );
            }
        }
    }

    let conn = fixture.duckdb();
    let mut stmt = conn.prepare(sql).expect("duckdb prepare");
    let mut rows = stmt.query([]).expect("duckdb query");
    let column_count = rows.as_ref().expect("statement reference").column_count();
    let mut duck_rows = Vec::new();
    use duckdb::types::ValueRef;
    while let Some(row) = rows.next().expect("duckdb step") {
        let mut values = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let value = row.get_ref(idx).expect("duckdb value");
            let cell = match value {
                ValueRef::Null => None,
                ValueRef::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => Some(format!("{value:?}")),
            };
            values.push(cell);
        }
        duck_rows.push(values);
    }
    println!("duck rows: {:?}", duck_rows);
}

fn generate_random_query(table: &str, row_count: i64, rng: &mut StdRng) -> RandomQuery {
    match rng.gen_range(0..3) {
        0 => random_simple_select(table, row_count, rng),
        1 => random_grouped_select(table, row_count, rng),
        _ => random_window_select(table, row_count, rng),
    }
}

fn random_simple_select(table: &str, row_count: i64, rng: &mut StdRng) -> RandomQuery {
    let mut columns: Vec<&str> = MASSIVE_COLUMNS.iter().map(|meta| meta.name).collect();
    columns.shuffle(rng);
    let select_count = rng.gen_range(2..=5).min(columns.len());
    let mut projection: Vec<String> = columns[..select_count]
        .iter()
        .map(|name| name.to_string())
        .collect();

    if rng.gen_bool(0.25) {
        projection.push("quantity * price AS gross_value".into());
    }
    if rng.gen_bool(0.15) {
        projection
            .push("CASE WHEN discount > 0.2 THEN 'high' ELSE 'low' END AS discount_band".into());
    }

    let distinct = rng.gen_bool(0.1);
    let mut sql = format!(
        "SELECT {distinct}{projection} FROM {table}",
        distinct = if distinct { "DISTINCT " } else { "" },
        projection = projection.join(", "),
        table = table
    );

    if rng.gen_bool(0.8) {
        let predicate_count = rng.gen_range(1..=3);
        let mut predicates = Vec::new();
        for _ in 0..predicate_count {
            predicates.push(random_predicate(row_count, rng));
        }
        let filter = glue_predicates(&predicates, rng);
        sql.push_str(" WHERE ");
        sql.push_str(&filter);
    }

    let mut order_matters = false;
    if rng.gen_bool(0.6) {
        let order_clause = build_order_clause(rng);
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_clause);
        order_matters = true;

        if rng.gen_bool(0.5) {
            let limit = rng.gen_range(20..=200);
            let offset = if rng.gen_bool(0.3) {
                format!(" OFFSET {}", rng.gen_range(0..=50))
            } else {
                String::new()
            };
            sql.push_str(&format!(" LIMIT {limit}{offset}"));
        }
    }

    RandomQuery {
        sql,
        duckdb_sql: None,
        order_matters,
    }
}

fn random_grouped_select(table: &str, row_count: i64, rng: &mut StdRng) -> RandomQuery {
    let group_candidates = ["tenant", "region", "segment", "active"];
    let mut groups = group_candidates.to_vec();
    groups.shuffle(rng);
    let group_count = rng.gen_range(1..=2).min(groups.len());
    let group_cols = &groups[..group_count];

    let mut measures = vec![
        "SUM(quantity) AS sum_qty".to_string(),
        "SUM(net_amount) AS sum_net".to_string(),
        "AVG(price) AS avg_price".to_string(),
        "MAX(discount) AS max_disc".to_string(),
        "COUNT(*) AS total_rows".to_string(),
    ];
    measures.shuffle(rng);
    measures.truncate(rng.gen_range(2..=4));

    if rng.gen_bool(0.4) {
        measures.push("COUNT(DISTINCT tenant) AS distinct_tenants".into());
    }
    if rng.gen_bool(0.3) {
        measures.push("VAR_SAMP(quantity) AS var_qty".into());
    }
    let has_sum_net = measures.iter().any(|measure| measure.contains("sum_net"));

    let mut sql = format!(
        "SELECT {groups}, {measures} FROM {table}",
        groups = group_cols.join(", "),
        measures = measures.join(", "),
        table = table
    );

    if rng.gen_bool(0.5) {
        let predicate_count = rng.gen_range(1..=2);
        let mut predicates = Vec::new();
        for _ in 0..predicate_count {
            predicates.push(random_predicate(row_count, rng));
        }
        sql.push_str(" WHERE ");
        sql.push_str(&glue_predicates(&predicates, rng));
    }

    sql.push_str(" GROUP BY ");
    sql.push_str(&group_cols.join(", "));

    if rng.gen_bool(0.3) {
        let having = if rng.gen_bool(0.5) {
            "SUM(net_amount) > 10000"
        } else {
            "COUNT(*) > 200"
        };
        sql.push_str(" HAVING ");
        sql.push_str(having);
    }

    sql.push_str(" ORDER BY ");
    let mut order_parts: Vec<String> = group_cols.iter().map(|col| format!("{col} ASC")).collect();
    if has_sum_net && rng.gen_bool(0.5) {
        order_parts.push("sum_net DESC".into());
    }
    sql.push_str(&order_parts.join(", "));

    RandomQuery {
        sql,
        duckdb_sql: None,
        order_matters: true,
    }
}

fn random_window_select(table: &str, row_count: i64, rng: &mut StdRng) -> RandomQuery {
    let partition_col = ["tenant", "region", "segment"]
        .choose(rng)
        .copied()
        .unwrap_or("tenant");
    let order_col = ["created_at", "quantity", "price"]
        .choose(rng)
        .copied()
        .unwrap_or("created_at");

    let mut projections = vec![
        partition_col.to_string(),
        order_col.to_string(),
        "quantity".to_string(),
        "price".to_string(),
        "ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY created_at) AS rn".into(),
        format!(
            "SUM(quantity) OVER (PARTITION BY {partition} ORDER BY {order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty",
            partition = partition_col,
            order = order_col
        ),
        format!(
            "LAG(price, 1, 0) OVER (PARTITION BY {partition} ORDER BY {order}) AS lag_price",
            partition = partition_col,
            order = order_col
        ),
        format!(
            "LEAD(net_amount, 1, 0) OVER (PARTITION BY {partition} ORDER BY {order}) AS lead_net",
            partition = partition_col,
            order = order_col
        ),
    ];

    if rng.gen_bool(0.5) {
        projections.push(format!(
            "FIRST_VALUE(description) OVER (PARTITION BY {partition} ORDER BY {order}) AS first_desc",
            partition = partition_col,
            order = order_col
        ));
    }
    if rng.gen_bool(0.5) {
        projections.push(format!(
            "LAST_VALUE(description) OVER (PARTITION BY {partition} ORDER BY {order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_desc",
            partition = partition_col,
            order = order_col
        ));
    }

    let mut sql = format!("SELECT {} FROM {table}", projections.join(", "));

    if rng.gen_bool(0.7) {
        let predicate_count = rng.gen_range(1..=2);
        let mut predicates = Vec::new();
        for _ in 0..predicate_count {
            predicates.push(random_predicate(row_count, rng));
        }
        sql.push_str(" WHERE ");
        sql.push_str(&glue_predicates(&predicates, rng));
    }

    if rng.gen_bool(0.6) {
        sql.push_str(" QUALIFY rn <= 5");
    }

    sql.push_str(" ORDER BY ");
    sql.push_str(partition_col);
    sql.push_str(", rn");

    RandomQuery {
        sql,
        duckdb_sql: None,
        order_matters: true,
    }
}

fn glue_predicates(predicates: &[String], rng: &mut StdRng) -> String {
    match predicates.len() {
        0 => "1=1".into(),
        1 => predicates[0].clone(),
        2 => format!("({} AND {})", predicates[0], predicates[1]),
        _ => {
            if rng.gen_bool(0.5) {
                format!(
                    "({} AND {}) OR {}",
                    predicates[0], predicates[1], predicates[2]
                )
            } else {
                format!(
                    "({} OR {}) AND {}",
                    predicates[0], predicates[1], predicates[2]
                )
            }
        }
    }
}

fn random_predicate(row_count: i64, rng: &mut StdRng) -> String {
    let column = MASSIVE_COLUMNS.choose(rng).unwrap();
    match (column.name, column.kind) {
        ("tenant", ColumnKind::Text) => {
            let tenants = ["alpha", "beta", "gamma", "delta", "omega"];
            if rng.gen_bool(0.5) {
                format!("tenant = '{}'", tenants.choose(rng).unwrap())
            } else {
                let mut sample = tenants.to_vec();
                sample.shuffle(rng);
                let list: Vec<String> = sample.iter().take(3).map(|v| format!("'{v}'")).collect();
                format!("tenant IN ({})", list.join(", "))
            }
        }
        ("region", ColumnKind::Text) => {
            let regions = ["americas", "emea", "apac", "africa", "antarctica", "orbit"];
            if rng.gen_bool(0.5) {
                format!("region = '{}'", regions.choose(rng).unwrap())
            } else {
                format!("region LIKE '%{}%'", regions.choose(rng).unwrap())
            }
        }
        ("segment", ColumnKind::Text) => {
            let segments = [
                "consumer",
                "enterprise",
                "public_sector",
                "startup",
                "partner",
            ];
            if rng.gen_bool(0.5) {
                format!("segment = '{}'", segments.choose(rng).unwrap())
            } else {
                format!("segment ILIKE '%{}%'", segments.choose(rng).unwrap())
            }
        }
        ("nullable_text", ColumnKind::Text) => match rng.gen_range(0..3) {
            0 => "nullable_text IS NULL".into(),
            1 => "nullable_text = ''".into(),
            _ => format!("nullable_text LIKE 'note-%'"),
        },
        ("description", ColumnKind::Text) => {
            if rng.gen_bool(0.5) {
                format!(
                    "description LIKE '%{}%'",
                    ["consumer", "enterprise", "startup"].choose(rng).unwrap()
                )
            } else {
                format!(
                    "description ILIKE '%{}%'",
                    ["A__", "B__", "C__", "D__"].choose(rng).unwrap()
                )
            }
        }
        ("active", ColumnKind::Bool) => {
            if rng.gen_bool(0.5) {
                "active = TRUE".into()
            } else {
                "active = FALSE".into()
            }
        }
        ("quantity", ColumnKind::Int) => {
            let base = rng.gen_range(100..=5000);
            match rng.gen_range(0..3) {
                0 => format!("quantity > {}", base),
                1 => format!(
                    "quantity BETWEEN {} AND {}",
                    base,
                    base + rng.gen_range(100..=1000)
                ),
                _ => format!("quantity IN ({}, {}, {})", base, base + 7, base + 11),
            }
        }
        ("id", ColumnKind::Int) => {
            let base = rng.gen_range(1..=row_count.max(1));
            format!("id <= {}", base)
        }
        ("price", ColumnKind::Float) => {
            let threshold = rng.gen_range(50..=750) as f64 / 10.0;
            if rng.gen_bool(0.5) {
                format!("price >= {threshold:.2}")
            } else {
                format!(
                    "price BETWEEN {low:.2} AND {high:.2}",
                    low = threshold,
                    high = threshold + rng.gen_range(5..=50) as f64
                )
            }
        }
        ("discount", ColumnKind::Float) => {
            let cut = rng.gen_range(0..=40) as f64 / 100.0;
            format!("discount <= {cut:.2}")
        }
        ("net_amount", ColumnKind::Float) => {
            let cut = rng.gen_range(1_000..=20_000) as f64;
            format!("net_amount > {cut:.2}")
        }
        ("nullable_number", ColumnKind::Float) => match rng.gen_range(0..3) {
            0 => "nullable_number IS NULL".into(),
            1 => "nullable_number IS NOT NULL".into(),
            _ => {
                let low = rng.gen_range(10..=200) as f64;
                format!(
                    "nullable_number BETWEEN {low:.2} AND {high:.2}",
                    high = low + rng.gen_range(10..=100) as f64
                )
            }
        },
        ("created_at", ColumnKind::Timestamp) => {
            let years = ["2020", "2021", "2022", "2023", "2024"];
            let months = ["01", "03", "05", "07", "09", "11"];
            let year = years.choose(rng).unwrap();
            let month = months.choose(rng).unwrap();
            format!("created_at >= '{year}-{month}-01 00:00:00'")
        }
        _ => "1 = 1".into(),
    }
}

fn build_order_clause(rng: &mut StdRng) -> String {
    let mut candidates = vec![
        "quantity DESC",
        "quantity ASC",
        "price DESC",
        "price ASC",
        "discount DESC NULLS LAST",
        "discount ASC",
        "created_at DESC",
        "created_at ASC",
        "tenant ASC",
        "region ASC",
        "nullable_number DESC NULLS LAST",
    ];
    candidates.shuffle(rng);
    candidates.truncate(rng.gen_range(1..=3));
    let mut clause = candidates.join(", ");
    if !clause.to_lowercase().contains("id ") {
        clause.push_str(", id ASC");
    }
    clause
}
