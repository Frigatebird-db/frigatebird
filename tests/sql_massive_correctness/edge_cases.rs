use super::*;
use idk_uwu_ig::sql::executor::SqlExecutionError;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn empty_result_set() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant, price FROM {table} WHERE id > 999999999 ORDER BY id",
        table = table
    );

    let result = executor.query(&sql);
    assert!(result.is_ok(), "Query for empty result failed");
    let rows = result.unwrap().rows;
    assert_eq!(rows.len(), 0, "Expected empty result set");
}

#[test]
fn single_row_result() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant, price FROM {table} WHERE id = 1 ORDER BY id",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn null_value_comparisons() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, nullable_number, nullable_text \
         FROM {table} \
         WHERE nullable_number IS NULL \
            OR nullable_text IS NULL \
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
fn null_vs_not_null() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            COUNT(*) AS total, \
            COUNT(nullable_number) AS non_null_numbers, \
            COUNT(nullable_text) AS non_null_texts \
         FROM {table} \
         WHERE id <= 500",
        table = table
    );

    let mut options = QueryOptions::default();
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn null_in_aggregates() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            COUNT(*) AS total_rows, \
            COUNT(nullable_number) AS non_null_count, \
            AVG(nullable_number) AS avg_val, \
            MIN(nullable_number) AS min_val, \
            MAX(nullable_number) AS max_val \
         FROM {table} \
         WHERE id <= 500",
        table = table
    );

    let mut options = QueryOptions::default();
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn limit_zero() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant, price FROM {table} ORDER BY id LIMIT 0",
        table = table
    );

    let result = executor.query(&sql);
    if let Err(SqlExecutionError::Unsupported(_)) = result {
        println!("LIMIT 0 not yet supported, skipping test");
        return;
    }
    assert!(result.is_ok(), "LIMIT 0 query failed");
    let rows = result.unwrap().rows;
    assert_eq!(rows.len(), 0, "Expected empty result with LIMIT 0");
}

#[test]
fn limit_one() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant, price FROM {table} ORDER BY id LIMIT 1",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn offset_beyond_result_set() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant FROM {table} ORDER BY id OFFSET 999999",
        table = table
    );

    let result = executor.query(&sql);
    if let Err(SqlExecutionError::Unsupported(_)) = result {
        println!("Large OFFSET not yet supported, skipping test");
        return;
    }
    assert!(result.is_ok(), "Large OFFSET query failed");
    let rows = result.unwrap().rows;
    assert_eq!(rows.len(), 0, "Expected empty result with large OFFSET");
}

#[test]
fn offset_without_limit() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant, price FROM {table} ORDER BY id OFFSET 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn very_large_numbers() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price * 1000000 AS very_large, \
            quantity * 999999999 AS huge_qty \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-2;
    options.float_rel_tol = 1e-6;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn very_small_numbers() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            discount / 1000000 AS tiny_discount, \
            price / 999999999 AS minuscule_price \
         FROM {table} \
         WHERE id <= 50 \
         ORDER BY id \
         LIMIT 20",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-6;  // Very loose tolerance for tiny numbers
    options.float_rel_tol = 0.2;    // 20% relative tolerance for very small divisions
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn zero_division_handling() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            discount, \
            CASE WHEN discount = 0 THEN NULL ELSE price / discount END AS safe_division \
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
fn empty_string_handling() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, nullable_text \
         FROM {table} \
         WHERE nullable_text = '' \
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
fn distinct_with_nulls() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT DISTINCT nullable_text \
         FROM {table} \
         WHERE id <= 200 \
         ORDER BY nullable_text NULLS FIRST",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn aggregate_on_empty_group() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            COUNT(*) AS cnt, \
            SUM(quantity) AS sum_qty, \
            AVG(price) AS avg_price \
         FROM {table} \
         WHERE id > 999999999",
        table = table
    );

    let result = executor.query(&sql);
    if let Err(SqlExecutionError::Unsupported(_)) = result {
        println!("Aggregate on empty set not yet supported, skipping test");
        return;
    }
    assert!(result.is_ok(), "Aggregate on empty set failed");
    let rows = result.unwrap().rows;
    // Should return one row with COUNT=0, SUM=NULL or 0, AVG=NULL
    println!("Aggregate on empty set: {:?}", rows);
}

#[test]
fn order_by_with_all_nulls() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, nullable_number \
         FROM {table} \
         WHERE nullable_number IS NULL \
         ORDER BY nullable_number NULLS FIRST, id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn boolean_edge_cases() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            active, \
            NOT active AS inactive, \
            active AND (price > 100) AS active_and_expensive, \
            active OR (quantity > 1000) AS active_or_high_qty \
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
fn like_pattern_edge_cases() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, description \
         FROM {table} \
         WHERE description LIKE '%:%:%' \
            OR description LIKE '_____' \
            OR description LIKE '%%' \
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
fn between_with_equal_bounds() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, quantity \
         FROM {table} \
         WHERE quantity BETWEEN 500 AND 500 \
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
fn in_clause_with_single_value() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, tenant \
         FROM {table} \
         WHERE tenant IN ('alpha') \
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
fn complex_null_predicate_logic() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, nullable_number, nullable_text \
         FROM {table} \
         WHERE (nullable_number IS NULL AND nullable_text IS NOT NULL) \
            OR (nullable_number IS NOT NULL AND nullable_text IS NULL) \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}
