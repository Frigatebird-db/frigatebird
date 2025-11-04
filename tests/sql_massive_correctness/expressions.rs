use super::*;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn case_expression_simple() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            CASE \
                WHEN price < 100 THEN 'cheap' \
                WHEN price < 500 THEN 'moderate' \
                ELSE 'expensive' \
            END AS price_category \
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
fn case_expression_with_null() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            nullable_number, \
            CASE \
                WHEN nullable_number IS NULL THEN 'missing' \
                WHEN nullable_number < 100 THEN 'low' \
                ELSE 'high' \
            END AS category \
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
fn case_expression_numeric_result() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            discount, \
            CASE \
                WHEN discount > 0.3 THEN discount * 1.5 \
                WHEN discount > 0.1 THEN discount * 1.2 \
                ELSE discount \
            END AS adjusted_discount \
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
fn case_expression_in_aggregate() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            COUNT(*) AS total, \
            SUM(CASE WHEN active THEN 1 ELSE 0 END) AS active_count, \
            SUM(CASE WHEN price > 500 THEN net_amount ELSE 0 END) AS high_price_revenue \
         FROM {table} \
         WHERE id <= 500",
        table = table
    );

    let mut options = QueryOptions::default();
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn nested_case_expressions() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            active, \
            CASE \
                WHEN active THEN \
                    CASE \
                        WHEN price > 500 THEN 'active-premium' \
                        ELSE 'active-standard' \
                    END \
                ELSE 'inactive' \
            END AS status \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 40",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn arithmetic_expressions_basic() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            quantity, \
            discount, \
            price + 10 AS price_plus_10, \
            price - 5 AS price_minus_5, \
            price * 2 AS price_doubled, \
            price / 2 AS price_halved, \
            quantity % 10 AS qty_mod_10 \
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
fn arithmetic_expressions_complex() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price * quantity * (1 - discount) AS gross_revenue, \
            (price * quantity - price * quantity * discount) AS net_revenue, \
            (price + quantity) / 2 AS avg_metric, \
            price * 1.1 + quantity * 0.5 AS weighted_sum \
         FROM {table} \
         WHERE id <= 100 \
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
fn arithmetic_with_parentheses() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            (price + quantity) * discount AS expr1, \
            price + (quantity * discount) AS expr2, \
            ((price - 10) * quantity) / 100 AS expr3 \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-5;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn arithmetic_in_where_clause() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, price, quantity, discount \
         FROM {table} \
         WHERE price * quantity > 10000 \
            AND (1 - discount) > 0.5 \
            AND quantity / 10 > 50 \
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
fn arithmetic_in_order_by() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT id, price, quantity, price * quantity AS total_value \
         FROM {table} \
         WHERE id <= 200 \
         ORDER BY price * quantity DESC, id ASC \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn mixed_case_and_arithmetic() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            quantity, \
            discount, \
            CASE \
                WHEN price * quantity > 50000 THEN (price * quantity * (1 - discount)) * 0.95 \
                WHEN price * quantity > 10000 THEN (price * quantity * (1 - discount)) * 0.97 \
                ELSE price * quantity * (1 - discount) \
            END AS final_amount \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-4;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn expression_with_constants() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price * 1.21 AS price_with_tax, \
            100 + quantity AS qty_offset, \
            CASE WHEN 1 = 1 THEN 'always' ELSE 'never' END AS constant_case, \
            3.14159 * price AS pi_price \
         FROM {table} \
         WHERE id <= 100 \
         ORDER BY id \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    options.float_abs_tol = 1e-4;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn null_handling_in_expressions() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            nullable_number, \
            nullable_number + 10 AS plus_10, \
            nullable_number * 2 AS doubled, \
            CASE WHEN nullable_number IS NULL THEN 0 ELSE nullable_number END AS coalesced \
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
fn string_concatenation() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            tenant, \
            region, \
            tenant || '-' || region AS location_code \
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
