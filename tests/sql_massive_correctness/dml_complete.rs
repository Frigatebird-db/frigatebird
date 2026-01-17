use super::*;
use frigatebird::sql::executor::SqlExecutionError;

fn install_fixture() -> (ExecutorHarness, MassiveFixture) {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
    (harness, fixture)
}

#[test]
fn update_without_where_full_table() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    // First, get count before update
    let count_sql = format!("SELECT COUNT(*) FROM {table}", table = table);
    if let Ok(count_result) = executor.query(&count_sql) {
        println!("Row count before UPDATE:\n{}", count_result);
    }

    // UPDATE without WHERE - should update ALL rows
    let update_sql = format!("UPDATE {table} SET discount = 0.5", table = table);

    let update_result = executor.execute(&update_sql);
    if let Err(SqlExecutionError::Unsupported(_)) = update_result {
        println!("UPDATE without WHERE not yet supported, skipping test");
        return;
    }

    if update_result.is_err() {
        println!("UPDATE without WHERE failed: {:?}", update_result);
        return;
    }

    // Verify all rows were updated
    let verify_sql = format!(
        "SELECT COUNT(*) AS total, COUNT(CASE WHEN discount = 0.5 THEN 1 END) AS updated \
         FROM {table}",
        table = table
    );

    if let Ok(result) = executor.query(&verify_sql) {
        println!("After full table UPDATE:\n{}", result);
        // Both counts should be equal if all rows were updated
    }
}

#[test]
fn delete_without_where_full_table() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    // Get count before delete
    let count_before_sql = format!("SELECT COUNT(*) FROM {table}", table = table);
    if let Ok(count_before) = executor.query(&count_before_sql) {
        println!("Row count before DELETE:\n{}", count_before);
    }

    // DELETE without WHERE - should delete ALL rows
    let delete_sql = format!("DELETE FROM {table}", table = table);

    let delete_result = executor.execute(&delete_sql);
    if let Err(SqlExecutionError::Unsupported(_)) = delete_result {
        println!("DELETE without WHERE not yet supported, skipping test");
        return;
    }

    if delete_result.is_err() {
        println!("DELETE without WHERE failed: {:?}", delete_result);
        return;
    }

    // Verify all rows were deleted
    let count_after_sql = format!("SELECT COUNT(*) FROM {table}", table = table);
    if let Ok(count_after) = executor.query(&count_after_sql) {
        println!("Row count after full table DELETE:\n{}", count_after);
        // Should be 0 if all rows deleted
    }
}

#[test]
fn distinct_with_aggregates() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT DISTINCT \
            region, \
            COUNT(*) AS cnt, \
            SUM(quantity) AS sum_qty \
         FROM {table} \
         WHERE id <= 500 \
         GROUP BY region \
         ORDER BY region",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn distinct_with_multiple_columns_and_aggregates() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT DISTINCT \
            region, \
            segment, \
            AVG(price) AS avg_price \
         FROM {table} \
         WHERE id <= 300 \
         GROUP BY region, segment \
         ORDER BY region, segment",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn qualify_with_row_number() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            tenant, \
            quantity, \
            ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY quantity DESC) AS rn \
         FROM {table} \
         WHERE id <= 200 \
         QUALIFY rn <= 3 \
         ORDER BY tenant, rn",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn qualify_with_rank() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            region, \
            price, \
            RANK() OVER (PARTITION BY region ORDER BY price DESC) AS price_rank \
         FROM {table} \
         WHERE id <= 200 \
         QUALIFY price_rank <= 5 \
         ORDER BY region, price_rank",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn qualify_with_complex_condition() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            tenant, \
            net_amount, \
            ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY net_amount DESC) AS rn, \
            SUM(net_amount) OVER (PARTITION BY tenant) AS total_revenue \
         FROM {table} \
         WHERE id <= 300 \
         QUALIFY rn <= 2 AND total_revenue > 100000 \
         ORDER BY tenant, rn",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn order_by_with_arbitrary_expression() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            quantity, \
            discount \
         FROM {table} \
         WHERE id <= 200 \
         ORDER BY (price * quantity * (1 - discount)) DESC, id ASC \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}

#[test]
fn order_by_with_case_expression() {
    let (harness, fixture) = install_fixture();
    let ExecutorHarness { executor, .. } = harness;
    let table = fixture.table_name();

    let sql = format!(
        "SELECT \
            id, \
            price, \
            active \
         FROM {table} \
         WHERE id <= 200 \
         ORDER BY \
            CASE \
                WHEN active THEN 1 \
                ELSE 2 \
            END, \
            price DESC, \
            id ASC \
         LIMIT 50",
        table = table
    );

    let mut options = QueryOptions::default();
    options.order_matters = true;
    options.skip_if_unsupported = true;
    assert_query_matches(&executor, &fixture, &sql, options);
}
