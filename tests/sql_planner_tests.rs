use idk_uwu_ig::sql::{FilterExpr, QueryPlan, TableAccess, plan_create_table_sql, plan_sql};

fn table_by_name<'a>(plan: &'a QueryPlan, name: &str) -> &'a TableAccess {
    plan.tables
        .iter()
        .find(|table| table.table_name == name)
        .expect("table missing from plan")
}

fn assert_leaf(filter: &FilterExpr, expected_sql: &str) {
    match filter {
        FilterExpr::Leaf(expr) => assert_eq!(expr.to_string(), expected_sql),
        _ => panic!("expected leaf filter"),
    }
}

#[test]
fn plans_basic_select_reads() {
    let plan = plan_sql("SELECT id, name FROM users WHERE status = 'active'").unwrap();
    let users = table_by_name(&plan, "users");

    assert!(users.write_columns.is_empty());
    assert!(users.read_columns.contains("id"));
    assert!(users.read_columns.contains("name"));
    assert!(users.read_columns.contains("status"));
    let filter = users.filters.as_ref().expect("missing filter");
    assert_leaf(filter, "status = 'active'");
}

#[test]
fn plans_insert_and_source_tables() {
    let plan =
        plan_sql("INSERT INTO archive (id) SELECT id FROM entries WHERE published = true").unwrap();

    let archive = table_by_name(&plan, "archive");
    assert!(archive.read_columns.is_empty());
    assert!(archive.write_columns.contains("id"));
    assert_eq!(archive.write_columns.len(), 1);

    let entries = table_by_name(&plan, "entries");
    assert!(entries.write_columns.is_empty());
    assert!(entries.read_columns.contains("id"));
    assert!(entries.read_columns.contains("published"));
}

#[test]
fn plans_update_reads_and_writes() {
    let plan = plan_sql("UPDATE accounts SET balance = balance + 10 WHERE id = 42").unwrap();
    let accounts = table_by_name(&plan, "accounts");

    assert!(accounts.write_columns.contains("balance"));
    assert!(accounts.read_columns.contains("balance"));
    assert!(accounts.read_columns.contains("id"));
}

#[test]
fn plans_delete_filters() {
    let plan = plan_sql("DELETE FROM sessions WHERE expires_at < NOW()").unwrap();
    let sessions = table_by_name(&plan, "sessions");

    assert!(sessions.write_columns.is_empty());
    assert!(sessions.read_columns.contains("expires_at"));
    let filter = sessions.filters.as_ref().expect("missing filter");
    assert_eq!(filter.to_string(), "expires_at < NOW()");
}

#[test]
fn captures_and_or_filters() {
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND (region = 'US' OR vip = true)",
    )
    .unwrap();
    let accounts = table_by_name(&plan, "accounts");
    let filter = accounts.filters.as_ref().expect("missing filter");

    match filter {
        FilterExpr::And(parts) => {
            assert_eq!(parts.len(), 2);
            assert_leaf(&parts[0], "status = 'active'");
            match &parts[1] {
                FilterExpr::Or(or_parts) => {
                    assert_eq!(or_parts.len(), 2);
                    assert_leaf(&or_parts[0], "region = 'US'");
                    assert_leaf(&or_parts[1], "vip = true");
                }
                _ => panic!("expected OR node"),
            }
        }
        _ => panic!("expected AND root"),
    }
}

#[test]
fn plans_basic_create_table() {
    let plan = plan_create_table_sql(
        "CREATE TABLE items (id UUID, name String, created DateTime) ORDER BY (id, created)",
    )
    .expect("plan create table");

    assert_eq!(plan.table_name, "items");
    assert_eq!(plan.columns.len(), 3);
    assert_eq!(plan.columns[0].name, "id");
    assert_eq!(plan.columns[1].name, "name");
    assert_eq!(plan.columns[2].name, "created");
    assert_eq!(plan.order_by, vec!["id".to_string(), "created".to_string()]);
    assert!(!plan.if_not_exists);
}

#[test]
fn plans_create_table_with_if_not_exists() {
    let sql = "CREATE TABLE IF NOT EXISTS metrics (ts TIMESTAMP, value DOUBLE)";
    let plan = plan_create_table_sql(sql).expect("plan create table");

    assert_eq!(plan.table_name, "metrics");
    assert_eq!(plan.columns.len(), 2);
    assert!(plan.order_by.is_empty());
    assert!(plan.if_not_exists);
}
