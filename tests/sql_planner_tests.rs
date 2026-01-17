use frigatebird::metadata_store::{
    ColumnDefinition, PageDirectory, TableDefinition, TableMetaStore,
};
use frigatebird::sql::physical_plan::PhysicalExpr;
use frigatebird::sql::types::DataType;
use frigatebird::sql::{FilterExpr, QueryPlan, TableAccess, plan_create_table_sql, plan_sql};
use sqlparser::ast::BinaryOperator;
use std::sync::{Arc, RwLock};

fn empty_directory() -> Arc<PageDirectory> {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(store));

    directory
        .register_table(TableDefinition::new(
            "users",
            vec![
                ColumnDefinition::from_type("id", DataType::Int64),
                ColumnDefinition::from_type("name", DataType::String),
                ColumnDefinition::from_type("status", DataType::String),
            ],
            vec![],
        ))
        .unwrap();
    directory
        .register_table(TableDefinition::new(
            "archive",
            vec![ColumnDefinition::from_type("id", DataType::Int64)],
            vec![],
        ))
        .unwrap();
    directory
        .register_table(TableDefinition::new(
            "entries",
            vec![
                ColumnDefinition::from_type("id", DataType::Int64),
                ColumnDefinition::from_type("published", DataType::Boolean),
            ],
            vec![],
        ))
        .unwrap();
    directory
        .register_table(TableDefinition::new(
            "accounts",
            vec![
                ColumnDefinition::from_type("id", DataType::Int64),
                ColumnDefinition::from_type("balance", DataType::Int64),
                ColumnDefinition::from_type("status", DataType::String),
                ColumnDefinition::from_type("region", DataType::String),
                ColumnDefinition::from_type("vip", DataType::Boolean),
            ],
            vec![],
        ))
        .unwrap();
    directory
        .register_table(TableDefinition::new(
            "sessions",
            vec![ColumnDefinition::from_type(
                "expires_at",
                DataType::Timestamp,
            )],
            vec![],
        ))
        .unwrap();

    directory
}

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
    let directory = empty_directory();
    let plan = plan_sql(
        "SELECT id, name FROM users WHERE status = 'active'",
        &directory,
    )
    .unwrap();
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
    let directory = empty_directory();
    let plan = plan_sql(
        "INSERT INTO archive (id) SELECT id FROM entries WHERE published = true",
        &directory,
    )
    .unwrap();

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
    let directory = empty_directory();
    let plan = plan_sql(
        "UPDATE accounts SET balance = balance + 10 WHERE id = 42",
        &directory,
    )
    .unwrap();
    let accounts = table_by_name(&plan, "accounts");

    assert!(accounts.write_columns.contains("balance"));
    assert!(accounts.read_columns.contains("balance"));
    assert!(accounts.read_columns.contains("id"));
}

#[test]
fn plans_delete_filters() {
    let directory = empty_directory();
    let plan = plan_sql("DELETE FROM sessions WHERE expires_at < NOW()", &directory).unwrap();
    let sessions = table_by_name(&plan, "sessions");

    assert!(sessions.write_columns.is_empty());
    assert!(sessions.read_columns.contains("expires_at"));
    let filter = sessions.filters.as_ref().expect("missing filter");

    // Check structure: expires_at < [Literal Timestamp]
    if let FilterExpr::Leaf(PhysicalExpr::BinaryOp { left, op, right }) = filter {
        match &**left {
            PhysicalExpr::Column { name, .. } => assert_eq!(name, "expires_at"),
            _ => panic!("left side is not a column"),
        }

        assert_eq!(*op, BinaryOperator::Lt);

        match &**right {
            PhysicalExpr::Literal(val) => {
                // ScalarValue is private, so we check the Debug string
                let s = format!("{:?}", val);
                assert!(
                    s.contains("Timestamp("),
                    "expected Timestamp literal, got {}",
                    s
                );
            }
            _ => panic!("right side is not a timestamp literal"),
        }
    } else {
        panic!("expected BinaryOp leaf filter");
    }
}

#[test]
fn captures_and_or_filters() {
    let directory = empty_directory();
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND (region = 'US' OR vip = true)",
        &directory,
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
