#[path = "common/mod.rs"]
mod common;

use common::*;
use duckdb::Connection;

fn query_duckdb(conn: &Connection, sql: &str) -> duckdb::Result<Vec<Vec<Option<String>>>> {
    let column_count = {
        let mut meta_stmt = conn.prepare(sql)?;
        {
            let mut meta_rows = meta_stmt.query([])?;
            let _ = meta_rows.next()?;
        }
        meta_stmt.column_count()
    };

    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let value = row.get_ref(idx)?;
            values.push(match value {
                duckdb::types::ValueRef::Null => None,
                duckdb::types::ValueRef::Boolean(b) => {
                    Some(if b { "TRUE".into() } else { "FALSE".into() })
                }
                duckdb::types::ValueRef::TinyInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::SmallInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::Int(i) => Some(i.to_string()),
                duckdb::types::ValueRef::BigInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::HugeInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::UTinyInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::USmallInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::UInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::UBigInt(i) => Some(i.to_string()),
                duckdb::types::ValueRef::Float(f) => Some(f.to_string()),
                duckdb::types::ValueRef::Double(f) => Some(f.to_string()),
                duckdb::types::ValueRef::Decimal(decimal) => Some(decimal.to_string()),
                duckdb::types::ValueRef::Timestamp(unit, raw) => {
                    let micros = match unit {
                        duckdb::types::TimeUnit::Second => raw * 1_000_000,
                        duckdb::types::TimeUnit::Millisecond => raw * 1_000,
                        duckdb::types::TimeUnit::Microsecond => raw,
                        duckdb::types::TimeUnit::Nanosecond => raw / 1_000,
                    };
                    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
                }
                duckdb::types::ValueRef::Text(text) => {
                    Some(String::from_utf8_lossy(text).to_string())
                }
                duckdb::types::ValueRef::Blob(blob) => Some(format!("{:?}", blob)),
                other => Some(format!("{other:?}")),
            });
        }
        out.push(values);
    }
    Ok(out)
}

#[test]
#[ignore]
fn debug_predicate_matrix_order() {
    let harness = setup_executor();
    let config = MassiveFixtureConfig::from_env();
    let fixture = MassiveFixture::install_with_config(&harness.executor, config);
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

    if let Some(catalog) = harness.directory.table_catalog(table) {
        for column in catalog.columns() {
            if column.name == "active" {
                println!("active column type = {:?}", column.data_type);
            }
        }
    }
    if let Some(desc) = harness.handler.list_pages_in_table(table, "active").first() {
        println!("active descriptor type = {:?}", desc.data_type);
        if let Some(page_arc) = harness.handler.get_page(desc.clone()) {
            println!("active page data = {:?}", page_arc.page.data);
        }
    }

    let ours = harness.executor.query(&sql).expect("frigatebird query");
    let duck = query_duckdb(fixture.duckdb(), &sql).expect("duckdb query");

    let ours_rows = ours.into_rows();
    println!("ours rows={}", ours_rows.len());
    println!("duck rows={}", duck.len());

    println!("first 10 ours:");
    for row in ours_rows.iter().take(10) {
        println!("  {:?}", row);
    }

    println!("first 10 duck:");
    for row in duck.iter().take(10) {
        println!("  {:?}", row);
    }
}
