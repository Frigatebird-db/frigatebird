use duckdb::Connection;
use float_cmp::approx_eq_f64;
use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SelectResult, SqlExecutor};
use once_cell::sync::OnceCell;
use rand::distributions::{Alphanumeric, DistString};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cmp::Ordering;
use std::fmt;
use std::sync::{Arc, RwLock};

pub struct ExecutorHarness {
    pub executor: SqlExecutor,
    pub handler: Arc<PageHandler>,
    pub directory: Arc<PageDirectory>,
}

/// Builds a fresh in-memory execution stack for integration tests.
pub fn setup_executor() -> ExecutorHarness {
    let store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(Arc::clone(&store)));
    let compressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::new()));
    let page_io = Arc::new(PageIO {});
    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        Arc::new(Compressor::new()),
    ));
    let handler = Arc::new(PageHandler::new(locator, fetcher, materializer));
    let executor = SqlExecutor::new(Arc::clone(&handler), Arc::clone(&directory));
    ExecutorHarness {
        executor,
        handler,
        directory,
    }
}

#[derive(Clone, Copy)]
pub enum ColumnKind {
    Text,
    Int,
    Float,
    Bool,
    Timestamp,
}

#[derive(Clone, Copy)]
pub struct ColumnMeta {
    pub name: &'static str,
    pub kind: ColumnKind,
}

impl ColumnMeta {
    pub const fn new(name: &'static str, kind: ColumnKind) -> Self {
        ColumnMeta { name, kind }
    }
}

pub const MASSIVE_TABLE: &str = "massive_correctness";

pub const MASSIVE_COLUMNS: [ColumnMeta; 13] = [
    ColumnMeta::new("id", ColumnKind::Int),
    ColumnMeta::new("tenant", ColumnKind::Text),
    ColumnMeta::new("region", ColumnKind::Text),
    ColumnMeta::new("segment", ColumnKind::Text),
    ColumnMeta::new("quantity", ColumnKind::Int),
    ColumnMeta::new("price", ColumnKind::Float),
    ColumnMeta::new("discount", ColumnKind::Float),
    ColumnMeta::new("net_amount", ColumnKind::Float),
    ColumnMeta::new("created_at", ColumnKind::Timestamp),
    ColumnMeta::new("active", ColumnKind::Bool),
    ColumnMeta::new("description", ColumnKind::Text),
    ColumnMeta::new("nullable_text", ColumnKind::Text),
    ColumnMeta::new("nullable_number", ColumnKind::Float),
];

const MASSIVE_ROW_COUNT: usize = 50_000;
const MASSIVE_INSERT_BATCH: usize = 500;

#[derive(Clone)]
pub struct BigFixtureRow {
    pub id: i64,
    pub tenant: String,
    pub region: String,
    pub segment: String,
    pub quantity: i64,
    pub price: f64,
    pub discount: f64,
    pub net_amount: f64,
    pub created_at: String,
    pub active: bool,
    pub description: String,
    pub nullable_text: Option<String>,
    pub nullable_number: Option<f64>,
}

impl BigFixtureRow {
    fn to_sql_values(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            quoted(&self.tenant),
            quoted(&self.region),
            quoted(&self.segment),
            self.quantity.to_string(),
            format_float(self.price),
            format_float(self.discount),
            format_float(self.net_amount),
            quoted(&self.created_at),
            if self.active { "TRUE".into() } else { "FALSE".into() },
            quoted(&self.description),
            match &self.nullable_text {
                Some(value) => quoted(value),
                None => "NULL".into(),
            },
            match self.nullable_number {
                Some(value) => format_float(value),
                None => "NULL".into(),
            },
        ]
    }
}

fn quoted(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

fn format_float(value: f64) -> String {
    if value.is_finite() {
        let mut s = format!("{value:.10}");
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        s
    } else {
        value.to_string()
    }
}

struct BigFixtureDataset {
    create_sql: String,
    insert_batches: Vec<String>,
    rows: Vec<BigFixtureRow>,
}

static MASSIVE_DATASET: OnceCell<BigFixtureDataset> = OnceCell::new();

fn dataset() -> &'static BigFixtureDataset {
    MASSIVE_DATASET.get_or_init(build_dataset)
}

fn build_dataset() -> BigFixtureDataset {
    let mut rng = StdRng::seed_from_u64(0x5A7A_5015_2024);
    let tenants = ["alpha", "beta", "gamma", "delta", "omega"];
    let regions = [
        "americas",
        "emea",
        "apac",
        "africa",
        "antarctica",
        "orbit",
    ];
    let segments = [
        "consumer",
        "enterprise",
        "public_sector",
        "startup",
        "partner",
    ];

    let mut rows = Vec::with_capacity(MASSIVE_ROW_COUNT);
    for idx in 0..MASSIVE_ROW_COUNT {
        let tenant = tenants[idx % tenants.len()].to_string();
        let region = regions[(idx / tenants.len()) % regions.len()].to_string();
        let segment = segments[(idx / regions.len()) % segments.len()].to_string();
        let quantity = (rng.gen_range(1..=5000) as i64) + ((idx % 7) as i64);
        let price = ((rng.gen_range(10_000..=750_000) as f64) / 100.0)
            * (1.0 + ((idx % 11) as f64) / 500.0);
        let discount = if idx % 9 == 0 {
            0.0
        } else {
            (rng.gen_range(0..=4_000) as f64) / 10_000.0
        };
        let net_amount = price * quantity as f64 * (1.0 - discount);
        let created_at = synthetic_timestamp(idx);
        let active = idx % 13 != 0;
        let description = format!(
            "{}:{}:{}:{}:{}",
            tenant,
            region,
            segment,
            idx % 997,
            random_code(idx, &mut rng)
        );
        let nullable_text = if idx % 10 == 0 {
            None
        } else if idx % 17 == 0 {
            Some(String::new())
        } else {
            Some(format!("note-{}", idx % 123))
        };
        let nullable_number = if idx % 8 == 0 {
            None
        } else {
            Some((quantity as f64).powf(1.3) * (1.0 - discount * 0.75))
        };

        rows.push(BigFixtureRow {
            id: idx as i64 + 1,
            tenant,
            region,
            segment,
            quantity,
            price,
            discount,
            net_amount,
            created_at,
            active,
            description,
            nullable_text,
            nullable_number,
        });
    }

    let create_sql = format!(
        "CREATE TABLE {table} (\
            id BIGINT,\
            tenant TEXT,\
            region TEXT,\
            segment TEXT,\
            quantity BIGINT,\
            price DOUBLE,\
            discount DOUBLE,\
            net_amount DOUBLE,\
            created_at TIMESTAMP,\
            active BOOLEAN,\
            description TEXT,\
            nullable_text TEXT,\
            nullable_number DOUBLE\
        ) ORDER BY (id, created_at)",
        table = MASSIVE_TABLE
    );

    let mut insert_batches = Vec::new();
    for chunk in rows.chunks(MASSIVE_INSERT_BATCH) {
        let mut statement = format!("INSERT INTO {table} (", table = MASSIVE_TABLE);
        for (idx, column) in MASSIVE_COLUMNS.iter().enumerate() {
            if idx > 0 {
                statement.push_str(", ");
            }
            statement.push_str(column.name);
        }
        statement.push_str(") VALUES ");

        for (row_idx, row) in chunk.iter().enumerate() {
            if row_idx > 0 {
                statement.push_str(", ");
            }
            statement.push('(');
            let values = row.to_sql_values();
            for (value_idx, value) in values.iter().enumerate() {
                if value_idx > 0 {
                    statement.push_str(", ");
                }
                statement.push_str(value);
            }
            statement.push(')');
        }
        statement.push(';');
        insert_batches.push(statement);
    }

    BigFixtureDataset {
        create_sql,
        insert_batches,
        rows,
    }
}

fn synthetic_timestamp(index: usize) -> String {
    let year = 2020 + (index % 5) as i32;
    let month = 1 + ((index / 5) % 12) as i32;
    let day = 1 + ((index / 37) % 28) as i32;
    let hour = ((index / 997) % 24) as i32;
    let minute = ((index / 13) % 60) as i32;
    let second = (index % 60) as i32;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn random_code(index: usize, rng: &mut StdRng) -> String {
    let mut alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'];
    alphabet.rotate_left(index % alphabet.len());
    let mut buf = String::with_capacity(6);
    for ch in alphabet.iter().take(3) {
        buf.push(*ch);
    }
    buf.push('-');
    buf.push_str(&Alphanumeric.sample_string(rng, 2));
    buf
}

pub struct MassiveFixture {
    dataset: &'static BigFixtureDataset,
    duckdb: Connection,
}

impl MassiveFixture {
    pub fn install(executor: &SqlExecutor) -> Self {
        let dataset = dataset();
        executor
            .execute(&dataset.create_sql)
            .expect("create massive fixture table");
        for batch in &dataset.insert_batches {
            executor
                .execute(batch)
                .unwrap_or_else(|err| panic!("failed to seed fixture batch: {err:?}"));
        }

        let duckdb = Connection::open_in_memory().expect("open duckdb reference");
        duckdb
            .execute_batch(&dataset.create_sql)
            .expect("create duckdb table");
        for batch in &dataset.insert_batches {
            duckdb
                .execute_batch(batch)
                .unwrap_or_else(|err| panic!("duckdb batch load failed: {err:?}"));
        }

        MassiveFixture { dataset, duckdb }
    }

    pub fn table_name(&self) -> &'static str {
        MASSIVE_TABLE
    }

    pub fn columns(&self) -> &'static [ColumnMeta] {
        &MASSIVE_COLUMNS
    }

    pub fn row_count(&self) -> usize {
        self.dataset.rows.len()
    }

    pub fn duckdb(&self) -> &Connection {
        &self.duckdb
    }

    pub fn rows(&self) -> &[BigFixtureRow] {
        &self.dataset.rows
    }
}

#[derive(Clone)]
pub struct QueryOptions<'a> {
    pub duckdb_sql: Option<&'a str>,
    pub order_matters: bool,
    pub float_abs_tol: f64,
    pub float_rel_tol: f64,
}

impl<'a> Default for QueryOptions<'a> {
    fn default() -> Self {
        QueryOptions {
            duckdb_sql: None,
            order_matters: false,
            float_abs_tol: 1e-6,
            float_rel_tol: 1e-6,
        }
    }
}

#[derive(Debug)]
pub struct QueryComparisonError {
    pub message: String,
}

impl fmt::Display for QueryComparisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for QueryComparisonError {}

#[derive(Clone, PartialEq)]
enum NormalizedValue {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
}

impl NormalizedValue {
    fn canonical_key(&self) -> String {
        match self {
            NormalizedValue::Null => "NULL".into(),
            NormalizedValue::Bool(value) => {
                if *value {
                    "BOOL:true".into()
                } else {
                    "BOOL:false".into()
                }
            }
            NormalizedValue::Number(value) => format!("NUM:{:.12}", value),
            NormalizedValue::Text(value) => format!("TEXT:{value}"),
        }
    }

    fn equals_with_tol(&self, other: &Self, abs_tol: f64, rel_tol: f64) -> bool {
        match (self, other) {
            (NormalizedValue::Null, NormalizedValue::Null) => true,
            (NormalizedValue::Bool(lhs), NormalizedValue::Bool(rhs)) => lhs == rhs,
            (NormalizedValue::Text(lhs), NormalizedValue::Text(rhs)) => lhs == rhs,
            (NormalizedValue::Number(lhs), NormalizedValue::Number(rhs)) => {
                if lhs.is_nan() && rhs.is_nan() {
                    true
                } else if lhs.is_infinite() || rhs.is_infinite() {
                    lhs == rhs
                } else {
                    approx_eq_f64(
                        *lhs,
                        *rhs,
                        float_cmp::F64Margin {
                            ulps: None,
                            epsilon: abs_tol.max(rel_tol * rhs.abs()),
                        },
                    )
                }
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
struct NormalizedRow {
    values: Vec<NormalizedValue>,
    key: Vec<String>,
}

impl NormalizedRow {
    fn new(values: Vec<NormalizedValue>) -> Self {
        let key = values.iter().map(NormalizedValue::canonical_key).collect();
        NormalizedRow { values, key }
    }
}

impl PartialEq for NormalizedRow {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for NormalizedRow {}

impl PartialOrd for NormalizedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for NormalizedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

fn normalize_rows(rows: Vec<Vec<Option<String>>>) -> Vec<NormalizedRow> {
    rows.into_iter()
        .map(|row| {
            let values = row
                .into_iter()
                .map(|cell| normalize_value(cell.as_deref()))
                .collect();
            NormalizedRow::new(values)
        })
        .collect()
}

fn normalize_value(raw: Option<&str>) -> NormalizedValue {
    match raw {
        None => NormalizedValue::Null,
        Some(text) => {
            let trimmed = text.trim();
            if trimmed.eq_ignore_ascii_case("true") {
                NormalizedValue::Bool(true)
            } else if trimmed.eq_ignore_ascii_case("false") {
                NormalizedValue::Bool(false)
            } else if let Ok(int_val) = trimmed.parse::<i128>() {
                NormalizedValue::Number(int_val as f64)
            } else if let Ok(float_val) = trimmed.parse::<f64>() {
                NormalizedValue::Number(float_val)
            } else {
                NormalizedValue::Text(trimmed.to_string())
            }
        }
    }
}

fn query_duckdb(conn: &Connection, sql: &str) -> duckdb::Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
    let mut stmt = conn.prepare(sql)?;
    let column_count = stmt.column_count();
    let mut column_names = Vec::with_capacity(column_count);
    for idx in 0..column_count {
        let name = stmt
            .column_name(idx)
            .map(|value| value.to_string())
            .unwrap_or_else(|| format!("column_{idx}"));
        column_names.push(name);
    }

    let mut rows = stmt.query([])?;
    let mut output = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let value: Option<String> = row.get(idx)?;
            values.push(value);
        }
        output.push(values);
    }
    Ok((column_names, output))
}

pub fn assert_query_matches(
    executor: &SqlExecutor,
    fixture: &MassiveFixture,
    sql: &str,
    options: QueryOptions<'_>,
) {
    let ours = executor
        .query(sql)
        .unwrap_or_else(|err| panic!("satori query failed: {err:?}"));
    let duck_sql = options.duckdb_sql.unwrap_or(sql);
    let (_duck_columns, duck_rows) = query_duckdb(fixture.duckdb(), duck_sql)
        .unwrap_or_else(|err| panic!("duckdb query failed for `{duck_sql}`: {err:?}"));
    compare_results(ours, duck_rows, options);
}

fn compare_results(
    ours: SelectResult,
    duck_rows: Vec<Vec<Option<String>>>,
    options: QueryOptions<'_>,
) {
    let mut ours_norm = normalize_rows(ours.rows);
    let mut duck_norm = normalize_rows(duck_rows);

    if ours_norm.len() != duck_norm.len() {
        panic!(
            "row count mismatch: ours={} duck={}",
            ours_norm.len(),
            duck_norm.len()
        );
    }

    if ours_norm.is_empty() {
        return;
    }

    if ours_norm[0].values.len() != duck_norm[0].values.len() {
        panic!(
            "column count mismatch: ours={} duck={}",
            ours_norm[0].values.len(),
            duck_norm[0].values.len()
        );
    }

    if !options.order_matters {
        ours_norm.sort();
        duck_norm.sort();
    }

    for (idx, (lhs, rhs)) in ours_norm.iter().zip(duck_norm.iter()).enumerate() {
        for (col_idx, (lhs_val, rhs_val)) in lhs.values.iter().zip(rhs.values.iter()).enumerate() {
            if !lhs_val.equals_with_tol(rhs_val, options.float_abs_tol, options.float_rel_tol) {
                panic!(
                    "value mismatch at row {idx}, column {col_idx}: ours={lhs_val:?} duck={rhs_val:?}"
                );
            }
        }
    }
}

pub fn sample_rows(limit: usize) -> Vec<BigFixtureRow> {
    let dataset = dataset();
    dataset
        .rows
        .iter()
        .step_by((dataset.rows.len() / limit.max(1)).max(1))
        .take(limit)
        .cloned()
        .collect()
}
