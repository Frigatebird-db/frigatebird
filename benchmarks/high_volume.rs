use idk_uwu_ig::cache::page_cache::{
    PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::{SqlExecutionError, SqlExecutor};
use std::env;
use std::error::Error;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    rows: usize,
    batch_size: usize,
    payload_bytes: usize,
    report_every: usize,
    rows_per_id: usize,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            rows: parse_arg("--rows").unwrap_or(10_000),
            batch_size: parse_arg("--batch-size").unwrap_or(1_000),
            payload_bytes: parse_arg("--payload-bytes").unwrap_or(1_024),
            report_every: parse_arg("--report-every").unwrap_or(100_000),
            rows_per_id: parse_arg("--rows-per-id").unwrap_or(1_000),
        })
    }
}

fn parse_arg(flag: &str) -> Option<usize> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag {
            if let Some(value) = args.next() {
                return value.parse::<usize>().ok();
            }
        }
    }
    None
}

fn setup_executor() -> SqlExecutor {
    let metadata_store = Arc::new(RwLock::new(TableMetaStore::new()));
    let page_directory = Arc::new(PageDirectory::new(Arc::clone(&metadata_store)));

    let compressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let uncompressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let page_io = Arc::new(PageIO {});

    let locator = Arc::new(PageLocator::new(Arc::clone(&page_directory)));
    let fetcher = Arc::new(PageFetcher::new(
        Arc::clone(&compressed_cache),
        Arc::clone(&page_io),
    ));
    let materializer = Arc::new(PageMaterializer::new(
        Arc::clone(&uncompressed_cache),
        Arc::new(Compressor::new()),
    ));

    let handler = Arc::new(PageHandler::new(locator, fetcher, materializer));
    SqlExecutor::new(handler, page_directory)
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchmarkConfig::from_env()?;
    println!(
        "Starting benchmark with rows={}, batch_size={}, payload_bytes={} …",
        config.rows, config.batch_size, config.payload_bytes
    );

    let executor = setup_executor();
    executor.execute(
        "CREATE TABLE bench (
            id TEXT,
            metric TEXT,
            payload TEXT
        ) ORDER BY id",
    )?;

    run_insert_phase(&executor, &config)?;
    run_query_phase(&executor, &config)?;

    Ok(())
}

fn run_insert_phase(
    executor: &SqlExecutor,
    config: &BenchmarkConfig,
) -> Result<(), SqlExecutionError> {
    let payload = "x".repeat(config.payload_bytes);
    let mut rows_written: usize = 0;
    let mut batches: usize = 0;
    let mut elapsed_total = Duration::ZERO;

    let rows_per_id = config.rows_per_id.max(1);

    while rows_written < config.rows {
        let mut statement = String::from("INSERT INTO bench (id, metric, payload) VALUES ");
        let mut first = true;
        let mut batch_rows = 0usize;

        while batch_rows < config.batch_size && rows_written < config.rows {
            let current_row = rows_written;
            let id_value = current_row / rows_per_id;
            let id = format!("{:020}", id_value);
            let metric = format!("{}", current_row % 10_000);
            if !first {
                statement.push_str(", ");
            } else {
                first = false;
            }
            statement.push_str("('");
            statement.push_str(&id);
            statement.push_str("', '");
            statement.push_str(&metric);
            statement.push_str("', '");
            statement.push_str(&payload);
            statement.push_str("')");

            rows_written += 1;
            batch_rows += 1;
        }

        let start = Instant::now();
        executor.execute(&statement)?;
        let batch_elapsed = start.elapsed();
        elapsed_total += batch_elapsed;
        batches += 1;

        if rows_written % config.report_every == 0 || rows_written == config.rows {
            println!(
                "Inserted {rows_written}/{total} rows (last batch: {batch_rows} rows in {batch_elapsed:?})",
                total = config.rows
            );
            println!("So far, that is {rows_written} shits lovingly stuffed into the table.");
        }
    }

    let throughput_rows = rows_per_second(config.rows, elapsed_total);
    let payload_bytes = config.rows as u64 * config.payload_bytes as u64;
    let throughput_mb = megabytes_per_second(payload_bytes, elapsed_total);
    println!(
        "Insert phase complete: {rows} rows in {time:?} \
         (~{throughput_rows:.2} rows/s, payload throughput ≈ {throughput_mb:.2} MiB/s across {batches} batches)",
        rows = config.rows,
        time = elapsed_total,
        batches = batches,
    );
    println!(
        "Grand total: {rows} shits inserted. That is a whole lot of shits.",
        rows = config.rows
    );

    Ok(())
}

fn run_query_phase(
    executor: &SqlExecutor,
    config: &BenchmarkConfig,
) -> Result<(), SqlExecutionError> {
    println!("Running query benchmarks…");

    let rows_per_id = config.rows_per_id.max(1);
    if config.rows == 0 {
        println!("No rows inserted; skipping query phase.");
        return Ok(());
    }

    let max_bucket = (config.rows.saturating_sub(1)) / rows_per_id;
    let ids_to_probe = [
        ("head", 0usize),
        ("mid", max_bucket / 2),
        ("tail", max_bucket),
    ];

    for (label, bucket) in ids_to_probe {
        let id_literal = format!("{:020}", bucket);
        let sql = format!(
            "SELECT COUNT(*), SUM(metric), AVG(metric) FROM bench WHERE id = '{id_literal}'"
        );
        let start = Instant::now();
        let result = executor.query(&sql)?;
        let elapsed = start.elapsed();
        println!(
            "Query `{label}` on id bucket {bucket} completed in {elapsed:?} (result: {:?})",
            result.rows.first().unwrap_or(&Vec::new())
        );
    }

    if config.rows > 0 {
        let sample_bucket = (config.rows / rows_per_id) / 3;
        let sample_id = format!("{:020}", sample_bucket);
        let sql = format!("SELECT COUNT(*), MAX(metric) FROM bench WHERE id = '{sample_id}'");
        let start = Instant::now();
        let result = executor.query(&sql)?;
        let elapsed = start.elapsed();
        println!(
            "Supplementary check on id={sample_id} took {elapsed:?} (result: {:?})",
            result.rows.first().unwrap_or(&Vec::new())
        );
    }

    Ok(())
}

fn rows_per_second(rows: usize, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return rows as f64;
    }
    rows as f64 / elapsed.as_secs_f64()
}

fn megabytes_per_second(bytes: u64, elapsed: Duration) -> f64 {
    const BYTES_PER_MIB: f64 = 1_048_576.0;
    if elapsed.is_zero() {
        return bytes as f64 / BYTES_PER_MIB;
    }
    (bytes as f64 / BYTES_PER_MIB) / elapsed.as_secs_f64()
}
