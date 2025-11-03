use idk_uwu_ig::cache::page_cache::PageCache;
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::sql::executor::SqlExecutor;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const DEFAULT_TOTAL_ROWS: usize = 10_000_000;
const DEFAULT_ROW_SIZE_KB: usize = 1;
const BATCH_SIZE: usize = 1000;

#[derive(Debug)]
struct BenchmarkConfig {
    total_rows: usize,
    row_size_bytes: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            total_rows: DEFAULT_TOTAL_ROWS,
            row_size_bytes: DEFAULT_ROW_SIZE_KB * 1024,
        }
    }
}

fn setup_executor() -> (SqlExecutor, Arc<PageHandler>, Arc<PageDirectory>) {
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
    (executor, handler, directory)
}

fn parse_args() -> BenchmarkConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut config = BenchmarkConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" | "-r" => {
                if i + 1 < args.len() {
                    config.total_rows = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Invalid value for --rows: {}", args[i + 1]);
                        std::process::exit(1);
                    });
                    i += 2;
                } else {
                    eprintln!("--rows requires a value");
                    std::process::exit(1);
                }
            }
            "--row-size" | "-s" => {
                if i + 1 < args.len() {
                    let size_kb: usize = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Invalid value for --row-size: {}", args[i + 1]);
                        std::process::exit(1);
                    });
                    config.row_size_bytes = size_kb * 1024;
                    i += 2;
                } else {
                    eprintln!("--row-size requires a value (in KB)");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    config
}

fn print_usage() {
    println!("Usage: high_volume_bench [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -r, --rows <N>         Number of rows to insert (default: 10,000,000)");
    println!("  -s, --row-size <KB>    Target row size in KB (default: 1)");
    println!("  -h, --help             Print this help message");
    println!();
    println!("Examples:");
    println!("  high_volume_bench --rows 1000000 --row-size 2");
    println!("  high_volume_bench -r 500000 -s 4");
}

fn generate_large_text(id: usize, prefix: &str, target_size: usize) -> String {
    // Generate text to fill up to target row size
    // Account for other fields (id, user_id, event_type) taking ~50 bytes
    // We have 2 large text fields (payload and metadata), so divide remaining space
    let base = format!("{}_{}", prefix, id);
    let remaining = if target_size > base.len() + 50 {
        (target_size - base.len() - 50) / 2
    } else {
        50 // minimum size
    };

    // Fill with repeated characters
    let fill = "x".repeat(remaining);
    format!("{}{}", base, fill)
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    if secs > 0 {
        format!("{}.{:03}s", secs, millis)
    } else {
        format!("{}ms", millis)
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn print_separator() {
    println!("\n{}", "=".repeat(80));
}

fn main() {
    let config = parse_args();
    let total_data_gb = (config.total_rows * config.row_size_bytes) as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    SATORI HIGH-VOLUME BENCHMARK                              ║");
    println!("║                    {} Rows × {}KB = ~{:.1}GB Dataset                           ║",
        format_number(config.total_rows),
        config.row_size_bytes / 1024,
        total_data_gb
    );
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    let (executor, _, _) = setup_executor();

    // STEP 1: Create Table
    print_separator();
    println!("STEP 1: Creating table...");
    let start = Instant::now();

    executor
        .execute(
            "CREATE TABLE benchmark_data (
                id TEXT,
                user_id TEXT,
                event_type TEXT,
                payload TEXT,
                metadata TEXT
            ) ORDER BY id"
        )
        .expect("Failed to create table");

    let duration = start.elapsed();
    println!("✓ Table created successfully in {}", format_duration(duration));

    // STEP 2: Insert rows
    print_separator();
    println!("\nSTEP 2: Inserting {} rows in batches of {}...", format_number(config.total_rows), BATCH_SIZE);
    let start = Instant::now();
    let mut total_inserted = 0;
    const LOG_INTERVAL: usize = 2000;

    for batch_start in (0..config.total_rows).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(config.total_rows);
        let mut values = Vec::new();

        for i in batch_start..batch_end {
            let id = format!("{:010}", i); // Zero-padded ID for proper ordering
            let user_id = format!("user_{}", i % 10000);
            let event_type = match i % 5 {
                0 => "login",
                1 => "purchase",
                2 => "view",
                3 => "click",
                _ => "other",
            };
            let payload = generate_large_text(i, "payload", config.row_size_bytes);
            let metadata = generate_large_text(i, "meta", config.row_size_bytes);

            values.push(format!(
                "('{}', '{}', '{}', '{}', '{}')",
                id, user_id, event_type, payload, metadata
            ));
        }

        let insert_sql = format!(
            "INSERT INTO benchmark_data (id, user_id, event_type, payload, metadata) VALUES {}",
            values.join(", ")
        );

        executor.execute(&insert_sql)
            .expect(&format!("Failed to insert batch starting at {}", batch_start));

        total_inserted += batch_end - batch_start;

        // Print progress every 2k entries
        if total_inserted % LOG_INTERVAL == 0 {
            let elapsed = start.elapsed();
            let rate = total_inserted as f64 / elapsed.as_secs_f64();
            let progress = (total_inserted as f64 / config.total_rows as f64) * 100.0;
            println!(
                "  Progress: {}/{} ({:.1}%) - {:.0} rows/sec - {}",
                format_number(total_inserted), format_number(config.total_rows), progress, rate, format_duration(elapsed)
            );
        }
    }

    let insert_duration = start.elapsed();
    let insert_rate = config.total_rows as f64 / insert_duration.as_secs_f64();
    println!("\n✓ Inserted {} rows in {}", format_number(config.total_rows), format_duration(insert_duration));
    println!("  Average rate: {:.0} rows/sec", insert_rate);
    println!("  Estimated data size: ~{:.1}GB", total_data_gb);

    // STEP 3: Read Benchmarks
    print_separator();
    println!("\nSTEP 3: Running read benchmarks...\n");

    // Benchmark 1: Point query (exact match)
    println!("Benchmark 1: Point Query (WHERE id = '0000005000')");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id, event_type FROM benchmark_data WHERE id = '0000005000'")
        .expect("Point query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 2: Range query with BETWEEN
    println!("\nBenchmark 2: Range Query (BETWEEN '0001000000' AND '0001001000')");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, event_type FROM benchmark_data WHERE id BETWEEN '0001000000' AND '0001001000'")
        .expect("Range query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 3: LIKE query
    println!("\nBenchmark 3: LIKE Query (user_id LIKE 'user_123%')");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id FROM benchmark_data WHERE user_id LIKE 'user_123%' LIMIT 100")
        .expect("LIKE query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 4: Filter by event type
    println!("\nBenchmark 4: Event Type Filter (event_type = 'purchase')");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id FROM benchmark_data WHERE event_type = 'purchase' LIMIT 1000")
        .expect("Event type query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 5: COUNT aggregate
    println!("\nBenchmark 5: COUNT Aggregate (total rows)");
    let start = Instant::now();
    let result = executor
        .query("SELECT COUNT(*) FROM benchmark_data")
        .expect("COUNT query failed");
    let duration = start.elapsed();
    println!("  ✓ Count: {} in {}", result.rows[0][0].as_ref().unwrap(), format_duration(duration));

    // Benchmark 6: COUNT with filter
    println!("\nBenchmark 6: COUNT with Filter (event_type = 'login')");
    let start = Instant::now();
    let result = executor
        .query("SELECT COUNT(*) FROM benchmark_data WHERE event_type = 'login'")
        .expect("COUNT with filter failed");
    let duration = start.elapsed();
    println!("  ✓ Count: {} in {}", result.rows[0][0].as_ref().unwrap(), format_duration(duration));

    // Benchmark 7: ORDER BY with LIMIT
    println!("\nBenchmark 7: ORDER BY with LIMIT (first 100 rows)");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id, event_type FROM benchmark_data ORDER BY id LIMIT 100")
        .expect("ORDER BY query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 8: OFFSET + LIMIT (pagination)
    println!("\nBenchmark 8: Pagination (OFFSET 5000000 LIMIT 100)");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id FROM benchmark_data ORDER BY id OFFSET 5000000 LIMIT 100")
        .expect("Pagination query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // Benchmark 9: DISTINCT
    println!("\nBenchmark 9: DISTINCT event_type");
    let start = Instant::now();
    let result = executor
        .query("SELECT DISTINCT event_type FROM benchmark_data")
        .expect("DISTINCT query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} distinct values in {}", result.rows.len(), format_duration(duration));

    // Benchmark 10: Complex WHERE with multiple conditions
    println!("\nBenchmark 10: Complex WHERE (event_type = 'purchase' AND id BETWEEN '0005000000' AND '0005010000')");
    let start = Instant::now();
    let result = executor
        .query("SELECT id, user_id, event_type FROM benchmark_data WHERE event_type = 'purchase' AND id BETWEEN '0005000000' AND '0005010000'")
        .expect("Complex WHERE query failed");
    let duration = start.elapsed();
    println!("  ✓ Returned {} rows in {}", result.rows.len(), format_duration(duration));

    // STEP 4: Update Benchmark
    print_separator();
    println!("\nSTEP 4: Update benchmark...");
    println!("\nBenchmark 11: UPDATE with WHERE clause");
    let start = Instant::now();
    executor
        .execute("UPDATE benchmark_data SET event_type = 'updated' WHERE id BETWEEN '0000000000' AND '0000001000'")
        .expect("Update failed");
    let duration = start.elapsed();
    println!("  ✓ Updated rows in {}", format_duration(duration));

    // Verify update
    let result = executor
        .query("SELECT COUNT(*) FROM benchmark_data WHERE event_type = 'updated'")
        .expect("Verify update failed");
    println!("  ✓ Verified {} rows updated", result.rows[0][0].as_ref().unwrap());

    // STEP 5: Delete Benchmark
    print_separator();
    println!("\nSTEP 5: Delete benchmark...");
    println!("\nBenchmark 12: DELETE with WHERE clause");
    let start = Instant::now();
    executor
        .execute("DELETE FROM benchmark_data WHERE id BETWEEN '0000000000' AND '0000000100'")
        .expect("Delete failed");
    let duration = start.elapsed();
    println!("  ✓ Deleted rows in {}", format_duration(duration));

    // Verify delete
    let result = executor
        .query("SELECT COUNT(*) FROM benchmark_data")
        .expect("Count after delete failed");
    println!("  ✓ Total rows after delete: {}", result.rows[0][0].as_ref().unwrap());

    // Final Summary
    print_separator();
    println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           BENCHMARK COMPLETE                                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!("\nSummary:");
    println!("  • Total rows inserted: {}", format_number(config.total_rows));
    println!("  • Row size: {}KB", config.row_size_bytes / 1024);
    println!("  • Insert time: {}", format_duration(insert_duration));
    println!("  • Insert rate: {:.0} rows/sec", insert_rate);
    println!("  • Estimated dataset size: ~{:.1}GB", total_data_gb);
    println!("  • All read benchmarks completed successfully");
    println!("\n");
}
