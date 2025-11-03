# Satori Benchmarks

High-volume performance benchmarks for the Satori HTAP database.

## High Volume Benchmark

The `high_volume.rs` benchmark tests Satori's performance with large datasets.

### What it does

1. **Creates a table** with 5 columns (configurable row size)
2. **Inserts rows** (configurable count) in batches of 1,000
3. **Runs 12 different read/write benchmarks**:
   - Point queries (exact ID match)
   - Range queries (BETWEEN)
   - LIKE pattern matching
   - Event type filtering
   - COUNT aggregates
   - COUNT with filters
   - ORDER BY with LIMIT
   - Pagination (OFFSET + LIMIT)
   - DISTINCT queries
   - Complex WHERE with multiple conditions (AND)
   - UPDATE with WHERE clause
   - DELETE with WHERE clause

### Running the benchmark

```bash
# Default: 10M rows × 1KB = ~10GB
cargo run --bin high_volume_bench --release

# Custom configuration: 1M rows × 2KB = ~2GB
cargo run --bin high_volume_bench --release -- --rows 1000000 --row-size 2

# Short flags also work
cargo run --bin high_volume_bench --release -- -r 500000 -s 4

# Debug mode (faster to compile, slower to run)
cargo run --bin high_volume_bench -- -r 100000 -s 1

# Show help
cargo run --bin high_volume_bench -- --help
```

### Configuration Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--rows` | `-r` | Number of rows to insert | 10,000,000 |
| `--row-size` | `-s` | Target row size in KB | 1 |
| `--help` | `-h` | Show help message | - |

### Expected Output

The benchmark will:
- Display configuration (rows, row size, total data size)
- Log progress every 2,000 inserted rows
- Show insert rate (rows/sec)
- Display timing for each query benchmark
- Provide a final summary with overall statistics

### Performance Notes

- Insert performance varies based on storage backend and cache settings
- Read performance depends on data locality and index usage
- The benchmark uses ORDER BY on the `id` column, which matches the table's sort key for optimal performance
- Data will be written to the storage directory during the benchmark
- For quick testing, use smaller values like `--rows 10000 --row-size 1`

### Example Configurations

```bash
# Quick test: 10K rows, 1KB each = ~10MB
cargo run --bin high_volume_bench -- -r 10000 -s 1

# Medium test: 1M rows, 1KB each = ~1GB
cargo run --bin high_volume_bench -- -r 1000000 -s 1

# Large test: 5M rows, 2KB each = ~10GB
cargo run --bin high_volume_bench -- -r 5000000 -s 2

# Stress test: 10M rows, 4KB each = ~40GB
cargo run --bin high_volume_bench --release -- -r 10000000 -s 4
```
