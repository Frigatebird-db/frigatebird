# Satori Benchmarks

High-volume performance benchmarks for the Satori HTAP database.

## High Volume Benchmark

The `high_volume.rs` benchmark tests Satori's performance with large datasets.

### What it does

1. **Creates a table** with 5 columns optimized for ~1KB rows
2. **Inserts 10 million rows** (~10GB total data) in batches of 1,000
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
   - GROUP BY with aggregates
   - UPDATE with WHERE clause
   - DELETE with WHERE clause

### Running the benchmark

```bash
# Build the benchmark
cargo build --bin high_volume_bench --release

# Run the benchmark (release mode recommended for realistic performance)
cargo run --bin high_volume_bench --release

# Or run in debug mode (slower but faster to compile)
cargo run --bin high_volume_bench
```

### Expected Output

The benchmark will:
- Log progress every 2,000 inserted rows
- Show insert rate (rows/sec)
- Display timing for each query benchmark
- Provide a final summary with overall statistics

### Performance Notes

- Insert performance varies based on storage backend and cache settings
- Read performance depends on data locality and index usage
- The benchmark uses ORDER BY on the `id` column, which matches the table's sort key for optimal performance
- ~10GB of data will be written to the storage directory during the benchmark
