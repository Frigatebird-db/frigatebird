# High-Volume SQL Benchmark

This benchmark exercises the SQL surface area only: it uses the public `SqlExecutor`
to create a table, insert large volumes of rows, and then run aggregate queries.
No internal handlers are invoked directly.

## Running

```bash
cargo run --release --bin high_volume_bench -- \
  --rows 1000000 \
  --batch-size 5000 \
  --payload-bytes 1024 \
  --report-every 100000 \
  --rows-per-id 1000
```

Arguments:

- `--rows` – total number of rows to insert (default: `10_000`).
- `--batch-size` – rows per `INSERT ... VALUES` statement (default: `1_000`).
- `--payload-bytes` – size of the large payload column per row (default: `1_024` bytes).
- `--report-every` – print progress after this many rows (default: `100_000`).
- `--rows-per-id` – rows mapped onto the same ORDER BY key so aggregate queries
  can touch a reasonably sized slice (default: `1_000`).

The run prints insertion throughput and the latency for a handful of aggregate queries,
each scoped to a specific ORDER BY bucket (the SQL executor requires equality filters on
the sort key). A supplementary lookup validates other aggregate operators on the same bucket.

> **Note:** Range predicates (for example `id >= '...' AND id <= '...'`) are not yet
> supported by the SQL executor, so the benchmark limits itself to full-table aggregates
> and equality predicates on the ordering column.
