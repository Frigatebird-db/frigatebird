<div align="center">
  <img src="./assets/frigatebird.svg" alt="Frigatebird" width="25%">
  <p>Frigatebird: A columnar SQL database with push-based query execution</p>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

</div>

## Overview

Frigatebird is an embedded columnar database that implements a **push-based Volcano execution model** with **morsel-driven parallelism**. Queries compile into pipelines where operators push batches downstream through channels, enabling parallel execution across multiple workers.

### Key Features

- **Push-based execution** - Operators push data downstream instead of pulling, enabling natural parallelism
- **Morsel-driven parallelism** - Data processed in 50k-row morsels that flow through pipelines independently
- **Late materialization** - Columns loaded only when needed, reducing I/O for selective queries
- **Three-tier caching** - Uncompressed (hot) → Compressed (warm) → Disk (cold)
- **Vectorized filtering** - Bitmap operations process 64 rows per CPU instruction
- **Dictionary encoding** - Automatic compression for low-cardinality string columns
- **WAL durability** - Write-ahead logging with three-phase commit for crash recovery
- **io_uring + O_DIRECT** - Batched async I/O bypassing OS page cache (Linux)

## Quick Start

```bash
# Start the interactive CLI
make frigatebird

# Or with cargo directly
cargo run --bin frigatebird
```

```sql
sql> CREATE TABLE events (id TEXT, ts TIMESTAMP, data TEXT) ORDER BY id
OK

sql> INSERT INTO events (id, ts, data) VALUES ('sensor-1', '2024-01-15 10:30:00', 'temperature=23.5')
OK

sql> SELECT * FROM events WHERE id = 'sensor-1'
id       | ts                  | data
sensor-1 | 2024-01-15 10:30:00 | temperature=23.5
(1 rows)

sql> \dt
  events

sql> \d events
Table: events
----------------------------------------
  id                   String
  ts                   Timestamp
  data                 String
```

Single command mode:
```bash
cargo run --bin frigatebird -- -c "SELECT COUNT(*) FROM events WHERE id = 'sensor-1'"
```

## How It Works

### Query Pipeline

A query like `SELECT name FROM users WHERE age > 25 AND city = 'NYC'` compiles into a pipeline:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Step 0    │     │   Step 1    │     │   Step 2    │
│  column:age │────▶│ column:city │────▶│ column:name │────▶ Output
│ filter:>25  │     │ filter:=NYC │     │ (project)   │
│   (root)    │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
   187k rows           31k rows            8k rows
```

Each step loads only its column and filters, passing surviving row IDs downstream. The final step materializes projection columns only for rows that passed all filters.

### Late Materialization

```
Without late materialization:  Load ALL columns × 187k rows = 750k values
With late materialization:     age: 187k + city: 31k + name: 8k = 226k values
                               ─────────────────────────────────────────────
                               70% less data loaded
```

### Parallel Execution

Workers grab morsels (page groups) using lock-free atomic compare-and-swap:

```
Time ──────────────────────────────────────────────▶

Worker 0  ║▓▓▓ M0:S0 ▓▓▓║     ║▓▓ M0:S1 ▓▓║    ║▓ M0:S2 ▓║
Worker 1  ║▓▓▓ M1:S0 ▓▓▓║   ║▓▓ M1:S1 ▓▓║   ║▓ M1:S2 ▓║
Worker 2    ║▓▓▓ M2:S0 ▓▓▓║   ║▓▓ M2:S1 ▓▓║    ║▓ M2:S2 ▓║
Worker 3      ║▓▓ M3:S0 ▓▓║   ║▓ M3:S1 ▓║    ║▓ M3:S2 ▓║

Legend: M0:S0 = Morsel 0, Step 0
```

Multiple morsels execute concurrently. Channels buffer batches between steps.

## CLI Commands

| Command | Description |
|---------|-------------|
| `\d` | List all tables |
| `\d <table>` | Describe table schema |
| `\dt` | List all tables |
| `\i <file>` | Execute SQL from file |
| `\q` | Quit |

## SQL Support

**DDL**
- `CREATE TABLE <name> (...columns...) ORDER BY <col[, ...]>`

**DML**
- `INSERT INTO <table> (...) VALUES (...)`
- `UPDATE <table> SET ... [WHERE ...]`
- `DELETE FROM <table> [WHERE ...]`

**Queries**
- Single table SELECT with WHERE, ORDER BY, LIMIT, OFFSET
- Predicates: `AND`, `OR`, comparisons, `BETWEEN`, `LIKE`, `ILIKE`, `RLIKE`, `IN`, `IS NULL`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `VARIANCE`, `STDDEV`, `PERCENTILE_CONT`
- Aggregate modifiers: `FILTER (WHERE ...)`, `DISTINCT`
- Conditional aggregates: `sumIf`, `avgIf`, `countIf`
- `GROUP BY` with `HAVING`, `QUALIFY`, `DISTINCT`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `SUM`, `FIRST_VALUE`, `LAST_VALUE`
- Time functions: `TIME_BUCKET`, `DATE_TRUNC`
- Scalar functions: `ABS`, `ROUND`, `CEIL`, `FLOOR`, `EXP`, `LN`, `LOG`, `POWER`, `WIDTH_BUCKET`

**Data Types**
- `TEXT` / `VARCHAR` / `STRING`
- `INT` / `INTEGER` / `BIGINT`
- `FLOAT` / `DOUBLE` / `REAL`
- `BOOL` / `BOOLEAN`
- `TIMESTAMP` / `DATETIME`
- `UUID`
- `INET` / `IP`

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                              FRIGATEBIRD                                │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  SQL Query ──▶ Parser ──▶ Planner ──▶ Pipeline Builder                 │
│                                              │                         │
│                                              ▼                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                      PIPELINE EXECUTOR                            │  │
│  │                                                                   │  │
│  │   ┌─────────┐    ┌─────────┐    ┌─────────┐                      │  │
│  │   │ Step 0  │───▶│ Step 1  │───▶│ Step N  │───▶ Output           │  │
│  │   │ (root)  │    │         │    │         │                      │  │
│  │   └─────────┘    └─────────┘    └─────────┘                      │  │
│  │        │              │              │                            │  │
│  │        └──────────────┴──────────────┘                            │  │
│  │            Workers execute steps in parallel                      │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                │                                       │
│                                ▼                                       │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                        PAGE HANDLER                               │  │
│  │                                                                   │  │
│  │   ┌───────────────┐   ┌───────────────┐   ┌──────────────────┐   │  │
│  │   │  Uncompressed │   │  Compressed   │   │      Disk        │   │  │
│  │   │  Cache (Hot)  │◀─▶│  Cache (Warm) │◀─▶│     (Cold)       │   │  │
│  │   └───────────────┘   └───────────────┘   └──────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                       METADATA STORE                              │  │
│  │           Tables • Column Chains • Page Descriptors               │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Storage

Data is stored in columnar format with one file per column:

```
storage/
├── data.00000          # Page data (4GB max per file, auto-rotates)
└── data.00001

wal_files/
├── frigatebird/        # WAL for durability
└── frigatebird-meta/   # Metadata journal
```

Pages are compressed with LZ4 and aligned to 4KB boundaries for O_DIRECT I/O.

## Running Tests

```bash
cargo test
```

## Documentation

See the `docs/` directory for detailed documentation:
- [Architecture](docs/architecture.md) - System design and execution model
- [Components](docs/components.md) - Deep dive into each subsystem
- [Data Flow](docs/data-flow.md) - Step-by-step query execution trace

## License

MIT License - see [LICENSE](LICENSE) for details.
