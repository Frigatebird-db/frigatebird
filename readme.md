# Frigatebird

Columnar SQL database with push-based Volcano execution and morsel-driven parallelism.

## Quick Start

```bash
cargo run --bin frigatebird
```

```sql
sql> CREATE TABLE events (id TEXT, ts INT, data TEXT) ORDER BY id
OK
sql> INSERT INTO events (id, ts, data) VALUES ('1', '1000', 'hello')
OK
sql> SELECT * FROM events WHERE id = '1'
id | ts | data
1 | 1000 | hello
(1 rows)
sql> \dt
  events
sql> \d events
Table: events
----------------------------------------
  id                   String
  ts                   Int64
  data                 String
```

Single command mode:
```bash
cargo run --bin frigatebird -- -c "SELECT 1 + 1"
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `\d` | List tables |
| `\d <table>` | Describe table |
| `\dt` | List tables |
| `\i <file>` | Execute SQL file |
| `\q` | Quit |

## SQL Support

**DDL**
- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`

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

## Running Tests

```bash
cargo test
```
