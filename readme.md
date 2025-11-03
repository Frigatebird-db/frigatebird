a distributed HTAP database that will mog everything that comes in its way

we are going to win

### Supported SQL (current scope)

- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`
  - Plain column definitions only; ORDER BY clause is mandatory.
- `INSERT INTO <table> (...) VALUES (...)`
  - Multiple value tuples allowed; literals only.
- `SELECT ... FROM <table>`
  - Single table, optional WHERE (supports `AND/OR`, comparisons, `BETWEEN`, `LIKE/ILIKE/RLIKE`, `IN`, `IS NULL`), arbitrary expression `ORDER BY` (with `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST`) plus `OFFSET`/`LIMIT`, aggregates in the projection, general `GROUP BY` with matching `HAVING`, `QUALIFY`, and `DISTINCT` (including alongside aggregates).
  - **Aggregates**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `VARIANCE`/`VAR_POP`/`VAR_SAMP`, `STDDEV`/`STDDEV_POP`/`STDDEV_SAMP`, `PERCENTILE_CONT`, `APPROX_QUANTILE` with optional `FILTER (WHERE ...)` clause and `DISTINCT` modifier. Conditional aggregates: `sumIf`, `avgIf`, `countIf`.
  - **Window functions**: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `SUM`, `FIRST_VALUE`, `LAST_VALUE` over `PARTITION BY`/`ORDER BY` with `ROWS` frames (`UNBOUNDED PRECEDING` or `N PRECEDING AND CURRENT ROW`) or `RANGE` frames (`interval PRECEDING AND CURRENT ROW` for `SUM` with single numeric ORDER BY).
  - **Time functions**: `TIME_BUCKET(interval, timestamp [, origin])`, `DATE_TRUNC(unit, timestamp)` supporting second/minute/hour/day/week units.
  - Projection list may mix plain columns with scalar expressions (arithmetic, CASE, built-in scalar/row functions like `ABS`, `ROUND`, `CEIL`, `FLOOR`, `EXP`, `LN`, `LOG`, `POWER`, `WIDTH_BUCKET`).
- `UPDATE <table> SET ... [WHERE ...]`
  - WHERE clause optional; predicates may target any columns. The engine updates every matching row, falling back to a scan when ORDER BY columns aren’t fully constrained.
- `DELETE FROM <table> [WHERE ...]`
  - WHERE clause optional; predicates share the same support as UPDATE.

### Not Yet Supported

- Joins, subqueries, CTEs, multi-table DML.
- `GROUP BY ROLLUP`/`GROUPING SETS` (infrastructure exists, not fully operational).
- `UNION`/set ops, `FETCH`, `LIMIT BY`.
- `INSERT ... SELECT`, default expressions, conflict handling.
- RETURNING clauses, multi-statement batches, transaction control.
- DDL beyond simple `CREATE TABLE` (no ALTER/DROP/constraints/temp tables).
- Time bucketing `WITH FILL`, multiple `COUNT(DISTINCT ...)` in single query.
