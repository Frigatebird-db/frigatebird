a distributed HTAP database that will mog everything that comes in its way

we are going to win

### Supported SQL (current scope)

- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`
  - Plain column definitions only; ORDER BY clause is mandatory.
- `INSERT INTO <table> (...) VALUES (...)`
  - Multiple value tuples allowed; literals only.
- `SELECT ... FROM <table>`
  - Single table, optional WHERE (supports `AND/OR`, comparisons, `BETWEEN`, `LIKE/ILIKE/RLIKE`, `IN`, `IS NULL`), arbitrary expression `ORDER BY` (with `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST`) plus `OFFSET`/`LIMIT`, aggregates in the projection, general `GROUP BY` with matching `HAVING`, `QUALIFY`, and `DISTINCT` (including alongside aggregates).
  - Projection list may mix plain columns with scalar expressions (arithmetic, CASE, built-in scalar functions) and the currently implemented window surface: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, and `SUM` over `PARTITION BY`/`ORDER BY` with either `ROWS UNBOUNDED PRECEDING` or `ROWS BETWEEN N PRECEDING AND CURRENT ROW`.
- `UPDATE <table> SET ... [WHERE ...]`
  - WHERE clause optional; predicates may target any columns. The engine updates every matching row, falling back to a scan when ORDER BY columns aren’t fully constrained.
- `DELETE FROM <table> [WHERE ...]`
  - WHERE clause optional; predicates share the same support as UPDATE.

### Not Yet Supported

- Joins, subqueries, CTEs, multi-table DML.
- Additional window functions beyond the current set, `UNION`/set ops.
- `FETCH`, `LIMIT BY`.
- `INSERT ... SELECT`, default expressions, conflict handling.
- RETURNING clauses, multi-statement batches, transaction control.
- DDL beyond simple `CREATE TABLE` (no ALTER/DROP/constraints/temp tables).
