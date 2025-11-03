a distributed HTAP database that will mog everything that comes in its way

we are going to win

### Supported SQL (current scope)

- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`
  - Plain column definitions only; ORDER BY clause is mandatory.
- `INSERT INTO <table> (...) VALUES (...)`
  - Multiple value tuples allowed; literals only.
- `SELECT ... FROM <table>`
  - Single table, optional WHERE (supports `AND/OR`, comparisons, `BETWEEN`, `LIKE/ILIKE/RLIKE`, `IN`, `IS NULL`), arbitrary expression `ORDER BY` with `OFFSET`/`LIMIT`, aggregates in the projection, general `GROUP BY` with matching `HAVING`, and basic `DISTINCT`.
  - Projection list may mix plain columns with scalar expressions (arithmetic, CASE, built-in scalar functions) and minimal window functions (`ROW_NUMBER(...)`, `SUM(...) OVER (PARTITION BY ... ORDER BY ... ROWS UNBOUNDED PRECEDING)`).
- `UPDATE <table> SET ... [WHERE ...]`
  - WHERE clause optional; predicates may target any columns. The engine updates every matching row, falling back to a scan when ORDER BY columns aren’t fully constrained.
- `DELETE FROM <table> [WHERE ...]`
  - WHERE clause optional; predicates share the same support as UPDATE.

### Not Yet Supported

- Joins, subqueries, CTEs, multi-table DML.
- `DISTINCT` with aggregates, additional window functions beyond the minimal set, `UNION`/set ops.
- `NULLS FIRST/LAST`, `FETCH`, `LIMIT BY`.
- `INSERT ... SELECT`, default expressions, conflict handling.
- RETURNING clauses, multi-statement batches, transaction control.
- DDL beyond simple `CREATE TABLE` (no ALTER/DROP/constraints/temp tables).
