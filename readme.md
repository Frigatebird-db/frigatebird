a distributed HTAP database that will mog everything that comes in its way

we are going to win

### Supported SQL (current scope)

- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`
  - Plain column definitions only; ORDER BY clause is mandatory.
- `INSERT INTO <table> (...) VALUES (...)`
  - Multiple value tuples allowed; literals only.
- `SELECT ... FROM <table>`
  - Single table, optional WHERE (supports `AND/OR`, comparisons, `BETWEEN`, `LIKE/ILIKE/RLIKE`, `IN`, `IS NULL`), optional `ORDER BY` matching the table’s leading ORDER BY columns (ASC or DESC on the first column), optional `OFFSET`/`LIMIT`, aggregates in the projection, and basic `DISTINCT`.
- `UPDATE <table> SET ... [WHERE ...]`
  - WHERE clause optional; predicates may target any columns. The engine updates every matching row, falling back to a scan when ORDER BY columns aren’t fully constrained.
- `DELETE FROM <table> [WHERE ...]`
  - WHERE clause optional; predicates share the same support as UPDATE.

### Not Yet Supported

- Joins, subqueries, CTEs, multi-table DML.
- `DISTINCT` with aggregates, `GROUP BY`, `HAVING`, window functions, `UNION`/set ops.
- Descending or arbitrary `ORDER BY`, `NULLS FIRST/LAST`, `FETCH`, `LIMIT BY`.
- `INSERT ... SELECT`, default expressions, conflict handling.
- RETURNING clauses, multi-statement batches, transaction control.
- DDL beyond simple `CREATE TABLE` (no ALTER/DROP/constraints/temp tables).
