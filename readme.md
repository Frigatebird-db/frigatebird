a distributed HTAP database that will mog everything that comes in its way

we are going to win

### Supported SQL (current scope)

- `CREATE TABLE <name> (...) ORDER BY <col[, ...]>`
  - Plain column definitions only; ORDER BY clause is mandatory.
- `INSERT INTO <table> (...) VALUES (...)`
  - Multiple value tuples allowed; literals only.
- `SELECT ... FROM <table>`
  - Single table, optional WHERE (supports `AND/OR`, comparisons, `BETWEEN`, `LIKE/ILIKE/RLIKE`, `IN`, `IS NULL`), optional `ORDER BY` matching the table’s ORDER BY prefix (ASC), optional `OFFSET`/`LIMIT`, aggregates allowed in projection.
- `UPDATE <table> SET ... WHERE ...`
  - WHERE clause required; predicates may reference any columns. Engine falls back to a scan when ORDER BY columns aren’t fully constrained.
- `DELETE FROM <table> WHERE ...`
  - WHERE clause required; same predicate support as UPDATE.

### Not Yet Supported

- Joins, subqueries, CTEs, multi-table DML.
- `DISTINCT`, `GROUP BY`, `HAVING`, window functions, `UNION`/set ops.
- Descending or arbitrary `ORDER BY`, `NULLS FIRST/LAST`, `FETCH`, `LIMIT BY`.
- `INSERT ... SELECT`, default expressions, conflict handling.
- RETURNING clauses, multi-statement batches, transaction control.
- DDL beyond simple `CREATE TABLE` (no ALTER/DROP/constraints/temp tables).
