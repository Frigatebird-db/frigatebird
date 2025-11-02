so basically currently we just use an empty string '' for non existent values instead of NULL, we need to add NULL support

and for the SELECT stuff on numerical columns, we need to add support for stuff like:

## Single-column numeric analytics in SQL

- `COUNT(*)`, `COUNT(col)` — total / non-null count
- `MIN(col)`, `MAX(col)` — range
- `SUM(col)` — total
- `AVG(col)` — mean
- `VARIANCE(col)`, `STDDEV(col)` — spread
- Percentiles / median:
  - `percentile_cont(0.5) WITHIN GROUP (ORDER BY col)`
  - `percentile_cont(0.9) WITHIN GROUP (ORDER BY col)` ...
- Histogram / bins:
  - `width_bucket(col, lo, hi, buckets)`
- Distinct values:
  - `COUNT(DISTINCT col)`
- Numeric transforms:
  - `ABS(col)`, `ROUND(col, n)`, `CEIL(col)`, `FLOOR(col)`
  - `LN(col)`, `LOG(...)`, `EXP(col)`, `POWER(col, k)`
- Null analysis:
  - `COUNT(*) - COUNT(col)` — null count
- Filtered counts on same col:
  - `SUM(CASE WHEN col > 0 THEN 1 ELSE 0 END)`
  - `SUM(CASE WHEN col BETWEEN a AND b THEN 1 ELSE 0 END)`
- Order for extremes:
  - `SELECT col FROM t ORDER BY col DESC LIMIT n;`


wdyt, how can we do it minimally, note that since these are just for SELECT, we can just quickly range scan in the pipeline, wdyt about it, tell me
