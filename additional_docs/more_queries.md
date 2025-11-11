read the source in @src/ and the currently supported sql stuff in @readme.md


so we need to add support for these sql stuff:

Value-based frames: RANGE BETWEEN INTERVAL 'X' PRECEDING AND CURRENT ROW

LAG/LEAD, FIRST_VALUE/LAST_VALUE

Percentiles: PERCENTILE_CONT, APPROX_QUANTILE

Subtotals: GROUP BY ROLLUP(...) / GROUPING SETS(...)

Time bucketing + gap fill: date_trunc/time_bucket(...) + WITH FILL

Conditional aggregates: agg(x) FILTER (WHERE …) / sumIf/avgIf/countIf

Multi-distinct in one SELECT: COUNT(DISTINCT a), COUNT(DISTINCT b) together

I think we can piggyback most of this stuff off existing systems and helpers right ? 

note that this is a performance critical path and we need to implement these minimally, before making any change, tell me how you would go on about doing it
