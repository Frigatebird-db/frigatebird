Standing up a minimal SELECT in this codebase boils down to adding a read-only twin to the mutating helpers that SqlExecutor already
  wraps. I’d keep it in the same module so we can reuse the private search routines (locate_rows_by_sort_tuple, row_matches_filters,
  extract_equality_filters) that already understand ORDER BY tables (src/sql/executor.rs:73, src/sql/executor.rs:523).

  Planned steps:

  - Public surface – add SqlExecutor::query(&self, sql) -> Result<SelectResult, SqlExecutionError> instead of overloading execute,
    so write-heavy callers don’t pay for result marshalling (src/sql/executor.rs:73). SelectResult should carry the projection order
    (Vec<String>) plus the rows.
  - Parsing / validation – reuse parse_sql and the existing AST walk to ensure we only accept Statement::Query with a single SELECT
    and one table (sqlparser AST from src/sql/executor.rs:85). For projection order we need the raw SelectItem list; the planner’s
    TableAccess.read_columns set loses ordering, so we’ll read projection straight from the AST.
  - Filter extraction – keep the current restrictions: require simple column = literal predicates on every ORDER BY column.
    extract_equality_filters and locate_rows_by_sort_tuple already implement this fast path and give us candidate row ids without
    scanning the table (src/sql/executor.rs:635 and src/sql/executor.rs:523).
  - Row materialisation – once we have the candidate row ids, fetch the rows in projection order. To stay fast:
      1. Sort/dedup the candidate ids, honour LIMIT/OFFSET if the query provides them (the AST is already available in scope).
      2. For each projected column, call PageHandler::list_range_in_table and get_pages to materialise the pages that cover the min..max
         row span exactly once (src/page_handler/mod.rs:312), then look up the individual row values by offset instead of calling
         read_entry_at repeatedly in a hot loop.
      3. Reuse the compare_strs helper for filter confirmation (src/sql/executor.rs:331) so numerics stay numerics.
  - Filter evaluation – factor the lightweight evaluator that already exists in the pipeline builder (src/pipeline/builder.rs:206) into a
    shared helper (e.g. pipeline::filter_eval). That gives us support for the simple comparison/LIKE operators without duplicating logic.
    For the minimal path we can fall back to row_matches_filters for the ORDER BY equality check and only run the evaluator when the
    WHERE clause contains additional predicates.
  - Result assembly – build Vec<Vec<String>> in projection order, cloning only the strings we actually return. With the pre-fetched pages
    we can push values straight from the cached entries into the output rows.
  - Unsupported branches – if the query lacks the ORDER BY predicates, references unknown columns, or includes joins/aggregation/
    RETURNING, bail out with SqlExecutionError::Unsupported just like the DML codepath (src/sql/executor.rs:23). That keeps behaviour
    predictable while we keep the implementation tight.

  This keeps the hot path limited to: parse → locate rows via binary search → batch materialise pages once → evaluate filters → stream
  out rows. It reuses the existing metadata/indexing logic, keeps allocations proportional to the result size, and avoids a full table
  scan in the common “lookup by primary key” pattern that INSERT/UPDATE/DELETE already rely on. Natural next steps after the minimal
  version land would be (1) broadening predicate support beyond equality, and (2) folding in pagination helpers (LIMIT/OFFSET) so clients
  don’t have to post-process rows themselves.
