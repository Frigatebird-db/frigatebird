# Pipeline Migration Plan (src/sql/)

Goal: move all SQL execution onto the pipeline, preserve behavior, and make
execution flow easy to reason about.

## Scope
- SELECT, UPDATE, DELETE paths in `src/sql/`
- Projection, filter, aggregation, sort, distinct, limit/offset, window
- Remove legacy non-pipeline execution once parity is proven

## Current Audit (Non-Pipeline Hotspots)
- Projection: `src/sql/executor/select_exec.rs` (`execute_vectorized_projection`)
- Aggregation: `src/sql/executor/aggregation_exec.rs`
- Window: `src/sql/executor/select_exec.rs` + `src/sql/executor/window_helpers.rs`
- Sort: `src/sql/executor/mod.rs` + `src/sql/executor/ordering.rs`
- Distinct/Limit: `src/sql/executor/select_exec.rs` + `src/sql/executor/mod.rs`
- DML: `src/sql/executor/dml.rs` (row-wise mutations after scan)
- Materialization: `merge_stream_to_batch` used broadly

Pipeline today only covers scan + filter (`src/sql/executor/scan_stream.rs`,
`src/pipeline/types.rs`).

## Execution Mapping (Current)
### SELECT routing (src/sql/executor/select.rs)
- Window path:
  - `execute_vectorized_window_query` in `src/sql/executor/select_exec.rs`.
  - Requires explicit projection plan; uses in-memory sort for partitions.
  - Selection: only simple filter via `PhysicalExpr` (no row-wise fallback).
- Non-aggregate path:
  - `execute_vectorized_projection` in `src/sql/executor/select_exec.rs`.
  - Handles projection, distinct, limit/offset, qualify in executor.
  - Applies ordering via `OrderClause` comparisons.
- Aggregate path (simple group):
  - `execute_vectorized_aggregation` in `src/sql/executor/aggregation_exec.rs`.
  - Handles group-by, having, order, distinct, limit/offset.
- Aggregate path (grouping sets):
  - `execute_grouping_set_aggregation_rows` in `src/sql/executor/aggregation_exec.rs`.
  - Produces `AggregatedRow` vectors then applies distinct/order/limit manually.

### Scan/filter (src/sql/executor/scan_helpers_exec.rs)
- `build_pipeline_scan_stream` builds scan + filter pipeline for required ordinals.
- `scan_stream.rs` bridges pipeline output to executor batches.
- Row-id usage currently disabled for SELECT (row_ids dropped in select.rs).

### Ordering/Distinct/Limit (src/sql/executor/select_exec.rs, mod.rs)
- Ordering uses `OrderClause` + `compare_order_keys` in executor.
- Distinct uses `deduplicate_batches` or row vector dedupe for aggregation.
- Limit/offset uses `apply_limit_offset` in `src/sql/executor/mod.rs`.

### Aggregation (src/sql/executor/aggregation_exec.rs, aggregates.rs)
- Vectorized aggregation includes custom distinct handling and fallback checks.
- Advanced aggregate modifiers are not supported in vectorized path.

### Window (src/sql/executor/window_helpers.rs)
- Window planning, alias rewrites, and in-memory partition ordering.
- Window operator is executor-side; pipeline is only used for scan/filter.

### DML (src/sql/executor/dml.rs)
- UPDATE/DELETE derive row ids with executor-level filtering.
- Mutations are row-wise after filtering.

## Key Invariants to Capture
- Column ordinals: all stages expect full ordinals for ordering, grouping, and window
- Row IDs: pipeline batches carry `row_ids`; DML relies on stable row id ordering
- Ordering: `OrderClause` ties are stable (window helpers rely on tie-break)
- Null handling: `compare_order_keys` behavior must be preserved
- Filters: `PhysicalExpr` is partial; row-wise fallback must be explicit

## Immediate Next Actions (Step 1 output)
- Produce a table mapping each executor function to a future operator.
- Identify which helper functions can be moved as-is into operators.

## Executor -> Operator Mapping (Draft)
| Executor/Helper | Current Role | Target Operator Module |
| --- | --- | --- |
| `execute_vectorized_projection` (`src/sql/executor/select_exec.rs`) | Projection + distinct + limit/offset | `pipeline/operators/project.rs`, `distinct.rs`, `limit.rs` |
| `execute_vectorized_window_query` (`src/sql/executor/select_exec.rs`) | Window + partition sort | `pipeline/operators/window.rs` (Phase B) |
| `execute_vectorized_aggregation` (`src/sql/executor/aggregation_exec.rs`) | Group-by + aggregates + having + order | `pipeline/operators/aggregate.rs` |
| `execute_grouping_set_aggregation_rows` (`src/sql/executor/aggregation_exec.rs`) | Grouping sets, row aggregation | `pipeline/operators/aggregate.rs` (grouping sets) |
| `compare_order_keys` (`src/sql/executor/ordering.rs`) | Sort key comparator | `pipeline/operators/sort.rs` |
| `apply_limit_offset` (`src/sql/executor/mod.rs`) | Limit/offset slicing | `pipeline/operators/limit.rs` |
| `deduplicate_batches` (`src/sql/executor/select_exec.rs`) | Distinct by row key | `pipeline/operators/distinct.rs` |
| `WindowOperator` (`src/sql/executor/window_helpers.rs`) | Window function execution | `pipeline/operators/window.rs` |
| Row-wise eval (`src/sql/executor/physical_evaluator.rs`) | WHERE fallback | `pipeline/operators/filter.rs` (explicit fallback) |
| DML filter loop (`src/sql/executor/dml.rs`) | UPDATE/DELETE row matching | `pipeline/operators/filter.rs` + DML adapter |

## Target Operator Contract
- Single `ColumnarBatch` contract
- Explicit `row_ids` semantics
- Deterministic null handling and type behavior
- Streaming where possible; bounded buffering for sort/window/aggregate

## Plan (Detailed)

### 1) Inventory + Invariants (audit)
1.1 Map each query path to stages and file/function references.
1.2 Document invariants required by each stage:
    - column ordinals required
    - row_ids presence/absence rules
    - ordering guarantees
    - null handling and type conversions
1.3 Capture parity checklist (tests covering each behavior).

### 2) Operator Interface + Wiring
2.1 Define a minimal pipeline operator trait for non-scan steps.
2.2 Define batch metadata needed by all operators (row_ids, aliases).
2.3 Create operator registry modules:
    - `src/pipeline/operators/{filter,project,aggregate,sort,distinct,limit,window}.rs`
2.4 Add small integration tests per operator.

### 3) Project + Filter First
3.1 Implement Project operator using existing projection helpers.
3.2 Implement explicit Filter operator that:
    - uses PhysicalExpr when possible
    - falls back to row-wise eval as an explicit operator
3.3 Replace `execute_vectorized_projection` with:
    `Scan -> Filter -> Project -> Distinct -> LimitOffset`
3.4 Run `cargo test --test sql_e2e_tests` and `order_by_integration_tests`.

### 4) Aggregation Operator
4.1 Implement Aggregate operator (grouping + aggregates).
4.2 Support group-by, grouping sets, and HAVING via existing helpers.
4.3 Replace `execute_vectorized_aggregation` and
    `execute_grouping_set_aggregation_rows`.
4.4 Run `cargo test --test sql_e2e_tests` and aggregation-specific tests.

### 5) Sort / Distinct / Limit Operators
5.1 Implement Sort operator wrapping current sort/spill logic.
5.2 Implement Distinct operator (dedupe via row key hash).
5.3 Implement LimitOffset operator (stateful stream slicer).
5.4 Remove use of `merge_stream_to_batch` in SELECT path.

### 6) Window Operator (phased)
6.1 Phase A: keep window in executor but accept batches from pipeline.
6.2 Phase B: move window computation into a pipeline operator.
6.3 Preserve current ordering + tie-break semantics.

### 7) DML Refactor
7.1 UPDATE/DELETE should derive row_ids from pipeline output.
7.2 Keep mutation logic the same; only replace matching/filtering.
7.3 Remove row-wise filtering from executor (moved into Filter operator).

### 8) Cleanup + Consolidation
8.1 Remove/comment legacy non-pipeline paths after parity.
8.2 Consolidate helpers into operator modules.
8.3 Run full suite (including `sql_massive_correctness`).

## Parity Gates
- `cargo test --test sql_e2e_tests`
- `cargo test --test order_by_integration_tests`
- `cargo test --test edge_cases_and_stress_tests`
- `cargo test --test sql_massive_correctness`

## Notes
- Keep changes incremental; do not remove legacy paths until parity is proven.
- Maintain all current SQL features, including complex expressions and window
  functions.
