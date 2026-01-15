# Plan: Eliminate Row-by-Row Scalar Scanning in SELECT

## Goal
Remove all per-row scalar scanning in SELECT execution paths and replace them with
page-level + vectorized kernels. No `read_entry_at` or row-by-row fallback in scan.

---

## 1) Inventory Current Scalar Paths

**I/O + per-row fetch**
- `src/pipeline/types.rs` -> `materialize_column_in_batch`
  - Uses `read_entry_at` for each `row_id`.

**Scalar fallback evaluation**
- `src/pipeline/filtering.rs` -> `filter_rows_with_expr`
  - Evaluates row-by-row when vectorized predicate unsupported.

**Sort-key refinement**
- `src/sql/runtime/scan_helpers.rs` -> `locate_rows_by_sort_tuple`
  - Refinement step calls `read_entry_at` per candidate row.

**Projection materialization**
- `src/sql/runtime/projection_helpers.rs` -> `materialize_columns`
  - Builds `HashMap<row_id, value>` using row iterators.

**Predicate eval**
- `src/sql/runtime/physical_evaluator.rs`
  - Still loops per-row (now branchless bitmap building, but still scalar predicate evaluation).

---

## 2) Replace Per-row Materialization with Page-group Gathers

**Core idea:** group `row_ids` by page group, load each page once, gather in memory.

### Required API additions
- `ColumnarPage::gather(indices: &[usize]) -> ColumnarPage`
  - Builds a new page containing only selected rows (pure in-memory gather).

### Apply to
- `materialize_column_in_batch` (`src/pipeline/types.rs`)
- `materialize_columns` (`src/sql/runtime/projection_helpers.rs`)

---

## 3) Remove `read_entry_at` from SELECT scan path

**Target:** no `read_entry_at` calls during SELECT scanning.

- Update `locate_rows_by_sort_tuple` refinement:
  - Gather sort columns by page group, compare in batch.
- Any remaining `read_entry_at` uses become write-only or utility-only.

---

## 4) Remove Scalar Filter Fallback

**Current fallback:** `filter_rows_with_expr` (row-by-row) in `apply_filter_expr`.

**Change:**
- Expand vectorized expression support to cover all SELECT predicates.
- If unsupported predicate appears:
  - Option A: hard error (strict)
  - Option B: log warning + refuse fallback (safe)

---

## 5) Vectorized Predicate Kernels

**Numeric/boolean (highest ROI)**
- Keep word-level bitmap building (already done).
- Add SIMD-friendly loops for comparisons.

**Strings**
- Dictionary: key-based equality/inequality (already good).
- LIKE prefix: fast-path `starts_with`.
- Non-prefix LIKE/RLIKE stays slower, but no per-row I/O.

---

## 6) Guardrails / Verification

- Add a debug counter or assert in `read_entry_at` to fail if invoked from SELECT path.
- Optional: debug log when vectorized fallback is not possible.

---

## Expected Outcome

- No per-row I/O in scan.
- Materialization + filtering operate on page-level vectors and word bitmaps.
- Branchless/low-branch filtering and cache-friendly gathers.
