---
title: "Pipeline Builder"
layout: default
nav_order: 10
---

# Pipeline Builder

The pipeline builder transforms a logical query plan into a sequential execution pipeline for filter operations. It takes the output from the query planner and generates step-by-step instructions for applying filters to table data.

## Purpose

When a query contains multiple filters on different columns (e.g., `WHERE age > 18 AND name = 'John' AND status = 'active'`), the pipeline builder determines the order in which these filters should be applied. Each step filters on a specific column, progressively reducing the working set of rows.

## Architecture

```
╔═══════════════════════════════════════════════════════════╗
║                      Query Planner                        ║
║  • Analyzes SQL AST                                       ║
║  • Produces QueryPlan with TableAccess + FilterExpr tree  ║
╚═══════════════════════╤═══════════════════════════════════╝
                        │ QueryPlan
                        ▼
╔═══════════════════════════════════════════════════════════╗
║                   Pipeline Builder                        ║
║  • Extracts leaf filters from FilterExpr tree             ║
║  • Groups filters by column                               ║
║  • Creates PipelineSteps + crossbeam channel chain        ║
║  • Attaches Pipeline metadata (id, next_free_slot, entry) ║
╚═══════════════════════╤═══════════════════════════════════╝
                        │ Vec<Job>
                        ▼
╔═══════════════════════════════════════════════════════════╗
║                Execution Job Chain                        ║
║  entry sender → Step 1 → chan → Step 2 → chan → ...       ║
║  Step 1: [column X filters] consumes receiver c0.recv     ║
║  Step 2: [column Y filters] consumes receiver c1.recv     ║
║  Step 3: [column Z filters] consumes receiver c2.recv     ║
╚═══════════════════════════════════════════════════════════╝
```

## Core Data Structures

### PipelineBatch

Represents the unit of work that flows through the crossbeam channels connecting each step. Right now it is a simple collection of row offsets; execution can evolve it into a richer type later without touching the builder.

```rust
pub type PipelineBatch = Vec<usize>;
```

### PipelineStep

Represents a single filtering step on one column, plus the channels that connect it to its neighbors:

```rust
pub struct PipelineStep {
    pub current_producer: Sender<PipelineBatch>,   // Sends batches to the next step
    pub previous_receiver: Receiver<PipelineBatch>,// Receives batches from the previous step
    pub column: String,                            // Column to filter
    pub filters: Vec<FilterExpr>,                  // Filters for this column
    pub is_root: bool,                             // True for the first step (synthetic input batch)
}
```

### Job

The complete execution plan for a table:

```rust
pub struct Job {
    pub table_name: String,
    pub steps: Vec<PipelineStep>,
    pub cost: usize,
    pub next_free_slot: AtomicUsize,
    pub id: String,
    pub entry_producer: Sender<PipelineBatch>,
}
```

The `entry_producer` feeds the very first step (`c0 → c1`), while each `PipelineStep` carries the producer for its downstream neighbor (`ci → c(i+1)`) and the receiver for the upstream neighbor (`c(i-1) → ci`). This results in a chain such as `[c1.prod, c0.recv] - [c2.prod, c1.recv] …`, allowing batches to flow strictly forward without extra lookups. The `cost` field caches the step count for quick heuristics, while `next_free_slot` (AtomicUsize, initialized to zero) and `id` (random string) help the runtime track and trace jobs with negligible overhead.

## Building a Pipeline

The `build_pipeline` function converts a `QueryPlan` into a `Vec<Job>`:

1. **Extract leaf filters**: Traverse the `FilterExpr` tree (AND/OR nodes) to collect all leaf filter expressions
2. **Group by column**: Analyze each filter to determine which column it primarily operates on, then group filters by column
3. **Create steps**: Convert each column group into a `PipelineStep`
4. **Wire channels & metadata**: Create a single crossbeam chain (`entry → step0 → step1 …`), assign each step its producer/receiver pair, and attach the entry producer, random ID, and zeroed `next_free_slot` to the parent `Job`

During step creation, the builder wires the crossbeam channels:

1. Allocate the initial `(Sender, Receiver)` pair for the pipeline entry
2. For each grouped column, create a new `(Sender, Receiver)` pair
3. Store the previous receiver and the new sender in the step
4. Pass the new receiver forward so the next step becomes its owner

This single pass keeps allocations to the minimum required channel endpoints.

## Pipeline Executor

Once jobs are built, the pipeline executor (`src/executor.rs`) coordinates their execution using two worker pools backed by the existing scheduler:

- **Main workers (≈85%)**: Block on a crossbeam MPMC queue of `Job`s. When a job arrives, the worker pushes it onto the shared `JobBoard` (a `crossbeam_skiplist::SkipSet` ordered by cost), emits a lightweight wake-up signal, and calls `job.get_next()`.
- **Reserve workers (≈15%)**: Block on the wake-up channel. When signaled, a reserve worker steals the most expensive job from the `JobBoard` and also calls `get_next()`, intentionally duplicating execution for intense workloads.
- **JobBoard**: A lock-free skiplist that keeps pointers to in-flight jobs sorted by `cost`, enabling constant awareness of the heaviest work.
- **Job::get_next()**: Each worker calls this, which CAS-es `next_free_slot` to claim the next `PipelineStep` and runs its `execute()` method (currently a stub that just forwards row-id batches along the channel chain).

The executor exposes a minimal API: initialize it with a thread budget (it splits the pool 85/15) and submit jobs via `submit(job)`. Everything else—channel wiring, wake-ups, job stealing, and per-step work—runs inside the pools with zero external locking.

`Job::get_next()` is the core entry point for workers. It uses a compare-and-swap loop on `next_free_slot` so that exactly one worker claims each step before calling `PipelineStep::execute()`. A successful claim immediately runs the step and returns; a failure simply retries with the updated slot index. Root steps synthesize a placeholder "all rows" batch so implementations can treat every step uniformly. The current `PipelineStep::execute()` is a stub that forwards batches to the next channel, but the structure is in place for column-level filtering.

### Job::get_next() Detailed Implementation

```rust
pub fn get_next(&self) {
    let total = self.steps.len();
    if total == 0 {
        return;
    }

    let mut slot = self.next_free_slot.load(AtomicOrdering::Relaxed);
    loop {
        if slot >= total {
            return;  // All steps completed
        }

        match self.next_free_slot.compare_exchange_weak(
            slot,              // Expected value
            slot + 1,          // New value
            AtomicOrdering::AcqRel,    // Success ordering
            AtomicOrdering::Relaxed,   // Failure ordering
        ) {
            Ok(_) => {
                // CAS succeeded - we claimed this slot
                self.steps[slot].execute();
                return;
            }
            Err(current) => {
                // CAS failed - another worker claimed this slot
                // Retry with updated slot value
                slot = current;
            }
        }
    }
}
```

**CAS Guarantees:**
- Exactly one worker claims each step (slot)
- Multiple workers can work on different steps of the same job
- No locks needed - compare_exchange_weak provides atomicity
- Weak variant allows spurious failures but is faster on some architectures
- AcqRel ordering ensures proper memory visibility between workers

**Execution Flow Example:**

```
Job with 4 steps, 2 workers (A and B)

T0: Worker A calls get_next()
    - slot = 0, CAS(0→1) succeeds
    - Executes step 0

T1: Worker B calls get_next()
    - slot = 1, CAS(1→2) succeeds
    - Executes step 1

T2: Worker A calls get_next()
    - slot = 2, CAS(2→3) succeeds
    - Executes step 2

T3: Worker A calls get_next()
    - slot = 3, CAS(3→4) succeeds
    - Executes step 3

T4: Worker B calls get_next()
    - slot = 4, slot >= total, returns (job complete)

Result: Steps 0,1,2,3 executed exactly once, in arbitrary order by workers
```

For complete details on the executor's dual-pool work-stealing architecture, see [executor](executor).

### Example

For the query:
```sql
SELECT id FROM users WHERE age > 18 AND age < 65 AND name = 'John'
```

The pipeline builder produces:

```
Job {
  table_name: "users",
  cost: 2,
  next_free_slot: AtomicUsize::new(0),
  id: "pipe-x9s…",
  entry_producer: c0.prod,
  steps: [
    PipelineStep {
        current_producer: c1.prod,
        previous_receiver: c0.recv,
        column: "age",
        filters: [age > 18, age < 65],
        is_root: true,
    },
    PipelineStep {
        current_producer: c2.prod,
        previous_receiver: c1.recv,
        column: "name",
        filters: [name = 'John'],
        is_root: false,
    }
  ]
}
```

Execution would:
1. Apply both age filters to all rows
2. Apply the name filter to rows that passed step 1

## Limitations

- Steps are in random order, no cost-based ordering
- Single column per step
- Sequential execution

## Module Location

- **Source**: `src/pipeline/builder.rs`
- **Module**: `src/pipeline/mod.rs`
- **Tests**: `tests/pipeline_tests.rs`

