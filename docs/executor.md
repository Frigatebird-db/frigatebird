---
title: "Pipeline Executor"
layout: default
nav_order: 11
---

# Pipeline Executor

The Pipeline Executor is the core query execution engine that orchestrates parallel processing of filter-based query pipelines. It uses a sophisticated dual-pool work-stealing architecture to efficiently distribute work across threads, with lock-free coordination via atomic operations.

---

## Purpose

When the pipeline builder creates Jobs (collections of PipelineSteps), the executor must:

1. **Distribute work efficiently** across multiple threads
2. **Parallelize expensive jobs** by allowing multiple workers to execute different steps simultaneously
3. **Prioritize heavy workloads** so they don't block the queue
4. **Coordinate without locks** using atomic compare-and-swap operations
5. **Balance throughput and latency** with separate main and reserve worker pools

---

## High-Level Architecture

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                           PIPELINE EXECUTOR                                   ║
║                                                                               ║
║  ┌────────────────┐                                                           ║
║  │ Client submits │  executor.submit(job)                                     ║
║  │ Job            │ ─────────────────────┐                                    ║
║  └────────────────┘                      │                                    ║
║                                          ▼                                    ║
║                              ┌─────────────────────┐                          ║
║                              │ job_tx (MPMC)       │                          ║
║                              │ Unbounded channel   │                          ║
║                              └─────────┬───────────┘                          ║
║                                        │                                      ║
║                    ┌───────────────────┴──────────────────┐                   ║
║                    │                                      │                   ║
║                    ▼                                      ▼                   ║
║        ╔═══════════════════════╗             ╔═══════════════════════╗        ║
║        ║  MAIN WORKER POOL     ║             ║  RESERVE WORKER POOL  ║        ║
║        ║  (85% of threads)     ║             ║  (15% of threads)     ║        ║
║        ╚═══════════════════════╝             ╚═══════════════════════╝        ║
║                    │                                      │                   ║
║                    │ 1. Insert JobHandle                  │                   ║
║                    │    into JobBoard                     │                   ║
║                    │                                      │                   ║
║                    │ 2. Send wake signal ────────────────▶│                   ║
║                    │    to reserve workers                │                   ║
║                    │                                      │                   ║
║                    │ 3. Start executing job               │                   ║
║                    │    via job.get_next()                │                   ║
║                    │                                      │                   ║
║                    ▼                                      ▼                   ║
║        ┌─────────────────────┐             ┌─────────────────────┐            ║
║        │ CAS claim step 0    │             │ Wait for wake signal│            ║
║        │ Execute filters     │             │ Pop heaviest job    │            ║
║        │ Forward batch       │             │ CAS claim step N    │            ║
║        └─────────────────────┘             └─────────────────────┘            ║
║                    │                                      │                   ║
║                    └──────────────┐          ┌────────────┘                   ║
║                                   ▼          ▼                                ║
║                         ┌──────────────────────────┐                          ║
║                         │  JOBBOARD (SkipSet)      │                          ║
║                         │  Sorted by cost (desc)   │                          ║
║                         │  [Job7, Job3, Job1]      │                          ║
║                         └──────────────────────────┘                          ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

**Key Design Decisions:**
- **Dual-pool architecture** separates job intake (main) from heavy workload handling (reserve)
- **Lock-free JobBoard** uses SkipSet for concurrent priority queue operations
- **CAS-based coordination** ensures exactly-once step execution without mutexes
- **Work stealing** allows reserve workers to assist with expensive jobs

---

## Core Components

### 1. PipelineExecutor

The main coordinator struct:

```rust
pub struct PipelineExecutor {
    job_tx: Sender<Arc<Job>>,              // Channel to send jobs to main workers
    wake_tx: Sender<()>,                   // Channel to wake reserve workers
    job_board: Arc<SkipSet<JobHandle>>,    // Shared priority queue of active jobs
    seq: Arc<AtomicU64>,                   // Monotonic sequence number generator
    main_pool: ThreadPool,                 // Pool of main workers (85%)
    reserve_pool: ThreadPool,              // Pool of reserve workers (15%)
}
```

**Initialization Flow:**

```
PipelineExecutor::new(total_threads: usize)
      │
      ├─▶ Step 1: Calculate pool sizes
      │     split_threads(total) → (main_count, reserve_count)
      │     Example: 16 threads → (14 main, 2 reserve)
      │
      ├─▶ Step 2: Create communication channels
      │     • job_tx/job_rx: Unbounded MPMC for job submission
      │     • wake_tx/wake_rx: Unbounded MPMC for wake signals
      │
      ├─▶ Step 3: Create shared data structures
      │     • JobBoard: Arc<SkipSet<JobHandle>> (sorted by cost)
      │     • Sequence counter: Arc<AtomicU64::new(0)>
      │
      ├─▶ Step 4: Spawn main worker pool
      │     main_pool = ThreadPool::new(main_count)
      │     for each worker:
      │         Clone: job_rx, board, seq, wake_tx
      │         Execute: run_main_worker(job_rx, board, seq, wake_tx)
      │
      └─▶ Step 5: Spawn reserve worker pool
            reserve_pool = ThreadPool::new(reserve_count)
            for each worker:
                Clone: wake_rx, board
                Execute: run_reserve_worker(wake_rx, board)
```

---

### 2. JobHandle

A wrapper that adds ordering semantics to Jobs for the JobBoard priority queue:

```rust
struct JobHandle {
    seq: u64,        // Monotonic sequence number (submission order)
    job: Arc<Job>,   // Reference-counted job
}
```

**Ordering Implementation:**

```rust
impl Ord for JobHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.cost().cmp(&other.cost()) {
            Ordering::Equal => self.seq.cmp(&other.seq),  // Tie-breaker
            ord => ord,  // Primary: by cost
        }
    }
}
```

**Priority Rules:**
1. **Primary:** Higher cost = higher priority
2. **Secondary:** Lower sequence number = higher priority (FIFO for same cost)

**Example Ordering:**

```
Job A: cost=10, seq=1  │
Job B: cost=10, seq=2  │  Same cost → compare seq
Job C: cost=5,  seq=3  ├─ A > B > C > D
Job D: cost=1,  seq=4  │

SkipSet internal order: [D, C, B, A]
pop_back() returns: A (highest priority)
```

---

### 3. Thread Pool Split Strategy

```rust
fn split_threads(total: usize) -> (usize, usize) {
    let mut main = (total * 85 + 99) / 100;  // Round up to 85%

    // Ensure at least 1 main and 1 reserve
    if main == 0 { main = 1; }
    if main >= total { main = total - 1; }

    let mut reserve = total - main;
    if reserve == 0 {
        reserve = 1;
        if main > 1 { main -= 1; }
    }

    (main, reserve)
}
```

**Split Table:**

| Total Threads | Main Workers | Reserve Workers | Ratio |
|---------------|--------------|-----------------|-------|
| 2 | 1 | 1 | 50/50 (minimum) |
| 4 | 3 | 1 | 75/25 |
| 8 | 7 | 1 | 87.5/12.5 |
| 16 | 14 | 2 | 87.5/12.5 |
| 32 | 27 | 5 | 84.4/15.6 |
| 100 | 85 | 15 | 85/15 (target) |

**Rationale:**
- **Main workers (85%)** process the steady stream of incoming jobs
- **Reserve workers (15%)** provide burst capacity for expensive, high-cost jobs
- Balance between **throughput** (main) and **latency** (reserve)

---

## Detailed Execution Flow

### Job Submission

```
Client Code
      │
      ├─▶ executor.submit(job)
      │
      ▼
PipelineExecutor::submit(&self, job: Job)
      │
      ├─▶ Arc::new(job)  (wrap in Arc for sharing)
      │
      └─▶ job_tx.send(Arc<Job>)
            │
            └─▶ Unbounded MPMC channel
                  └─▶ Any main worker can receive
```

**Properties:**
- **Non-blocking:** Sender never blocks (unbounded channel)
- **Shared ownership:** Arc allows multiple workers to hold references
- **Best-effort:** If channel closed (executor shutdown), send fails silently

---

### Main Worker Loop

Each main worker runs this loop on a dedicated thread:

```
fn run_main_worker(
    job_rx: Receiver<Arc<Job>>,
    board: Arc<SkipSet<JobHandle>>,
    seq: Arc<AtomicU64>,
    wake_tx: Sender<()>,
)

LOOP:
      │
      ├─▶ STEP 1: Block waiting for job
      │     job = job_rx.recv()?
      │     └─▶ Blocks until job arrives or channel closes
      │
      ├─▶ STEP 2: Generate sequence number
      │     seq_num = seq.fetch_add(1, Relaxed)
      │     └─▶ Atomic increment, returns old value
      │
      ├─▶ STEP 3: Wrap job in JobHandle
      │     handle = JobHandle::new(Arc::clone(&job), seq_num)
      │     └─▶ Maintains cost-based ordering
      │
      ├─▶ STEP 4: Insert into JobBoard
      │     board.insert(handle)
      │     └─▶ SkipSet automatically maintains sorted order
      │           O(log n) insertion time
      │
      ├─▶ STEP 5: Wake reserve workers (best-effort)
      │     wake_tx.send(())
      │     └─▶ Signal that work is available
      │
      └─▶ STEP 6: Start executing job immediately
            job.get_next()
            └─▶ CAS claim first available step and execute
```

**Visual Timeline:**

```
Main Worker Thread
─────────────────────────────────────────────────────────────────────

T0:  │ Blocked on job_rx.recv()
     │
T1:  ├─▶ Job arrives! recv() returns
     │
T2:  ├─▶ seq.fetch_add(1) → seq=42
     │
T3:  ├─▶ Create JobHandle { seq:42, job:Arc }
     │
T4:  ├─▶ board.insert(handle)
     │   └─▶ JobBoard: [Job1(cost=3), Job2(cost=5), ThisJob(cost=7)]
     │                                                  ▲ Inserted here
     │
T5:  ├─▶ wake_tx.send(()) ─────────────────────────────┐
     │                                                  │
T6:  └─▶ job.get_next()                                │
          ├─▶ CAS: next_free_slot(0 → 1) ✓             │
          ├─▶ Execute step[0]                          │
          │   • Apply filters                          ▼
          │   • Send batch to next step       Reserve Worker wakes
          │
          └─▶ Continue to next step...
```

---

### Reserve Worker Loop

Each reserve worker runs this loop:

```
fn run_reserve_worker(
    wake_rx: Receiver<()>,
    board: Arc<SkipSet<JobHandle>>,
)

LOOP:
      │
      ├─▶ STEP 1: Block waiting for wake signal
      │     wake_rx.recv()?
      │     └─▶ Sleeps until woken by main worker
      │
      ├─▶ STEP 2: Steal heaviest job from JobBoard
      │     job = select_heaviest(&board)?
      │           │
      │           ├─▶ board.pop_back()
      │           │   └─▶ Remove and return highest-cost job
      │           │       (back of sorted set)
      │           │
      │           └─▶ Extract Arc<Job> from JobHandle
      │
      └─▶ STEP 3: Execute job
            job.get_next()
            └─▶ CAS claim next available step and execute
```

**Visual Timeline:**

```
Reserve Worker Thread
─────────────────────────────────────────────────────────────────────

T0:  │ Blocked on wake_rx.recv()
     │ (sleeping, consuming no CPU)
     │
T1:  ├─▶ Wake signal arrives! recv() returns
     │
T2:  ├─▶ board.pop_back()
     │   │
     │   │ Before: [Job1(cost=3), Job2(cost=5), Job3(cost=7)]
     │   │                                           ▲ pop_back()
     │   │ After:  [Job1(cost=3), Job2(cost=5)]
     │   │
     │   └─▶ Returns Job3 (highest cost)
     │
T3:  └─▶ job.get_next()
          ├─▶ CAS: next_free_slot(3 → 4) ✓
          │   (Main worker already claimed steps 0-2)
          │
          ├─▶ Execute step[3]
          │   • Apply filters
          │   • Send batch to next step
          │
          └─▶ Continue to next step...
```

**Key Properties:**
- **Idle when no work:** Blocked on channel, zero CPU usage
- **Steals heaviest jobs:** Always takes highest-cost job (most steps remaining)
- **Can duplicate work:** May steal job that main worker is still processing (intentional!)

---

## CAS-Based Step Execution

The core coordination mechanism that ensures exactly-once step execution:

### Job Structure

```rust
pub struct Job {
    pub table_name: String,
    pub steps: Vec<PipelineStep>,           // All pipeline steps
    pub cost: usize,                        // Number of steps (cached)
    pub next_free_slot: AtomicUsize,        // Index of next unclaimed step
    pub id: String,                         // Random identifier
    pub entry_producer: Sender<PipelineBatch>,  // Input to first step
}
```

### Job::get_next() Implementation

```rust
pub fn get_next(&self) {
    let total = self.steps.len();
    if total == 0 { return; }

    let mut slot = self.next_free_slot.load(Ordering::Relaxed);

    loop {
        // Check if all steps completed
        if slot >= total { return; }

        // Attempt to claim this slot
        match self.next_free_slot.compare_exchange_weak(
            slot,                          // Expected: current slot
            slot + 1,                      // Desired: next slot
            Ordering::AcqRel,              // Success ordering
            Ordering::Relaxed,             // Failure ordering
        ) {
            Ok(_) => {
                // SUCCESS: We claimed this slot!
                self.steps[slot].execute();
                return;
            }
            Err(current) => {
                // FAILURE: Another worker claimed it first
                // Update slot to current value and retry
                slot = current;
            }
        }
    }
}
```

### CAS Visualization

**Scenario:** 2 workers competing for steps in a job with 5 steps:

```
Initial State:
┌──────────────────────────────────────┐
│ Job                                  │
│  steps: [Step0, Step1, Step2, ...]  │
│  next_free_slot: AtomicUsize(0) ────┼─── Both workers read this
└──────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
    Worker A                Worker B
        │                       │
        │ slot = 0              │ slot = 0
        │                       │
        │ CAS(0 → 1)            │ CAS(0 → 1)
        │ expect: 0             │ expect: 0
        │ update: 1             │ update: 1
        │                       │
        ├─▶ SUCCEEDS ✓          ├─▶ FAILS ✗
        │   next_free_slot=1    │   (already changed to 1)
        │                       │
        │ Execute step[0]       │ slot = 1 (from failure)
        │                       │
        │                       │ CAS(1 → 2)
        │                       │ expect: 1
        │                       │ update: 2
        │                       │
        │                       ├─▶ SUCCEEDS ✓
        │                       │   next_free_slot=2
        │                       │
        │                       │ Execute step[1]
```

**Result:**
- Worker A executes step[0]
- Worker B executes step[1]
- No step executed twice
- No step skipped
- Zero locks used

---

## Parallel Work Stealing in Action

### Example: Expensive Job with 10 Steps

```
TIMELINE: Job with cost=10 submitted to executor

T0: Main Worker receives job
    ┌──────────────────────────────────┐
    │ Job                              │
    │  steps: [0,1,2,3,4,5,6,7,8,9]    │
    │  next_free_slot: 0               │
    └──────────────────────────────────┘
         │
         ├─▶ Insert into JobBoard
         ├─▶ Send wake signal
         └─▶ Start executing

T1: Main Worker claims step 0
    next_free_slot: 0 → 1 (CAS success)
    ┌─────────────┐
    │ Execute S0  │  Main Worker
    └─────────────┘

T2: Reserve Worker wakes up
    ┌─────────────────────┐
    │ pop_back(JobBoard)  │  Reserve Worker
    │ → Gets same Job!    │
    └─────────────────────┘

T3: Main Worker claims step 1
    next_free_slot: 1 → 2 (CAS success)
    ┌─────────────┐
    │ Execute S1  │  Main Worker
    └─────────────┘

T4: Reserve Worker claims step 2
    next_free_slot: 2 → 3 (CAS success)
    ┌─────────────┐
    │ Execute S2  │  Reserve Worker
    └─────────────┘

T5: Main Worker claims step 3
    Reserve Worker claims step 4
    (Both succeed - different slots)
    ┌─────────────┐  ┌─────────────┐
    │ Execute S3  │  │ Execute S4  │
    └─────────────┘  └─────────────┘
     Main Worker      Reserve Worker

T6-T9: Workers continue racing
    Both workers call get_next() repeatedly
    CAS ensures each step claimed by exactly one worker

T10: All steps completed
     next_free_slot: 10 (>= steps.len())
     Both workers return from get_next()
```

**Performance Improvement:**
- **Serial execution (1 worker):** 10 time units
- **Parallel execution (2 workers):** ~5-6 time units
- **Speedup:** ~1.8x (nearly 2x with perfect work distribution)

---

## JobBoard Dynamics

The JobBoard is a concurrent priority queue implemented as `SkipSet<JobHandle>`:

### SkipSet Properties

```
╔═══════════════════════════════════════════════════════════════╗
║                      SkipSet Structure                        ║
║                                                               ║
║  Level 3:  [     A    ───────────────────────▶    D    ]     ║
║                 │                                   │         ║
║  Level 2:  [ A  ─┴──▶  B  ────────▶  C  ────────▶  D  ]     ║
║                        │            │             │         ║
║  Level 1:  [ A ───▶ B ─┴─▶ C ──▶ D ─┴─▶ E ──▶ F ─┴─▶ G ]    ║
║             ▲                                          ▲      ║
║           front()                                  back()     ║
║        (lowest cost)                           (highest cost) ║
╚═══════════════════════════════════════════════════════════════╝
```

- **Sorted order:** Maintained automatically by Ord implementation
- **Concurrent operations:** Lock-free insert/remove
- **Logarithmic time:** O(log n) insert, O(log n) pop
- **pop_back():** Removes and returns highest-cost job

### JobBoard Lifecycle Example

```
State 0: Empty
JobBoard: []

─── Main Worker receives Job A (cost=5, seq=0) ───

State 1:
JobBoard: [A(5,0)]
          └─▶ A inserted via board.insert()

─── Main Worker receives Job B (cost=3, seq=1) ───

State 2:
JobBoard: [B(3,1), A(5,0)]
           ▲        ▲
         lower    higher
         cost     cost

─── Reserve Worker pops heaviest ───

State 3:
handle = board.pop_back()  // Returns A(5,0)
JobBoard: [B(3,1)]

─── Main Worker receives Job C (cost=7, seq=2) ───

State 4:
JobBoard: [B(3,1), C(7,2)]
                    ▲
                  highest

─── Reserve Worker pops heaviest ───

State 5:
handle = board.pop_back()  // Returns C(7,2)
JobBoard: [B(3,1)]

─── Job B completes, removed from active set ───

State 6:
JobBoard: []
```

**Important:** Jobs may be executing even after removed from JobBoard (workers hold Arc references)

---

## Channel Communication

### Job Channel (MPMC Unbounded)

```
job_tx (Sender)  ────▶  CHANNEL  ────▶  job_rx (Receiver)
     ▲                                        │
     │                                        ├─▶ Main Worker 1
     │                                        ├─▶ Main Worker 2
     │                                        ├─▶ Main Worker 3
     │                                        └─▶ ...
executor.submit()
```

**Properties:**
- **Unbounded:** Never blocks sender
- **MPMC:** Multiple producers (clients), multiple consumers (main workers)
- **Fairness:** Workers compete for jobs (no starvation)

### Wake Channel (MPMC Unbounded)

```
wake_tx (Sender)  ────▶  CHANNEL  ────▶  wake_rx (Receiver)
     ▲                                        │
     │                                        ├─▶ Reserve Worker 1
     │                                        ├─▶ Reserve Worker 2
     │                                        └─▶ ...
     │
Main Workers
send on every job
```

**Properties:**
- **Best-effort:** If channel full or receiver busy, signal may be dropped (ok!)
- **Coalescing:** Multiple wake signals collapsed into one
- **Lightweight:** Just a unit `()` value

---

## Memory Ordering and Atomics

### Sequence Number Generation

```rust
seq.fetch_add(1, Ordering::Relaxed)
```

**Relaxed ordering is safe because:**
- Only used for ordering tie-breaker (not for synchronization)
- Each thread gets a unique value (atomicity guaranteed)
- No happens-before relationships required

### CAS in Job::get_next()

```rust
compare_exchange_weak(
    expected,
    new,
    Ordering::AcqRel,    // Success ordering
    Ordering::Relaxed,   // Failure ordering
)
```

**AcqRel on success:**
- **Acquire:** See all writes from previous worker that claimed earlier slot
- **Release:** Publish step execution completion to future workers

**Relaxed on failure:**
- Just need to reload current value and retry
- No synchronization needed for failed CAS

---

## Complete Example: Two Workers, Four Steps

```
Job submitted with 4 steps: [S0, S1, S2, S3]
Workers: Main Worker (MW), Reserve Worker (RW)

═══════════════════════════════════════════════════════════════════

T0: Job arrives at MW
    MW: job_rx.recv() → Job{steps:[S0,S1,S2,S3], next_free_slot:0}

T1: MW creates JobHandle
    handle = JobHandle { seq: 42, job: Arc<Job>, cost: 4 }

T2: MW inserts into JobBoard
    JobBoard.insert(handle)
    JobBoard: [handle]

T3: MW sends wake signal
    wake_tx.send(()) → Queued in wake channel

T4: MW starts executing
    MW: job.get_next()
        slot = 0
        CAS(0 → 1) ✓ Success!

    State: next_free_slot = 1
           JobBoard: [handle]
           MW executing S0
           RW still asleep

T5: RW wakes up
    RW: wake_rx.recv() → Got signal!

T6: RW steals job from JobBoard
    RW: board.pop_back()
        → Returns handle
        → JobBoard now empty: []

    RW: job = handle.into_job()
        → RW now has Arc<Job>

T7: MW finishes S0, claims S1
    MW: job.get_next()
        slot = 1
        CAS(1 → 2) ✓ Success!

    State: next_free_slot = 2
           MW executing S1

T8: RW claims S2
    RW: job.get_next()
        slot = 2
        CAS(2 → 3) ✓ Success!

    State: next_free_slot = 3
           MW executing S1
           RW executing S2

T9: MW finishes S1, claims S3
    MW: job.get_next()
        slot = 3
        CAS(3 → 4) ✓ Success!

    State: next_free_slot = 4
           MW executing S3
           RW executing S2

T10: RW finishes S2, tries to claim S4
     RW: job.get_next()
         slot = 4
         if 4 >= 4: return  ← Job complete!

     RW: Returns, goes back to waiting

T11: MW finishes S3, tries to claim S5
     MW: job.get_next()
         slot = 4
         if 4 >= 4: return  ← Job complete!

     MW: Returns, waits for next job

═══════════════════════════════════════════════════════════════════

FINAL STATE:
  - All 4 steps executed exactly once
  - MW executed: S0, S1, S3
  - RW executed: S2
  - Total time: ~60% of serial execution (parallelism benefit)
```

---

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| submit(job) | O(1) | Channel send |
| JobBoard insert | O(log n) | SkipSet insert |
| JobBoard pop_back | O(log n) | SkipSet remove |
| CAS claim step | O(1) | Atomic operation |
| Step execution | O(m) | m = rows to filter |

### Space Complexity

- **JobBoard:** O(n) where n = active jobs
- **Channels:** O(k) where k = queued jobs/signals
- **Arc<Job>:** Shared, reference counted
- **Per-worker stack:** O(1) fixed size

### Scalability

**Amdahl's Law Applied:**
- Jobs with few steps (cost < 5): Limited parallelism benefit
- Jobs with many steps (cost > 10): Near-linear speedup with 2+ workers
- Bottleneck: Serial portion = job submission + JobBoard operations

**Measured Speedup (example):**
- 1 worker: 100% (baseline)
- 2 workers: ~180% (1.8x speedup for high-cost jobs)
- 4 workers: ~320% (3.2x speedup for high-cost jobs)
- 8 workers: ~560% (5.6x speedup for high-cost jobs)

Diminishing returns due to:
- CAS contention on `next_free_slot`
- Channel synchronization overhead
- JobBoard lock-free coordination cost

---

## Limitations and Trade-offs

### 1. Fixed 85/15 Split
**Limitation:** Ratio hardcoded, not adaptive
**Impact:** Suboptimal for workloads with many expensive jobs or many cheap jobs
**Future:** Dynamic adjustment based on queue depth

### 2. Unbounded Job Queue
**Limitation:** job_tx channel is unbounded
**Impact:** Memory can grow unboundedly with fast submissions
**Mitigation:** Bounded channel with backpressure in future

### 3. Best-Effort Wake Signals
**Limitation:** Wake signals can be dropped or coalesced
**Impact:** Reserve workers may not wake immediately
**Acceptable:** Reserve workers are optimization, not required for correctness

### 4. No Step Ordering Guarantees
**Limitation:** Steps execute in arbitrary order (CAS race)
**Impact:** Cannot rely on step N completing before step N+1 starts
**Design:** Intentional - steps must be independent (connected via channels)

### 5. Work Duplication Risk
**Limitation:** Main and reserve workers can simultaneously work on same job
**Impact:** CAS ensures no duplicate step execution, but both workers contend
**Trade-off:** Parallelism benefit outweighs contention cost for high-cost jobs

---

## Integration with Pipeline Builder

### Pipeline → Executor Flow

```
SQL Query
    │
    ▼
Query Planner
    │
    ▼
QueryPlan { tables, filters, ... }
    │
    ▼
Pipeline Builder
    │
    ├─▶ Group filters by column
    ├─▶ Create PipelineSteps
    ├─▶ Wire crossbeam channels
    └─▶ Build Job
          │
          ▼
Job {
    table_name: "users",
    steps: Vec<PipelineStep>,  // One per column filter
    cost: 5,                   // Number of steps
    next_free_slot: AtomicUsize(0),
    id: "pipe-xyz...",
    entry_producer: Sender<Batch>,
}
    │
    ▼
executor.submit(job)
    │
    ▼
[Execution flow from earlier sections]
```

---

## Module Location

- **Source:** `src/executor.rs`
- **Dependencies:**
  - `crate::pipeline::Job` - Job struct definition
  - `crate::pool::scheduler::ThreadPool` - Worker pool implementation
  - `crossbeam::channel` - MPMC channels
  - `crossbeam_skiplist::SkipSet` - Concurrent sorted set
- **Tests:** `src/executor.rs` (inline unit tests)

---

## Related Documentation

- [Pipeline Builder](pipeline) - Creates Jobs consumed by executor
- [Thread Pool Scheduler](scheduler) - ThreadPool implementation details
- [Query Planner](query_planner) - Generates QueryPlan
- [Architecture Overview](architecture_overview) - System-wide context
