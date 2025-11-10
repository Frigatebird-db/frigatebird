---
title: "Thread Pool Scheduler"
layout: default
nav_order: 13
---

# Thread Pool Scheduler

The `scheduler` module implements a minimalist thread-pool abstraction. It provides basic task queuing and execution across worker threads using crossbeam channels.

## Structure

```rust
struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<channel::Sender<Job>>,
}
type Job = Box<dyn FnOnce() + Send + 'static>;
```

Uses crossbeam unbounded channel. Receivers are cloned per worker (MPMC). Sender dropped on shutdown → workers exit.

## Architecture

```
╔════════════════════════════════════ ThreadPool ════════════════════════════════════╗
║ workers: Vec<Worker>       sender: Option<channel::Sender<Job>>                    ║
║                                                                                     ║
║      ╔═══════════════╗     ╔═══════════════╗           ╔═══════════════╗          ║
║      ║  Worker[0]    ║     ║  Worker[1]    ║    ...    ║ Worker[size-1]║          ║
║      ║  thread: Join ║     ║  thread: Join ║           ║  thread: Join ║          ║
║      ╚═══════╤═══════╝     ╚═══════╤═══════╝           ╚═══════╤═══════╝          ║
║              │                     │                           │                  ║
║              ▼                     ▼                           ▼                  ║
║         Receiver      Receiver.clone()       Receiver.clone()                     ║
║              └─────────────────┬──────────────────┘                               ║
║                                │                                                  ║
║              Jobs queued via ThreadPool::execute(job)                             ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
```

## Construction (`ThreadPool::new`)

```
fn new(size)
      │
      ├─▶ create channel::unbounded<Job>() → (sender, receiver)
      │
      ├─▶ spawn `size` worker threads:
      │     let receiver = receiver.clone()  // crossbeam receiver is clonable
      │     loop {
      │         match receiver.recv() {
      │            Ok(job) => job(),
      │            Err(_) => break (channel closed)
      │         }
      │     }
      │
      └─▶ store JoinHandle in Worker.thread
```

## Task Submission (`ThreadPool::execute`)

```
fn execute<F>(f: F)
   where F: FnOnce() + Send + 'static

   └─ sender.send(Box::new(f)).unwrap()
```

### Flow

```
ThreadPool::execute(task A) ────────▶ sender
ThreadPool::execute(task B) ────────▶ sender
                                         │
                                         ▼
                                  crossbeam channel
                                   (MPMC, lock-free)
                                         │
                      ┌──────────────────┼──────────────────┐
                      │                  │                  │
                      ▼                  ▼                  ▼
           Worker 0 receiver    Worker 1 receiver    Worker 2 receiver
                 │                      │                    │
                 └──────── first recv() wins task ──────────┘
                               │
                               ├──▶ executes task A
                               └──▶ executes task B
```

## Shutdown (`Drop` impl)

```
impl Drop for ThreadPool
      │
      ├─▶ sender.take() → drop sender end → closes channel
      │
      └─▶ iterate workers:
            if thread handle exists:
                thread.join().unwrap()
```

## Benefits of Crossbeam

- **Lock-free MPMC** — no `Arc<Mutex<>>` overhead, better performance
- **Clonable receivers** — cleaner API, each worker owns a receiver clone
- **Better contention** — optimized for multi-consumer scenarios

## Usage

Used by `PipelineExecutor` to spawn main and reserve worker pools for query execution.
