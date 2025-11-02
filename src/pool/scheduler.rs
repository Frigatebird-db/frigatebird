/*
in a systems, there would be m = threads and n = cores, at any given time, we can only theoretically only ensure that atmost m things are running truly parallely due to physics

so we can kinda... try to keep similar stuff in a single core, like... maybe bind some stuff to a single core, like WAL maybe ?? so generally 2 threads would be doing the WAL work
(future nubskr: okay, this shit will complicated things on shared cloud due to their hypervisor scheduling algorithms, let's skip this core pinning stuff{for now})

let's just deal with threads, we have n = no. of threads in the systems, and kinda
reserver some threads for some things, like a some percent of total threads are responsible for this, other for something, other for something else....

let's call then x,y,z and (x+y+z)=100 , and we divide into more as well

now we need to categorise our stuff which needs to be done so that we can get the probability distribution:

- I think we must make each connection 'sticky' to some thread, so that we can kinda guarantee the order of operations on a connection level


so to broadly categorise, we have:
- ops (I think we should let one whole thread handle all the things in a particular op , i.e get stuff from meta store,go to the io handler for pages and then serve)
- wal (flushes and everything included btw, so the thing is, we can let the above handle this as well)
I am worried about the wal threads getting overloaded btw, essentially we want to report a write request as suceeded once it hits the disk in any form whether in WAL or actual disk

so I think in our case, it would be way faster.. idk man, idk if it would be faster or slower lol haha, I mean.... it could be pretty fast lol

the thing is, if we use a single thread for WAL, I dont think we can kinda eat the overhead of MPSC

this is a pretty fair scheduler which treats every single
*/

use crossbeam::channel;
use std::thread;

// Minimal thread pool
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<channel::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = channel::unbounded::<Job>();

        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            let receiver = receiver.clone();

            let thread = thread::spawn(move || {
                loop {
                    match receiver.recv() {
                        Ok(job) => job(),
                        Err(_) => break, // channel closed, exit loop
                    }
                }
            });

            workers.push(Worker {
                thread: Some(thread),
            });
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.as_ref().unwrap().send(Box::new(f)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Close channel by dropping sender
        self.sender.take();

        // Wait for workers to finish
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join(); // Ignore panics in worker threads
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn thread_pool_new_creates_pool() {
        let pool = ThreadPool::new(4);
        assert_eq!(pool.workers.len(), 4);
        assert!(pool.sender.is_some());
    }

    #[test]
    fn thread_pool_executes_single_job() {
        let pool = ThreadPool::new(2);
        let (tx, rx) = mpsc::channel();

        pool.execute(move || {
            tx.send(42).unwrap();
        });

        let result = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn thread_pool_executes_multiple_jobs() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        for _ in 0..10 {
            let counter_clone = Arc::clone(&counter);
            let tx_clone = tx.clone();
            pool.execute(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                tx_clone.send(()).unwrap();
            });
        }

        // Wait for all jobs to complete
        for _ in 0..10 {
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn thread_pool_jobs_execute_concurrently() {
        let pool = ThreadPool::new(4);
        let start_barrier = Arc::new(std::sync::Barrier::new(4));
        let (tx, rx) = mpsc::channel();

        for i in 0..4 {
            let barrier_clone = Arc::clone(&start_barrier);
            let tx_clone = tx.clone();
            pool.execute(move || {
                barrier_clone.wait(); // All threads wait here
                tx_clone.send(i).unwrap();
            });
        }

        // All should complete around the same time if truly parallel
        let mut results = Vec::new();
        for _ in 0..4 {
            results.push(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }

        assert_eq!(results.len(), 4);
    }

    #[test]
    fn thread_pool_graceful_shutdown() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        {
            let pool = ThreadPool::new(2);
            for _ in 0..5 {
                let counter_clone = Arc::clone(&counter);
                let tx_clone = tx.clone();
                pool.execute(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                    tx_clone.send(()).unwrap();
                });
            }

            // Wait for all jobs
            for _ in 0..5 {
                rx.recv_timeout(Duration::from_secs(1)).unwrap();
            }
        } // Pool dropped here

        // All jobs should have completed
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn thread_pool_single_worker() {
        let pool = ThreadPool::new(1);
        let (tx, rx) = mpsc::channel();

        for i in 0..3 {
            let tx_clone = tx.clone();
            pool.execute(move || {
                tx_clone.send(i).unwrap();
            });
        }

        // Even with one worker, all jobs execute
        let mut results = Vec::new();
        for _ in 0..3 {
            results.push(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        }

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn thread_pool_job_ordering_with_single_worker() {
        let pool = ThreadPool::new(1);
        let (tx, rx) = mpsc::channel();

        for i in 0..5 {
            let tx_clone = tx.clone();
            pool.execute(move || {
                tx_clone.send(i).unwrap();
            });
        }

        // With single worker, jobs should execute in order
        let results: Vec<_> = (0..5)
            .map(|_| rx.recv_timeout(Duration::from_secs(1)).unwrap())
            .collect();

        assert_eq!(results, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn thread_pool_handles_panicking_job() {
        let pool = ThreadPool::new(2);
        let (tx, rx) = mpsc::channel();

        // Job that panics
        pool.execute(|| {
            panic!("Intentional panic");
        });

        // Job that succeeds
        pool.execute(move || {
            tx.send(42).unwrap();
        });

        // Pool should still function after panic
        let result = rx.recv_timeout(Duration::from_secs(1));
        assert!(
            result.is_ok(),
            "Pool should continue working after a job panics"
        );
    }

    #[test]
    fn thread_pool_many_workers() {
        let pool = ThreadPool::new(16);
        let counter = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        for _ in 0..100 {
            let counter_clone = Arc::clone(&counter);
            let tx_clone = tx.clone();
            pool.execute(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                tx_clone.send(()).unwrap();
            });
        }

        for _ in 0..100 {
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }
}

// fn main() {
//     println!("=== Thread pool with {} threads ===", 4);
//     let pool = ThreadPool::new(4);
//     let (tx, rx) = mpsc::channel();

//     let start = Instant::now();
//     for _ in 0..1000 {
//         let tx = tx.clone();
//         pool.execute(move || {
//             // Simulate same tiny work
//             let _ = 1 + 1;
//             tx.send(()).unwrap();
//         });
//     }

//     // Wait for all tasks to complete
//     for _ in 0..1000 {
//         rx.recv().unwrap();
//     }
//     let pool_duration = start.elapsed();
//     println!("1000 tasks via thread pool: {:?}", pool_duration);
//     println!("Average per task: {:?}\n", pool_duration / 1000);
// }
