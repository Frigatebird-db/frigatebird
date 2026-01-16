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
                while let Ok(job) = receiver.recv() {
                    job();
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
        self.sender.take();

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
