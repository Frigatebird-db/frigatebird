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
use std::time::Instant;

// Minimal thread pool
struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<channel::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    fn new(size: usize) -> Self {
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

    fn execute<F>(&self, f: F)
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
                thread.join().unwrap();
            }
        }
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
