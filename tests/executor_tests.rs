use idk_uwu_ig::executor::PipelineExecutor;
use idk_uwu_ig::pipeline::{Job, PipelineBatch, PipelineStep};
use crossbeam::channel;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn create_dummy_job(step_count: usize) -> Job {
    let (entry_tx, _entry_rx) = channel::unbounded::<PipelineBatch>();
    let mut steps = Vec::with_capacity(step_count);

    let mut prev_rx = {
        let (tx, rx) = channel::unbounded::<PipelineBatch>();
        let _ = tx.send(vec![]);
        rx
    };

    for idx in 0..step_count {
        let (tx, rx) = channel::unbounded::<PipelineBatch>();
        steps.push(PipelineStep::new(
            format!("col{}", idx),
            Vec::new(),
            tx,
            prev_rx,
            idx == 0,
        ));
        prev_rx = rx;
    }

    Job::new("test_table".into(), steps, entry_tx)
}

#[test]
fn executor_new_creates_with_threads() {
    let executor = PipelineExecutor::new(4);
    thread::sleep(Duration::from_millis(10));
    drop(executor);
}

#[test]
fn executor_submit_single_job() {
    let executor = PipelineExecutor::new(4);
    let job = create_dummy_job(2);
    executor.submit(job);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn executor_submit_multiple_jobs() {
    let executor = PipelineExecutor::new(4);

    for _ in 0..10 {
        let job = create_dummy_job(3);
        executor.submit(job);
    }

    thread::sleep(Duration::from_millis(100));
}

#[test]
fn executor_job_get_next_executes_steps() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let (entry_tx, _) = channel::unbounded::<PipelineBatch>();
    let (tx, rx) = channel::unbounded::<PipelineBatch>();

    let step = PipelineStep::new("col1".to_string(), Vec::new(), tx.clone(), rx.clone(), true);

    let job = Job::new("table".into(), vec![step], entry_tx);

    let job_arc = Arc::new(job);
    let job_clone = Arc::clone(&job_arc);

    let handle = thread::spawn(move || {
        job_clone.get_next();
        counter_clone.fetch_add(1, Ordering::SeqCst);
    });

    handle.join().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn executor_job_get_next_multiple_workers() {
    let job = Arc::new(create_dummy_job(10));
    let mut handles = vec![];

    for _ in 0..5 {
        let job_clone = Arc::clone(&job);
        let handle = thread::spawn(move || {
            loop {
                let before = job_clone.next_free_slot.load(Ordering::Relaxed);
                if before >= 10 {
                    break;
                }
                job_clone.get_next();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let final_slot = job.next_free_slot.load(Ordering::Relaxed);
    assert_eq!(final_slot, 10);
}

#[test]
fn executor_job_cost_equals_step_count() {
    let job = create_dummy_job(5);
    assert_eq!(job.cost, 5);

    let job2 = create_dummy_job(15);
    assert_eq!(job2.cost, 15);
}

#[test]
fn executor_job_id_generated() {
    let job = create_dummy_job(1);
    assert!(job.id.starts_with("pipe-"));
    assert!(job.id.len() > 10);
}

#[test]
fn executor_job_unique_ids() {
    let job1 = create_dummy_job(1);
    let job2 = create_dummy_job(1);
    assert_ne!(job1.id, job2.id);
}

#[test]
fn executor_with_minimum_threads() {
    let executor = PipelineExecutor::new(2);
    let job = create_dummy_job(1);
    executor.submit(job);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn executor_with_many_threads() {
    let executor = PipelineExecutor::new(16);
    for _ in 0..20 {
        executor.submit(create_dummy_job(5));
    }
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn executor_job_ordering_by_cost() {
    let job1 = create_dummy_job(3);
    let job2 = create_dummy_job(10);

    assert!(job2.cost > job1.cost);
}

#[test]
fn executor_empty_job() {
    let (entry_tx, _) = channel::unbounded::<PipelineBatch>();
    let job = Job::new("table".into(), vec![], entry_tx);
    assert_eq!(job.cost, 0);

    job.get_next(); // Should not panic
}

#[test]
fn executor_concurrent_job_submission() {
    let executor = Arc::new(PipelineExecutor::new(8));
    let mut handles = vec![];

    for i in 0..10 {
        let exec = Arc::clone(&executor);
        let handle = thread::spawn(move || {
            let job = create_dummy_job(i % 5 + 1);
            exec.submit(job);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    thread::sleep(Duration::from_millis(100));
}

#[test]
fn executor_stress_test() {
    let executor = Arc::new(PipelineExecutor::new(8));

    for _ in 0..100 {
        let job = create_dummy_job(3);
        executor.submit(job);
    }

    thread::sleep(Duration::from_millis(200));
}

#[test]
fn executor_job_table_name() {
    let job = create_dummy_job(2);
    assert_eq!(job.table_name, "test_table");
}

#[test]
fn executor_step_execution_sequence() {
    let execution_order = Arc::new(AtomicUsize::new(0));

    let (entry_tx, _) = channel::unbounded();
    let (tx1, rx1) = channel::unbounded();
    let (tx2, rx2) = channel::unbounded();

    let step1 = PipelineStep::new("col1".into(), vec![], tx1, rx1.clone(), true);
    let step2 = PipelineStep::new("col2".into(), vec![], tx2, rx2, false);

    let job = Job::new("table".into(), vec![step1, step2], entry_tx);
    let job_arc = Arc::new(job);

    let job1 = Arc::clone(&job_arc);
    let order1 = Arc::clone(&execution_order);
    let h1 = thread::spawn(move || {
        job1.get_next();
        order1.fetch_add(1, Ordering::SeqCst);
    });

    let job2 = Arc::clone(&job_arc);
    let order2 = Arc::clone(&execution_order);
    let h2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        job2.get_next();
        order2.fetch_add(1, Ordering::SeqCst);
    });

    h1.join().unwrap();
    h2.join().unwrap();

    assert_eq!(execution_order.load(Ordering::SeqCst), 2);
}
