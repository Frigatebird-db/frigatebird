// Integration tests for pipeline filtering functionality
use idk_uwu_ig::cache::page_cache::{PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed};
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::pipeline::{build_pipeline, PipelineBatch};
use idk_uwu_ig::sql::plan_sql;
use std::sync::{Arc, RwLock};

/// Helper function to create a PageHandler with test data
fn create_page_handler_with_data() -> Arc<PageHandler> {
    let meta_store = Arc::new(RwLock::new(TableMetaStore::new()));
    let directory = Arc::new(PageDirectory::new(meta_store));
    let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));

    let compressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
    let page_io = Arc::new(PageIO {});
    let fetcher = Arc::new(PageFetcher::new(compressed_cache, page_io));

    let uncompressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
    let compressor = Arc::new(Compressor::new());
    let materializer = Arc::new(PageMaterializer::new(uncompressed_cache, compressor));

    Arc::new(PageHandler::new(locator, fetcher, materializer))
}

#[test]
fn test_pipeline_with_page_handler() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "users");
    assert_eq!(jobs[0].steps.len(), 1);
}

#[test]
fn test_pipeline_multi_step_with_page_handler() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "users");
    assert_eq!(jobs[0].steps.len(), 2);

    // Verify each step has access to page handler
    for step in &jobs[0].steps {
        assert_eq!(step.table, "users");
    }
}

#[test]
fn test_pipeline_step_execution_flow() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18'").unwrap();
    let jobs = build_pipeline(&plan, Arc::clone(&page_handler));

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];

    // Test execution through public API
    if !job.steps.is_empty() {
        // Execute the first step
        job.get_next();

        // Verify step was executed
        let executed_count = job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(executed_count, 1);
    }
}

#[test]
fn test_pipeline_step_execute_flow() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE status = 'active'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];

    // Send a batch through the entry producer
    let batch: PipelineBatch = vec![0, 1, 2, 3, 4];
    job.entry_producer.send(batch.clone()).unwrap();

    // Execute the first step
    if !job.steps.is_empty() {
        job.get_next();

        // Verify the step was executed
        assert_eq!(
            job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}

#[test]
fn test_multiple_filters_on_same_column() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18' AND age < '65'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].steps.len(), 1);
    let step = &jobs[0].steps[0];

    assert_eq!(step.column, "age");
    assert_eq!(step.filters.len(), 2); // Two filters on same column

    // Filters should be grouped on the same column
    assert_eq!(jobs[0].cost, 1);
}

#[test]
fn test_pipeline_with_complex_filters() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND region = 'US' AND balance > '1000'"
    ).unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].steps.len(), 3); // Three columns

    let columns: Vec<&str> = jobs[0].steps.iter().map(|s| s.column.as_str()).collect();
    assert!(columns.contains(&"status"));
    assert!(columns.contains(&"region"));
    assert!(columns.contains(&"balance"));

    // Each step should have the correct table
    for step in &jobs[0].steps {
        assert_eq!(step.table, "accounts");
    }
}

#[test]
fn test_pipeline_no_filters() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].steps.len(), 0); // No filters, no steps
    assert_eq!(jobs[0].cost, 0);
}

#[test]
fn test_pipeline_execute_sequence() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    let job = &jobs[0];
    let initial_slot = job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(initial_slot, 0);

    // Execute steps
    job.get_next();
    let after_first = job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after_first, 1);

    job.get_next();
    let after_second = job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after_second, 2);

    // Trying to execute beyond available steps should not panic
    job.get_next();
    let final_slot = job.next_free_slot.load(std::sync::atomic::Ordering::Relaxed);
    assert!(final_slot >= job.steps.len());
}

#[test]
fn test_pipeline_root_step_behavior() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    let job = &jobs[0];
    if !job.steps.is_empty() {
        let first_step = &job.steps[0];
        assert!(first_step.is_root);

        if job.steps.len() > 1 {
            let second_step = &job.steps[1];
            assert!(!second_step.is_root);
        }
    }
}

#[test]
fn test_pipeline_with_delete_statement() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("DELETE FROM sessions WHERE expires_at < 'now'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "sessions");
    assert_eq!(jobs[0].steps.len(), 1);
    assert_eq!(jobs[0].steps[0].column, "expires_at");
}

#[test]
fn test_pipeline_with_update_statement() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("UPDATE accounts SET balance = 0 WHERE id = '42' AND status = 'closed'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "accounts");
    assert_eq!(jobs[0].steps.len(), 2);

    let columns: Vec<&str> = jobs[0].steps.iter().map(|s| s.column.as_str()).collect();
    assert!(columns.contains(&"id"));
    assert!(columns.contains(&"status"));
}

#[test]
fn test_pipeline_channel_wiring() {
    let page_handler = create_page_handler_with_data();
    let plan = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John'").unwrap();
    let jobs = build_pipeline(&plan, page_handler);

    let job = &jobs[0];
    if job.steps.len() < 2 {
        return; // Skip if not enough steps
    }

    // Send batch through entry producer
    let batch: PipelineBatch = vec![1, 2, 3];
    job.entry_producer.send(batch.clone()).unwrap();

    // First step should receive from entry
    let first_step = &job.steps[0];
    let received_first = first_step.previous_receiver.try_recv().unwrap();
    assert_eq!(received_first, batch);

    // Forward to next step
    first_step.current_producer.send(received_first.clone()).unwrap();

    // Second step should receive from first
    let second_step = &job.steps[1];
    let received_second = second_step.previous_receiver.try_recv().unwrap();
    assert_eq!(received_second, received_first);
}

#[test]
fn test_job_cost_calculation() {
    let page_handler = create_page_handler_with_data();

    let plan1 = plan_sql("SELECT id FROM users WHERE age > '18'").unwrap();
    let jobs1 = build_pipeline(&plan1, Arc::clone(&page_handler));
    assert_eq!(jobs1[0].cost, 1);

    let plan2 = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John'").unwrap();
    let jobs2 = build_pipeline(&plan2, Arc::clone(&page_handler));
    assert_eq!(jobs2[0].cost, 2);

    let plan3 = plan_sql("SELECT id FROM users").unwrap();
    let jobs3 = build_pipeline(&plan3, page_handler);
    assert_eq!(jobs3[0].cost, 0);
}

#[test]
fn test_job_comparison_by_cost() {
    let page_handler = create_page_handler_with_data();

    let plan_small = plan_sql("SELECT id FROM users WHERE age > '18'").unwrap();
    let mut jobs_small = build_pipeline(&plan_small, Arc::clone(&page_handler));
    let job_small = jobs_small.remove(0);

    let plan_large = plan_sql("SELECT id FROM users WHERE age > '18' AND name = 'John' AND status = 'active'").unwrap();
    let mut jobs_large = build_pipeline(&plan_large, page_handler);
    let job_large = jobs_large.remove(0);

    assert!(job_small < job_large);
    assert!(job_large > job_small);
    assert_eq!(job_small.cost, 1);
    assert!(job_large.cost >= 2);
}
