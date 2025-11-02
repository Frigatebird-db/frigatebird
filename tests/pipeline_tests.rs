use idk_uwu_ig::cache::page_cache::{
    PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
};
use idk_uwu_ig::helpers::compressor::Compressor;
use idk_uwu_ig::metadata_store::{PageDirectory, TableMetaStore};
use idk_uwu_ig::page_handler::page_io::PageIO;
use idk_uwu_ig::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
use idk_uwu_ig::pipeline::{PipelineBatch, build_pipeline};
use idk_uwu_ig::sql::plan_sql;
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, RwLock};

fn create_test_page_handler() -> Arc<PageHandler> {
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
fn builds_empty_pipeline_for_no_filters() {
    let plan = plan_sql("SELECT id FROM users").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "users");
    assert_eq!(jobs[0].steps.len(), 0);
    assert_eq!(jobs[0].cost, 0);
}

#[test]
fn builds_single_step_for_single_column_filter() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "users");
    assert_eq!(jobs[0].steps.len(), 1);
    assert_eq!(jobs[0].steps[0].column, "age");
    assert_eq!(jobs[0].steps[0].filters.len(), 1);
    assert_eq!(jobs[0].cost, 1);
}

#[test]
fn builds_multiple_steps_for_multiple_column_filters() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "users");
    assert_eq!(jobs[0].steps.len(), 2); // Two columns: age and name

    // Steps can be in any order (random for now)
    let columns: Vec<&str> = jobs[0].steps.iter().map(|s| s.column.as_str()).collect();
    assert!(columns.contains(&"age"));
    assert!(columns.contains(&"name"));
    assert_eq!(jobs[0].cost, jobs[0].steps.len());
}

#[test]
fn groups_multiple_filters_on_same_column() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18 AND age < 65").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].steps.len(), 1);
    assert_eq!(jobs[0].steps[0].column, "age");
    assert_eq!(jobs[0].steps[0].filters.len(), 2); // Two filters on same column
    assert_eq!(jobs[0].cost, 1);
}

#[test]
fn builds_pipeline_for_complex_query() {
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND region = 'US' AND balance > 1000",
    )
    .unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "accounts");
    assert_eq!(jobs[0].steps.len(), 3); // Three columns: status, region, balance

    let columns: Vec<&str> = jobs[0].steps.iter().map(|s| s.column.as_str()).collect();
    assert!(columns.contains(&"status"));
    assert!(columns.contains(&"region"));
    assert!(columns.contains(&"balance"));
    assert_eq!(jobs[0].cost, jobs[0].steps.len());
}

#[test]
fn builds_pipeline_for_delete_with_filter() {
    let plan = plan_sql("DELETE FROM sessions WHERE expires_at < NOW()").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "sessions");
    assert_eq!(jobs[0].steps.len(), 1);
    assert_eq!(jobs[0].steps[0].column, "expires_at");
    assert_eq!(jobs[0].cost, 1);
}

#[test]
fn builds_pipeline_for_update_with_filter() {
    let plan =
        plan_sql("UPDATE accounts SET balance = 0 WHERE id = 42 AND status = 'closed'").unwrap();
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].table_name, "accounts");
    assert_eq!(jobs[0].steps.len(), 2); // Two columns: id, status

    let columns: Vec<&str> = jobs[0].steps.iter().map(|s| s.column.as_str()).collect();
    assert!(columns.contains(&"id"));
    assert!(columns.contains(&"status"));
    assert_eq!(jobs[0].cost, jobs[0].steps.len());
}

#[test]
fn job_get_next_advances_steps() {
    let plan =
        plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'").expect("valid plan");
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);
    let job = &jobs[0];

    job.get_next();
    job.get_next();

    assert_eq!(
        job.next_free_slot.load(AtomicOrdering::Relaxed),
        job.steps.len()
    );
}

#[test]
fn compares_jobs_by_cost() {
    let page_handler = create_test_page_handler();

    let mut single_step = build_pipeline(
        &plan_sql("SELECT id FROM users WHERE age > 18").expect("valid single-step plan"),
        Arc::clone(&page_handler),
    );
    let job_small = single_step.remove(0);

    let mut multi_step = build_pipeline(
        &plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'")
            .expect("valid multi-step plan"),
        page_handler,
    );
    let job_large = multi_step.remove(0);

    assert!(job_small < job_large);
    assert!(job_large > job_small);
    assert_eq!(job_small.partial_cmp(&job_large), Some(CmpOrdering::Less));
}

#[test]
fn wires_channel_chain_between_steps() {
    let plan =
        plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'").expect("valid plan");
    let page_handler = create_test_page_handler();
    let jobs = build_pipeline(&plan, page_handler);

    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert!(job.steps.len() >= 2);
    assert_eq!(job.next_free_slot.load(AtomicOrdering::Relaxed), 0);
    assert!(!job.id.is_empty());
    assert_eq!(job.cost, job.steps.len());

    let batch: PipelineBatch = vec![1, 2, 3];
    job.entry_producer
        .send(batch.clone())
        .expect("entry producer should accept batches");

    let first_step = &job.steps[0];
    let received_first = first_step
        .previous_receiver
        .try_recv()
        .expect("first step should receive entry batch");
    assert_eq!(received_first, batch);

    first_step
        .current_producer
        .send(received_first.clone())
        .expect("first step should forward batch");

    let second_step = &job.steps[1];
    let received_second = second_step
        .previous_receiver
        .try_recv()
        .expect("second step should receive forwarded batch");
    assert_eq!(received_second, received_first);
}
