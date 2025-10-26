use idk_uwu_ig::pipeline::{build_pipeline, PipelineBatch};
use idk_uwu_ig::sql::plan_sql;
use std::sync::atomic::Ordering;

#[test]
fn builds_empty_pipeline_for_no_filters() {
    let plan = plan_sql("SELECT id FROM users").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "users");
    assert_eq!(pipelines[0].steps.len(), 0);
}

#[test]
fn builds_single_step_for_single_column_filter() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "users");
    assert_eq!(pipelines[0].steps.len(), 1);
    assert_eq!(pipelines[0].steps[0].column, "age");
    assert_eq!(pipelines[0].steps[0].filters.len(), 1);
}

#[test]
fn builds_multiple_steps_for_multiple_column_filters() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "users");
    assert_eq!(pipelines[0].steps.len(), 2); // Two columns: age and name

    // Steps can be in any order (random for now)
    let columns: Vec<&str> = pipelines[0]
        .steps
        .iter()
        .map(|s| s.column.as_str())
        .collect();
    assert!(columns.contains(&"age"));
    assert!(columns.contains(&"name"));
}

#[test]
fn groups_multiple_filters_on_same_column() {
    let plan = plan_sql("SELECT id FROM users WHERE age > 18 AND age < 65").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].steps.len(), 1);
    assert_eq!(pipelines[0].steps[0].column, "age");
    assert_eq!(pipelines[0].steps[0].filters.len(), 2); // Two filters on same column
}

#[test]
fn builds_pipeline_for_complex_query() {
    let plan = plan_sql(
        "SELECT id FROM accounts WHERE status = 'active' AND region = 'US' AND balance > 1000",
    )
    .unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "accounts");
    assert_eq!(pipelines[0].steps.len(), 3); // Three columns: status, region, balance

    let columns: Vec<&str> = pipelines[0]
        .steps
        .iter()
        .map(|s| s.column.as_str())
        .collect();
    assert!(columns.contains(&"status"));
    assert!(columns.contains(&"region"));
    assert!(columns.contains(&"balance"));
}

#[test]
fn builds_pipeline_for_delete_with_filter() {
    let plan = plan_sql("DELETE FROM sessions WHERE expires_at < NOW()").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "sessions");
    assert_eq!(pipelines[0].steps.len(), 1);
    assert_eq!(pipelines[0].steps[0].column, "expires_at");
}

#[test]
fn builds_pipeline_for_update_with_filter() {
    let plan =
        plan_sql("UPDATE accounts SET balance = 0 WHERE id = 42 AND status = 'closed'").unwrap();
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].table_name, "accounts");
    assert_eq!(pipelines[0].steps.len(), 2); // Two columns: id, status

    let columns: Vec<&str> = pipelines[0]
        .steps
        .iter()
        .map(|s| s.column.as_str())
        .collect();
    assert!(columns.contains(&"id"));
    assert!(columns.contains(&"status"));
}

#[test]
fn wires_channel_chain_between_steps() {
    let plan =
        plan_sql("SELECT id FROM users WHERE age > 18 AND name = 'John'").expect("valid plan");
    let pipelines = build_pipeline(&plan);

    assert_eq!(pipelines.len(), 1);
    let pipeline = &pipelines[0];
    assert!(pipeline.steps.len() >= 2);
    assert_eq!(pipeline.next_free_slot.load(Ordering::Relaxed), 0);
    assert!(!pipeline.id.is_empty());

    let batch: PipelineBatch = vec![1, 2, 3];
    pipeline
        .entry_producer
        .send(batch.clone())
        .expect("entry producer should accept batches");

    let first_step = &pipeline.steps[0];
    let received_first = first_step
        .previous_receiver
        .try_recv()
        .expect("first step should receive entry batch");
    assert_eq!(received_first, batch);

    first_step
        .current_producer
        .send(received_first.clone())
        .expect("first step should forward batch");

    let second_step = &pipeline.steps[1];
    let received_second = second_step
        .previous_receiver
        .try_recv()
        .expect("second step should receive forwarded batch");
    assert_eq!(received_second, received_first);
}
