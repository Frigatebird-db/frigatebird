use super::types;
use types::{Job, PipelineBatch, PipelineStep};

use crate::page_handler::PageHandler;
use crate::sql::models::{FilterExpr, QueryPlan};
use crossbeam::channel::{Receiver, Sender, unbounded};
use rand::{Rng, distributions::Alphanumeric};
use std::collections::HashMap;
use std::sync::Arc;

pub fn build_pipeline(plan: &QueryPlan, page_handler: Arc<PageHandler>) -> Vec<Job> {
    let mut jobs = Vec::with_capacity(plan.tables.len());

    for table in &plan.tables {
        if let Some(filter) = &table.filters {
            let leaf_filters = extract_leaf_filters(filter);

            let grouped = group_filters_by_column(leaf_filters);
            let catalog = page_handler
                .table_catalog(&table.table_name)
                .expect("missing table catalog for pipeline");

            // Convert groups into pipeline steps (random order for now)
            let mut grouped_steps: Vec<(String, usize, Vec<FilterExpr>)> = Vec::new();
            for (column, filters) in grouped {
                let ordinal = catalog
                    .column(&column)
                    .map(|col| col.ordinal)
                    .expect("missing column in pipeline catalog");
                grouped_steps.push((column, ordinal, filters));
            }
            let (entry_producer, output_receiver, steps) = attach_channels(
                grouped_steps,
                table.table_name.clone(),
                Arc::clone(&page_handler),
            );

            jobs.push(Job::new(
                table.table_name.clone(),
                steps,
                entry_producer,
                output_receiver,
            ));
        } else {
            // No filters, empty pipeline
            let (entry_producer, output_receiver) = unbounded::<PipelineBatch>();
            jobs.push(Job::new(
                table.table_name.clone(),
                Vec::new(),
                entry_producer,
                output_receiver,
            ));
        }
    }

    jobs
}

fn extract_leaf_filters(filter: &FilterExpr) -> Vec<FilterExpr> {
    let mut leaves = Vec::new();
    collect_leaves(filter, &mut leaves);
    leaves
}

fn collect_leaves(filter: &FilterExpr, leaves: &mut Vec<FilterExpr>) {
    match filter {
        FilterExpr::Leaf(expr) => leaves.push(FilterExpr::Leaf(expr.clone())),
        FilterExpr::And(filters) | FilterExpr::Or(filters) => {
            for f in filters {
                collect_leaves(f, leaves);
            }
        }
    }
}

use crate::sql::physical_plan::PhysicalExpr;

fn group_filters_by_column(filters: Vec<FilterExpr>) -> HashMap<String, Vec<FilterExpr>> {
    let mut groups: HashMap<String, Vec<FilterExpr>> = HashMap::new();

    for filter in filters {
        if let FilterExpr::Leaf(expr) = &filter {
            let column = extract_primary_column_physical(expr).unwrap_or_else(|| "*".to_string());
            groups.entry(column).or_default().push(filter);
        }
    }

    groups
}

fn extract_primary_column_physical(expr: &PhysicalExpr) -> Option<String> {
    match expr {
        PhysicalExpr::Column { name, .. } => Some(name.clone()),
        PhysicalExpr::BinaryOp { left, .. } => extract_primary_column_physical(left),
        PhysicalExpr::UnaryOp { expr, .. } => extract_primary_column_physical(expr),
        PhysicalExpr::Like { expr, .. } => extract_primary_column_physical(expr),
        PhysicalExpr::InList { expr, .. } => extract_primary_column_physical(expr),
        PhysicalExpr::Cast { expr, .. } => extract_primary_column_physical(expr),
        _ => None,
    }
}

fn attach_channels(
    grouped_steps: Vec<(String, usize, Vec<FilterExpr>)>,
    table: String,
    page_handler: Arc<PageHandler>,
) -> (
    Sender<PipelineBatch>,
    Receiver<PipelineBatch>,
    Vec<PipelineStep>,
) {
    let (entry_producer, mut previous_receiver) = unbounded::<PipelineBatch>();
    let mut steps = Vec::with_capacity(grouped_steps.len());
    let mut is_root = true;

    for (column, ordinal, filters) in grouped_steps {
        let (current_producer, next_receiver) = unbounded::<PipelineBatch>();
        steps.push(PipelineStep::new(
            table.clone(),
            column,
            ordinal,
            filters,
            Vec::new(),
            is_root,
            Arc::clone(&page_handler),
            current_producer,
            previous_receiver,
            None,
        ));
        previous_receiver = next_receiver;
        is_root = false;
    }

    (entry_producer, previous_receiver, steps)
}

pub(super) fn generate_pipeline_id() -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();
    format!("pipe-{suffix}")
}
