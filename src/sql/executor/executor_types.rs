use super::ordering::OrderKey;
use sqlparser::ast::Expr;
use std::collections::BTreeSet;

pub(super) struct ProjectionPlan {
    pub(super) headers: Vec<String>,
    pub(super) items: Vec<ProjectionItem>,
    pub(super) required_ordinals: BTreeSet<usize>,
}

impl ProjectionPlan {
    pub(super) fn new() -> Self {
        Self {
            headers: Vec::new(),
            items: Vec::new(),
            required_ordinals: BTreeSet::new(),
        }
    }

    pub(super) fn needs_dataset(&self) -> bool {
        self.items
            .iter()
            .any(|item| matches!(item, ProjectionItem::Computed { .. }))
    }
}

pub(super) enum ProjectionItem {
    Direct { ordinal: usize },
    Computed { expr: Expr },
}

pub(super) struct GroupByInfo {
    pub(super) sets: Vec<GroupingSetPlan>,
}

#[derive(Clone)]
pub(super) struct GroupingSetPlan {
    pub(super) expressions: Vec<Expr>,
    pub(super) masked_exprs: Vec<Expr>,
}

#[derive(Clone)]
pub(super) struct AggregatedRow {
    pub(super) order_key: OrderKey,
    pub(super) values: Vec<Option<String>>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub(super) struct GroupKey {
    values: Vec<Option<String>>,
}

impl GroupKey {
    pub(super) fn empty() -> Self {
        Self { values: Vec::new() }
    }

    pub(super) fn from_values(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    pub(super) fn value_at(&self, idx: usize) -> Option<Option<String>> {
        self.values.get(idx).cloned()
    }
}

pub(super) enum VectorAggregationOutput {
    Aggregate { slot_index: usize },
    GroupExpr { group_index: usize },
    Literal { value: Option<String> },
    ScalarExpr { expr: Expr },
}
