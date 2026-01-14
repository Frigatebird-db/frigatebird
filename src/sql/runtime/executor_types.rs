use super::ordering::OrderKey;
use sqlparser::ast::Expr;
use std::collections::BTreeSet;

pub(crate) struct ProjectionPlan {
    pub(crate) headers: Vec<String>,
    pub(crate) items: Vec<ProjectionItem>,
    pub(crate) required_ordinals: BTreeSet<usize>,
}

impl ProjectionPlan {
    pub(crate) fn new() -> Self {
        Self {
            headers: Vec::new(),
            items: Vec::new(),
            required_ordinals: BTreeSet::new(),
        }
    }

    pub(crate) fn needs_dataset(&self) -> bool {
        self.items
            .iter()
            .any(|item| matches!(item, ProjectionItem::Computed { .. }))
    }
}

pub(crate) enum ProjectionItem {
    Direct { ordinal: usize },
    Computed { expr: Expr },
}

pub(crate) struct GroupByInfo {
    pub(crate) sets: Vec<GroupingSetPlan>,
}

#[derive(Clone)]
pub(crate) struct GroupingSetPlan {
    pub(crate) expressions: Vec<Expr>,
    pub(crate) masked_exprs: Vec<Expr>,
}

#[derive(Clone)]
pub(crate) struct AggregatedRow {
    pub(crate) order_key: OrderKey,
    pub(crate) values: Vec<Option<String>>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub(crate) struct GroupKey {
    values: Vec<Option<String>>,
}

impl GroupKey {
    pub(crate) fn empty() -> Self {
        Self { values: Vec::new() }
    }

    pub(crate) fn from_values(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    pub(crate) fn value_at(&self, idx: usize) -> Option<Option<String>> {
        self.values.get(idx).cloned()
    }
}

pub(crate) enum VectorAggregationOutput {
    Aggregate { slot_index: usize },
    GroupExpr { group_index: usize },
    Literal { value: Option<String> },
    ScalarExpr { expr: Expr },
}
