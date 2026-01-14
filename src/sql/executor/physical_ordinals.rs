use crate::sql::physical_plan::PhysicalExpr;
use std::collections::BTreeSet;

pub(crate) fn collect_physical_ordinals(expr: &PhysicalExpr, ordinals: &mut BTreeSet<usize>) {
    match expr {
        PhysicalExpr::Column { index, .. } => {
            ordinals.insert(*index);
        }
        PhysicalExpr::BinaryOp { left, right, .. } => {
            collect_physical_ordinals(left, ordinals);
            collect_physical_ordinals(right, ordinals);
        }
        PhysicalExpr::UnaryOp { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Like {
            expr,
            pattern,
            case_insensitive: _,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::RLike {
            expr,
            pattern,
            negated: _,
        } => {
            collect_physical_ordinals(expr, ordinals);
            collect_physical_ordinals(pattern, ordinals);
        }
        PhysicalExpr::InList { expr, list, .. } => {
            collect_physical_ordinals(expr, ordinals);
            for item in list {
                collect_physical_ordinals(item, ordinals);
            }
        }
        PhysicalExpr::Cast { expr, .. } => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::IsNull(expr) | PhysicalExpr::IsNotNull(expr) => {
            collect_physical_ordinals(expr, ordinals);
        }
        PhysicalExpr::Literal(_) => {}
    }
}
