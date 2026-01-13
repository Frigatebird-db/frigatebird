use super::{OrderClause, SelectResult, SqlExecutionError, SqlExecutor};
use super::executor_types::ProjectionPlan;
use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::pipeline::select_planner::{execute_projection_pipeline, execute_window_pipeline};
use crate::pipeline::window_helpers::WindowFunctionPlan;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Offset};
use std::collections::{BTreeSet, HashMap};

impl SqlExecutor {
    pub(super) fn execute_vectorized_projection(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        projection_plan: &ProjectionPlan,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
        qualify_expr: Option<&Expr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<SelectResult, SqlExecutionError> {
        execute_projection_pipeline(
            self,
            table,
            catalog,
            columns,
            projection_plan,
            required_ordinals,
            selection_expr,
            selection_physical_expr,
            selection_applied_in_scan,
            column_ordinals,
            column_types,
            order_clauses,
            result_columns,
            limit_expr,
            offset_expr,
            distinct_flag,
            qualify_expr,
            row_ids,
        )
    }

    pub(super) fn execute_vectorized_window_query(
        &self,
        table: &str,
        catalog: &TableCatalog,
        columns: &[ColumnCatalog],
        projection_plan: ProjectionPlan,
        window_plans: Vec<WindowFunctionPlan>,
        partition_exprs: Vec<Expr>,
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&Expr>,
        selection_physical_expr: Option<&PhysicalExpr>,
        selection_applied_in_scan: bool,
        column_ordinals: &HashMap<String, usize>,
        column_types: &HashMap<String, DataType>,
        order_clauses: &[OrderClause],
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        distinct_flag: bool,
        qualify_expr: Option<Expr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<SelectResult, SqlExecutionError> {
        execute_window_pipeline(
            self,
            table,
            catalog,
            columns,
            projection_plan,
            window_plans,
            partition_exprs,
            required_ordinals,
            selection_expr,
            selection_physical_expr,
            selection_applied_in_scan,
            column_ordinals,
            column_types,
            order_clauses,
            result_columns,
            limit_expr,
            offset_expr,
            distinct_flag,
            qualify_expr,
            row_ids,
        )
    }
}
