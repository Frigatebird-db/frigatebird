use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::sql::executor::{
    AggregatedRow, AggregateProjectionPlan, OrderClause, SelectResult, SqlExecutionError,
    SqlExecutor,
};
use crate::sql::types::DataType;
use sqlparser::ast::{Expr, Offset};
use std::collections::{BTreeSet, HashMap};

use super::PipelineOperator;

pub struct AggregateOperator<'a> {
    executor: &'a SqlExecutor,
    table: &'a str,
    catalog: &'a TableCatalog,
    columns: &'a [ColumnCatalog],
    aggregate_plan: &'a AggregateProjectionPlan,
    required_ordinals: &'a BTreeSet<usize>,
    column_ordinals: &'a HashMap<String, usize>,
    column_types: &'a HashMap<String, DataType>,
    prefer_exact_numeric: bool,
    result_columns: Vec<String>,
    limit_expr: Option<Expr>,
    offset_expr: Option<Offset>,
    having: Option<&'a Expr>,
    qualify_expr: Option<&'a Expr>,
    order_clauses: &'a [OrderClause],
    distinct_flag: bool,
}

impl<'a> AggregateOperator<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        executor: &'a SqlExecutor,
        table: &'a str,
        catalog: &'a TableCatalog,
        columns: &'a [ColumnCatalog],
        aggregate_plan: &'a AggregateProjectionPlan,
        required_ordinals: &'a BTreeSet<usize>,
        column_ordinals: &'a HashMap<String, usize>,
        column_types: &'a HashMap<String, DataType>,
        prefer_exact_numeric: bool,
        result_columns: Vec<String>,
        limit_expr: Option<Expr>,
        offset_expr: Option<Offset>,
        having: Option<&'a Expr>,
        qualify_expr: Option<&'a Expr>,
        order_clauses: &'a [OrderClause],
        distinct_flag: bool,
    ) -> Self {
        Self {
            executor,
            table,
            catalog,
            columns,
            aggregate_plan,
            required_ordinals,
            column_ordinals,
            column_types,
            prefer_exact_numeric,
            result_columns,
            limit_expr,
            offset_expr,
            having,
            qualify_expr,
            order_clauses,
            distinct_flag,
        }
    }

    pub(crate) fn execute_simple_from_batch(
        &self,
        batch: &crate::sql::executor::batch::ColumnarBatch,
        group_exprs: &[Expr],
    ) -> Result<SelectResult, SqlExecutionError> {
        let aggregated_rows = self
            .executor
            .execute_grouping_set_aggregation_rows_from_batch(
                batch,
                self.table,
                self.catalog,
                self.columns,
                self.aggregate_plan,
                group_exprs,
                self.required_ordinals,
                self.column_ordinals,
                self.column_types,
                self.prefer_exact_numeric,
                self.having,
                self.qualify_expr,
                self.order_clauses,
                None,
            )?;
        self.executor.finalize_aggregation_rows(
            aggregated_rows,
            self.order_clauses,
            self.distinct_flag,
            self.limit_expr.clone(),
            self.offset_expr.clone(),
            self.result_columns.clone(),
        )
    }

    pub(crate) fn execute_grouping_set_rows_from_batch(
        &self,
        batch: &crate::sql::executor::batch::ColumnarBatch,
        group_exprs: &[Expr],
        masked_exprs: Option<&[Expr]>,
    ) -> Result<Vec<AggregatedRow>, SqlExecutionError> {
        self.executor.execute_grouping_set_aggregation_rows_from_batch(
            batch,
            self.table,
            self.catalog,
            self.columns,
            self.aggregate_plan,
            group_exprs,
            self.required_ordinals,
            self.column_ordinals,
            self.column_types,
            self.prefer_exact_numeric,
            self.having,
            self.qualify_expr,
            self.order_clauses,
            masked_exprs,
        )
    }

}

impl<'a> PipelineOperator for AggregateOperator<'a> {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn execute(
        &mut self,
        _input: super::PipelineBatch,
    ) -> Result<Vec<super::PipelineBatch>, SqlExecutionError> {
        Err(SqlExecutionError::Unsupported(
            "aggregate operator requires explicit batch-driven execution"
                .into(),
        ))
    }
}
