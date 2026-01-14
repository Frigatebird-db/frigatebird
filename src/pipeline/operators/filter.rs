use crate::metadata_store::ColumnCatalog;
use crate::pipeline::filtering::apply_filter_expr;
use crate::sql::executor::{SqlExecutionError, SqlExecutor};
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::types::DataType;
use crate::metadata_store::TableCatalog;
use sqlparser::ast::Expr;
use std::collections::HashMap;

use super::{PipelineBatch, PipelineOperator};

pub struct FilterOperator<'a> {
    executor: &'a SqlExecutor,
    expr: &'a Expr,
    physical_expr: Option<&'a PhysicalExpr>,
    catalog: &'a TableCatalog,
    table: &'a str,
    columns: &'a [ColumnCatalog],
    column_ordinals: &'a HashMap<String, usize>,
    column_types: &'a HashMap<String, DataType>,
}

impl<'a> FilterOperator<'a> {
    pub(crate) fn new(
        executor: &'a SqlExecutor,
        expr: &'a Expr,
        physical_expr: Option<&'a PhysicalExpr>,
        catalog: &'a TableCatalog,
        table: &'a str,
        columns: &'a [ColumnCatalog],
        column_ordinals: &'a HashMap<String, usize>,
        column_types: &'a HashMap<String, DataType>,
    ) -> Self {
        Self {
            executor,
            expr,
            physical_expr,
            catalog,
            table,
            columns,
            column_ordinals,
            column_types,
        }
    }
}

impl<'a> PipelineOperator for FilterOperator<'a> {
    fn name(&self) -> &'static str {
        "filter"
    }

    fn execute(&mut self, input: PipelineBatch) -> Result<Vec<PipelineBatch>, SqlExecutionError> {
        let filtered = apply_filter_expr(
            self.executor,
            input,
            self.expr,
            self.physical_expr,
            self.catalog,
            self.table,
            self.columns,
            self.column_ordinals,
            self.column_types,
        )?;
        Ok(vec![filtered])
    }
}
