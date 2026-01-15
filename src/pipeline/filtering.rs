use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::page_handler::PageHandler;
use crate::sql::physical_plan::PhysicalExpr;
use crate::sql::runtime::SqlExecutionError;
use crate::sql::runtime::batch::{Bitmap, ColumnData, ColumnarBatch, ColumnarPage};
use crate::sql::runtime::expressions::evaluate_expression_on_batch;
use crate::sql::runtime::physical_evaluator::PhysicalEvaluator;
use crate::sql::types::DataType;
use sqlparser::ast::Expr;
use std::collections::HashMap;

pub(crate) fn apply_qualify_filter(
    batch: ColumnarBatch,
    expr: &Expr,
    catalog: &TableCatalog,
) -> Result<ColumnarBatch, SqlExecutionError> {
    let filter_page = evaluate_expression_on_batch(expr, &batch, catalog)?;
    let bitmap = boolean_bitmap_from_page(&filter_page)?;
    if bitmap.count_ones() == batch.num_rows {
        return Ok(batch);
    }
    if bitmap.count_ones() == 0 {
        return Ok(ColumnarBatch::new());
    }
    Ok(batch.filter_by_bitmap(&bitmap))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_filter_expr(
    _page_handler: &PageHandler,
    batch: ColumnarBatch,
    expr: &Expr,
    physical_expr: Option<&PhysicalExpr>,
    catalog: &TableCatalog,
    _table: &str,
    _columns: &[ColumnCatalog],
    _column_ordinals: &HashMap<String, usize>,
    _column_types: &HashMap<String, DataType>,
) -> Result<ColumnarBatch, SqlExecutionError> {
    if batch.num_rows == 0 {
        return Ok(batch);
    }

    if let Some(physical_expr) = physical_expr {
        let bitmap = PhysicalEvaluator::evaluate_filter(physical_expr, &batch);
        return Ok(batch.filter_by_bitmap(&bitmap));
    }

    match evaluate_expression_on_batch(expr, &batch, catalog) {
        Ok(filter_page) => {
            let bitmap = boolean_bitmap_from_page(&filter_page)?;
            Ok(batch.filter_by_bitmap(&bitmap))
        }
        Err(SqlExecutionError::Unsupported(_)) => Err(SqlExecutionError::Unsupported(
            "non-vectorized filter expressions are disabled in the pipeline".into(),
        )),
        Err(err) => Err(err),
    }
}

fn boolean_bitmap_from_page(page: &ColumnarPage) -> Result<Bitmap, SqlExecutionError> {
    match &page.data {
        ColumnData::Boolean(values) => {
            let mut bitmap = Bitmap::new(page.len());
            for (idx, &value) in values.iter().enumerate() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                if value {
                    bitmap.set(idx);
                }
            }
            Ok(bitmap)
        }
        ColumnData::Text(col) => {
            let mut bitmap = Bitmap::new(page.len());
            for idx in 0..col.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let value = col.get_bytes(idx);
                if value.eq_ignore_ascii_case(b"true") {
                    bitmap.set(idx);
                }
            }
            Ok(bitmap)
        }
        ColumnData::Dictionary(dict) => {
            let mut bitmap = Bitmap::new(page.len());
            for idx in 0..dict.len() {
                if page.null_bitmap.is_set(idx) {
                    continue;
                }
                let value = dict.get_bytes(idx);
                if value.eq_ignore_ascii_case(b"true") {
                    bitmap.set(idx);
                }
            }
            Ok(bitmap)
        }
        _ => Err(SqlExecutionError::Unsupported(
            "QUALIFY expressions must produce boolean results".into(),
        )),
    }
}
