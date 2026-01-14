use crate::metadata_store::{ColumnCatalog, TableCatalog};
use crate::sql::runtime::aggregates::MaterializedColumns;
use crate::sql::runtime::batch::{Bitmap, ColumnData, ColumnarBatch, ColumnarPage};
use crate::sql::runtime::expressions::evaluate_expression_on_batch;
use crate::sql::runtime::physical_evaluator::PhysicalEvaluator;
use crate::sql::runtime::projection_helpers::materialize_columns;
use crate::sql::runtime::SqlExecutionError;
use crate::page_handler::PageHandler;
use crate::sql::physical_plan::PhysicalExpr;
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

pub(crate) fn apply_filter_expr(
    page_handler: &PageHandler,
    batch: ColumnarBatch,
    expr: &Expr,
    physical_expr: Option<&PhysicalExpr>,
    catalog: &TableCatalog,
    table: &str,
    columns: &[ColumnCatalog],
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
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
        Err(SqlExecutionError::Unsupported(_)) => {
            let ordinals = crate::sql::runtime::helpers::collect_expr_column_ordinals(
                expr,
                column_ordinals,
                table,
            )?;
            let materialized = materialize_columns(
                page_handler,
                table,
                columns,
                &ordinals,
                &batch.row_ids,
            )?;
            let matching_rows = filter_rows_with_expr(
                expr,
                &batch.row_ids,
                &materialized,
                column_ordinals,
                column_types,
                false,
            )?;
            if matching_rows.is_empty() {
                return Ok(ColumnarBatch::new());
            }
            let matching: std::collections::HashSet<u64> =
                matching_rows.into_iter().collect();
            let mut bitmap = Bitmap::new(batch.num_rows);
            for (idx, row_id) in batch.row_ids.iter().enumerate() {
                if matching.contains(row_id) {
                    bitmap.set(idx);
                }
            }
            Ok(batch.filter_by_bitmap(&bitmap))
        }
        Err(err) => Err(err),
    }
}

fn boolean_bitmap_from_page(page: &ColumnarPage) -> Result<Bitmap, SqlExecutionError> {
    match &page.data {
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
        _ => Err(SqlExecutionError::Unsupported(
            "QUALIFY expressions must produce boolean results".into(),
        )),
    }
}

fn filter_rows_with_expr(
    expr: &Expr,
    rows: &[u64],
    materialized: &MaterializedColumns,
    column_ordinals: &HashMap<String, usize>,
    column_types: &HashMap<String, DataType>,
    prefer_exact_numeric: bool,
) -> Result<Vec<u64>, SqlExecutionError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let dataset = crate::sql::runtime::aggregates::AggregateDataset::new(
        rows,
        materialized,
        column_ordinals,
        column_types,
        None,
        prefer_exact_numeric,
    );

    let mut filtered = Vec::with_capacity(rows.len());
    for &row_idx in rows {
        let value = crate::sql::runtime::expressions::evaluate_row_expr(
            expr,
            row_idx,
            &dataset,
        )?;
        if value.as_bool().unwrap_or(false) {
            filtered.push(row_idx);
        }
    }

    Ok(filtered)
}
