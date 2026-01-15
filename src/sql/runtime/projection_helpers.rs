use super::SqlExecutionError;
use super::aggregates::MaterializedColumns;
use super::executor_types::{ProjectionItem, ProjectionPlan};
use super::helpers::{
    collect_expr_column_ordinals, column_name_from_expr, object_name_matches_table,
    wildcard_options_supported,
};
use super::values::CachedValue;
use crate::metadata_store::ColumnCatalog;
use crate::page_handler::PageHandler;
use sqlparser::ast::Expr;
use sqlparser::ast::SelectItem;
use std::collections::{BTreeSet, HashMap};

pub(crate) fn build_projection(
    projection: Vec<SelectItem>,
    table_columns: &[ColumnCatalog],
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    table_alias: Option<&str>,
) -> Result<ProjectionPlan, SqlExecutionError> {
    if projection.is_empty() {
        return Err(SqlExecutionError::Unsupported(
            "SELECT requires at least one projection item".into(),
        ));
    }

    let mut iter = projection.into_iter();
    let first = iter.next().expect("projection is not empty");

    match first {
        SelectItem::Wildcard(options) => {
            if iter.next().is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "mixing * with other projection items is not supported".into(),
                ));
            }
            if !wildcard_options_supported(&options) {
                return Err(SqlExecutionError::Unsupported(
                    "wildcard options are not supported".into(),
                ));
            }
            Ok(collect_all_columns(table_columns))
        }
        SelectItem::QualifiedWildcard(object_name, options) => {
            if iter.next().is_some() {
                return Err(SqlExecutionError::Unsupported(
                    "mixing qualified * with other projection items is not supported".into(),
                ));
            }
            if !wildcard_options_supported(&options) {
                return Err(SqlExecutionError::Unsupported(
                    "wildcard options are not supported".into(),
                ));
            }
            if !object_name_matches_table(&object_name, table_name, table_alias) {
                return Err(SqlExecutionError::Unsupported(
                    "qualified wildcard must reference the target table".into(),
                ));
            }
            Ok(collect_all_columns(table_columns))
        }
        item => {
            let mut plan = ProjectionPlan::new();
            push_projection_item(item, column_ordinals, table_name, &mut plan)?;
            for item in iter {
                push_projection_item(item, column_ordinals, table_name, &mut plan)?;
            }
            Ok(plan)
        }
    }
}

fn push_projection_item(
    item: SelectItem,
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    plan: &mut ProjectionPlan,
) -> Result<(), SqlExecutionError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            push_expression_item(expr, None, column_ordinals, table_name, plan)
        }
        SelectItem::ExprWithAlias { expr, alias } => push_expression_item(
            expr,
            Some(alias.value.clone()),
            column_ordinals,
            table_name,
            plan,
        ),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            Err(SqlExecutionError::Unsupported(
                "wildcard projection must be the only projection item".into(),
            ))
        }
    }
}

fn push_expression_item(
    expr: Expr,
    alias: Option<String>,
    column_ordinals: &HashMap<String, usize>,
    table_name: &str,
    plan: &mut ProjectionPlan,
) -> Result<(), SqlExecutionError> {
    if let Some(column_name) = column_name_from_expr(&expr)
        && let Some(&ordinal) = column_ordinals.get(&column_name)
    {
        let header = alias.unwrap_or_else(|| column_name.clone());
        plan.headers.push(header);
        plan.items.push(ProjectionItem::Direct { ordinal });
        plan.required_ordinals.insert(ordinal);
        return Ok(());
    }

    let ordinals = collect_expr_column_ordinals(&expr, column_ordinals, table_name)?;
    plan.required_ordinals.extend(ordinals.iter().copied());
    let header = alias.unwrap_or_else(|| expr.to_string());
    plan.headers.push(header);
    plan.items.push(ProjectionItem::Computed { expr });
    Ok(())
}

fn collect_all_columns(table_columns: &[ColumnCatalog]) -> ProjectionPlan {
    let mut plan = ProjectionPlan::new();
    for column in table_columns {
        plan.headers.push(column.name.clone());
        plan.items.push(ProjectionItem::Direct {
            ordinal: column.ordinal,
        });
        plan.required_ordinals.insert(column.ordinal);
    }
    plan
}

pub(crate) fn materialize_columns(
    page_handler: &PageHandler,
    table: &str,
    table_columns: &[ColumnCatalog],
    ordinals: &BTreeSet<usize>,
    rows: &[u64],
) -> Result<MaterializedColumns, SqlExecutionError> {
    let mut result: MaterializedColumns = HashMap::with_capacity(ordinals.len());
    if ordinals.is_empty() || rows.is_empty() {
        return Ok(result);
    }

    for &ordinal in ordinals {
        let column = table_columns.get(ordinal).ok_or_else(|| {
            SqlExecutionError::OperationFailed(format!(
                "invalid column ordinal {ordinal} on table {table}"
            ))
        })?;
        let page = page_handler.gather_column_for_rows(table, &column.name, rows);
        let mut values: HashMap<u64, CachedValue> = HashMap::with_capacity(rows.len());
        for (idx, row_id) in rows.iter().copied().enumerate() {
            let value = page
                .get_value_as_string(idx)
                .map(CachedValue::Text)
                .unwrap_or(CachedValue::Null);
            values.insert(row_id, value);
        }
        result.insert(ordinal, values);
    }

    Ok(result)
}
