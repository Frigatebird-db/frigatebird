use crate::sql::models::{
    ColumnSpec, CreateTablePlan, FilterExpr, PlannerError, PlannerResult, QueryPlan, TableAccess,
};
use sqlparser::ast::{
    Assignment, BinaryOperator, ColumnDef, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    GroupByExpr, Ident, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, WindowType,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn plan_statement(statement: &Statement) -> PlannerResult<QueryPlan> {
    match statement {
        Statement::Query(query) => plan_query(query),
        Statement::Insert {
            table_name,
            columns,
            source,
            returning,
            ..
        } => plan_insert(
            table_name,
            columns,
            source.as_deref(),
            returning.as_ref().map(|items| items.as_slice()),
        ),
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            from,
        } => {
            if from.is_some() {
                return Err(PlannerError::Unsupported(
                    "UPDATE ... FROM is not supported yet".into(),
                ));
            }
            plan_update(
                table,
                assignments,
                selection.as_ref(),
                returning.as_ref().map(|items| items.as_slice()),
            )
        }
        Statement::Delete {
            tables,
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
        } => {
            if !tables.is_empty() || using.is_some() {
                return Err(PlannerError::Unsupported(
                    "DELETE with multiple tables is not supported yet".into(),
                ));
            }
            plan_delete(
                from,
                selection.as_ref(),
                returning.as_ref().map(|items| items.as_slice()),
                order_by,
                limit.as_ref(),
            )
        }
        _ => Err(PlannerError::Unsupported(
            "only SELECT, INSERT, UPDATE, and DELETE statements are supported for now".into(),
        )),
    }
}

pub fn plan_create_table_statement(statement: &Statement) -> PlannerResult<CreateTablePlan> {
    match statement {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            query,
            like,
            order_by,
            if_not_exists,
            or_replace,
            temporary,
            ..
        } => {
            if *or_replace || *temporary {
                return Err(PlannerError::Unsupported(
                    "CREATE TABLE with REPLACE or TEMPORARY is not supported yet".into(),
                ));
            }
            if like.is_some() || query.is_some() {
                return Err(PlannerError::Unsupported(
                    "CREATE TABLE ... LIKE/AS SELECT is not supported yet".into(),
                ));
            }
            if !constraints.is_empty() || !with_options.is_empty() {
                return Err(PlannerError::Unsupported(
                    "CREATE TABLE constraints or WITH options are not supported yet".into(),
                ));
            }
            if columns.is_empty() {
                return Err(PlannerError::Unsupported(
                    "CREATE TABLE must specify at least one column".into(),
                ));
            }

            let mut specs = Vec::with_capacity(columns.len());
            for column in columns {
                specs.push(column_to_spec(column));
            }

            let mut sort_key = Vec::new();
            if let Some(items) = order_by {
                for ident in items {
                    sort_key.push(ident.value.clone());
                }
            }

            Ok(CreateTablePlan::new(
                object_name_to_string(name),
                specs,
                sort_key,
                *if_not_exists,
            ))
        }
        _ => Err(PlannerError::Unsupported(
            "only CREATE TABLE statements are supported".into(),
        )),
    }
}

fn plan_query(query: &Query) -> PlannerResult<QueryPlan> {
    let mut plan = match query.body.as_ref() {
        SetExpr::Select(select) => plan_select(select)?,
        SetExpr::Query(inner) => plan_query(inner)?,
        _ => Err(PlannerError::Unsupported(
            "only simple SELECT queries are supported right now".into(),
        ))?,
    };

    if let Some(table) = plan.tables.first_mut() {
        collect_order_by_columns(&query.order_by, &mut table.read_columns)?;
        if let Some(limit) = &query.limit {
            collect_expr_columns(limit, &mut table.read_columns)?;
        }
        for expr in &query.limit_by {
            collect_expr_columns(expr, &mut table.read_columns)?;
        }
        if let Some(offset) = &query.offset {
            collect_expr_columns(&offset.value, &mut table.read_columns)?;
        }
        if let Some(fetch) = &query.fetch {
            if let Some(quantity) = &fetch.quantity {
                collect_expr_columns(quantity, &mut table.read_columns)?;
            }
        }
    }

    Ok(plan)
}

fn plan_select(select: &Select) -> PlannerResult<QueryPlan> {
    if select.from.len() > 1 {
        return Err(PlannerError::Unsupported(
            "SELECT with multiple FROM tables is not supported yet".into(),
        ));
    }

    if select.from.is_empty() {
        return Ok(QueryPlan::new(vec![]));
    }

    let table_name = extract_table_name(&select.from[0])?;
    let mut table_access = TableAccess::new(table_name);
    collect_select_columns(select, &mut table_access.read_columns)?;

    if let Some(selection) = &select.selection {
        attach_filter(&mut table_access, selection);
    }
    if let Some(having) = &select.having {
        attach_filter(&mut table_access, having);
    }
    if let Some(qualify) = &select.qualify {
        attach_filter(&mut table_access, qualify);
    }

    Ok(QueryPlan::new(vec![table_access]))
}

fn attach_filter(table_access: &mut TableAccess, expr: &Expr) {
    let filter = build_filter(expr);
    table_access.add_filter(filter);
}

fn build_filter(expr: &Expr) -> FilterExpr {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => FilterExpr::and(build_filter(left), build_filter(right)),
            BinaryOperator::Or => FilterExpr::or(build_filter(left), build_filter(right)),
            _ => FilterExpr::leaf(expr.clone()),
        },
        Expr::Nested(inner) => build_filter(inner),
        _ => FilterExpr::leaf(expr.clone()),
    }
}

fn plan_insert(
    table_name: &ObjectName,
    columns: &[Ident],
    source: Option<&Query>,
    returning: Option<&[SelectItem]>,
) -> PlannerResult<QueryPlan> {
    let mut tables = Vec::new();
    let mut target = TableAccess::new(object_name_to_string(table_name));

    if columns.is_empty() {
        target.write_columns.insert("*".into());
    } else {
        for column in columns {
            target.write_columns.insert(column.value.clone());
        }
    }

    if let Some(items) = returning {
        collect_select_items(items, &mut target.read_columns)?;
    }

    tables.push(target);

    if let Some(source_query) = source {
        let source_plan = plan_query(source_query)?;
        tables.extend(source_plan.tables);
    }

    Ok(merge_tables(tables))
}

fn plan_update(
    table: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
    returning: Option<&[SelectItem]>,
) -> PlannerResult<QueryPlan> {
    if !table.joins.is_empty() {
        return Err(PlannerError::Unsupported(
            "UPDATE with JOINs is not supported yet".into(),
        ));
    }

    let table_name = extract_table_name(table)?;
    let mut table_access = TableAccess::new(table_name);

    for assignment in assignments {
        if let Some(column_name) = assignment.id.last() {
            table_access.write_columns.insert(column_name.value.clone());
        }
        collect_expr_columns(&assignment.value, &mut table_access.read_columns)?;
    }

    if let Some(predicate) = selection {
        collect_expr_columns(predicate, &mut table_access.read_columns)?;
        attach_filter(&mut table_access, predicate);
    }

    if let Some(items) = returning {
        collect_select_items(items, &mut table_access.read_columns)?;
    }

    Ok(QueryPlan::new(vec![table_access]))
}

fn plan_delete(
    from: &FromTable,
    selection: Option<&Expr>,
    returning: Option<&[SelectItem]>,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
) -> PlannerResult<QueryPlan> {
    let table_with_joins = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return Err(PlannerError::Unsupported(
                    "DELETE with multiple tables is not supported yet".into(),
                ));
            }
            &tables[0]
        }
    };

    if !table_with_joins.joins.is_empty() {
        return Err(PlannerError::Unsupported(
            "DELETE with JOINs is not supported yet".into(),
        ));
    }

    let table_name = extract_table_name(table_with_joins)?;
    let mut table_access = TableAccess::new(table_name);

    if let Some(predicate) = selection {
        collect_expr_columns(predicate, &mut table_access.read_columns)?;
        attach_filter(&mut table_access, predicate);
    }

    collect_order_by_columns(order_by, &mut table_access.read_columns)?;

    if let Some(expr) = limit {
        collect_expr_columns(expr, &mut table_access.read_columns)?;
    }

    if let Some(items) = returning {
        collect_select_items(items, &mut table_access.read_columns)?;
    }

    Ok(QueryPlan::new(vec![table_access]))
}

fn collect_select_columns(select: &Select, columns: &mut BTreeSet<String>) -> PlannerResult<()> {
    if let Some(distinct) = &select.distinct {
        if let sqlparser::ast::Distinct::On(exprs) = distinct {
            for expr in exprs {
                collect_expr_columns(expr, columns)?;
            }
        }
    }

    if let Some(top) = &select.top {
        if let Some(quantity) = &top.quantity {
            if let sqlparser::ast::TopQuantity::Expr(expr) = quantity {
                collect_expr_columns(expr, columns)?;
            }
        }
    }

    for item in &select.projection {
        collect_select_item(item, columns)?;
    }

    if let Some(selection) = &select.selection {
        collect_expr_columns(selection, columns)?;
    }

    match &select.group_by {
        GroupByExpr::All => {}
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                collect_expr_columns(expr, columns)?;
            }
        }
    }

    for expr in &select.cluster_by {
        collect_expr_columns(expr, columns)?;
    }

    for expr in &select.distribute_by {
        collect_expr_columns(expr, columns)?;
    }

    for expr in &select.sort_by {
        collect_expr_columns(expr, columns)?;
    }

    if let Some(having) = &select.having {
        collect_expr_columns(having, columns)?;
    }

    if let Some(qualify) = &select.qualify {
        collect_expr_columns(qualify, columns)?;
    }

    Ok(())
}

fn collect_select_items(items: &[SelectItem], columns: &mut BTreeSet<String>) -> PlannerResult<()> {
    for item in items {
        collect_select_item(item, columns)?;
    }
    Ok(())
}

fn collect_select_item(item: &SelectItem, columns: &mut BTreeSet<String>) -> PlannerResult<()> {
    match item {
        SelectItem::UnnamedExpr(expr) => collect_expr_columns(expr, columns),
        SelectItem::ExprWithAlias { expr, .. } => collect_expr_columns(expr, columns),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            columns.insert("*".into());
            Ok(())
        }
    }
}

fn collect_order_by_columns(
    order_by: &[OrderByExpr],
    columns: &mut BTreeSet<String>,
) -> PlannerResult<()> {
    for expr in order_by {
        collect_expr_columns(&expr.expr, columns)?;
    }
    Ok(())
}

fn collect_expr_columns(expr: &Expr, columns: &mut BTreeSet<String>) -> PlannerResult<()> {
    use Expr::*;

    match expr {
        Identifier(ident) => {
            columns.insert(ident.value.clone());
        }
        CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.insert(last.value.clone());
            }
        }
        QualifiedWildcard(_) | Wildcard => {
            columns.insert("*".into());
        }
        JsonAccess { left, right, .. } => {
            collect_expr_columns(left, columns)?;
            collect_expr_columns(right, columns)?;
        }
        CompositeAccess { expr, .. } => collect_expr_columns(expr, columns)?,
        IsFalse(expr) | IsNotFalse(expr) | IsTrue(expr) | IsNotTrue(expr) | IsNull(expr)
        | IsNotNull(expr) | IsUnknown(expr) | IsNotUnknown(expr) => {
            collect_expr_columns(expr, columns)?
        }
        IsDistinctFrom(left, right) | IsNotDistinctFrom(left, right) => {
            collect_expr_columns(left, columns)?;
            collect_expr_columns(right, columns)?;
        }
        InList {
            expr: inner, list, ..
        } => {
            collect_expr_columns(inner, columns)?;
            for item in list {
                collect_expr_columns(item, columns)?;
            }
        }
        InSubquery { expr: inner, .. } | InUnnest { expr: inner, .. } => {
            collect_expr_columns(inner, columns)?;
        }
        Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_expr_columns(inner, columns)?;
            collect_expr_columns(low, columns)?;
            collect_expr_columns(high, columns)?;
        }
        BinaryOp { left, right, .. }
        | Like {
            expr: left,
            pattern: right,
            ..
        }
        | ILike {
            expr: left,
            pattern: right,
            ..
        }
        | SimilarTo {
            expr: left,
            pattern: right,
            ..
        }
        | RLike {
            expr: left,
            pattern: right,
            ..
        }
        | AnyOp { left, right, .. }
        | AllOp { left, right, .. } => {
            collect_expr_columns(left, columns)?;
            collect_expr_columns(right, columns)?;
        }
        UnaryOp { expr: inner, .. } | Nested(inner) => collect_expr_columns(inner, columns)?,
        Convert { expr: inner, .. }
        | Cast { expr: inner, .. }
        | TryCast { expr: inner, .. }
        | SafeCast { expr: inner, .. }
        | AtTimeZone {
            timestamp: inner, ..
        }
        | Extract { expr: inner, .. }
        | Ceil { expr: inner, .. }
        | Floor { expr: inner, .. }
        | Collate { expr: inner, .. } => collect_expr_columns(inner, columns)?,
        Position { expr: inner, r#in } => {
            collect_expr_columns(inner, columns)?;
            collect_expr_columns(r#in, columns)?;
        }
        Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            collect_expr_columns(inner, columns)?;
            if let Some(expr) = substring_from {
                collect_expr_columns(expr, columns)?;
            }
            if let Some(expr) = substring_for {
                collect_expr_columns(expr, columns)?;
            }
        }
        Trim {
            expr: inner,
            trim_what,
            trim_characters,
            ..
        } => {
            collect_expr_columns(inner, columns)?;
            if let Some(expr) = trim_what {
                collect_expr_columns(expr, columns)?;
            }
            if let Some(chars) = trim_characters {
                for expr in chars {
                    collect_expr_columns(expr, columns)?;
                }
            }
        }
        Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_expr_columns(inner, columns)?;
            collect_expr_columns(overlay_what, columns)?;
            collect_expr_columns(overlay_from, columns)?;
            if let Some(expr) = overlay_for {
                collect_expr_columns(expr, columns)?;
            }
        }
        Value(_) | IntroducedString { .. } | TypedString { .. } => {}
        MapAccess { column, keys } => {
            collect_expr_columns(column, columns)?;
            for key in keys {
                collect_expr_columns(key, columns)?;
            }
        }
        ArrayIndex { obj, indexes } => {
            collect_expr_columns(obj, columns)?;
            for idx in indexes {
                collect_expr_columns(idx, columns)?;
            }
        }
        Array(array) => {
            for expr in &array.elem {
                collect_expr_columns(expr, columns)?;
            }
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                collect_expr_columns(expr, columns)?;
            }
            for expr in conditions {
                collect_expr_columns(expr, columns)?;
            }
            for expr in results {
                collect_expr_columns(expr, columns)?;
            }
            if let Some(expr) = else_result {
                collect_expr_columns(expr, columns)?;
            }
        }
        Tuple(exprs) => {
            for expr in exprs {
                collect_expr_columns(expr, columns)?;
            }
        }
        Struct { values, .. } => {
            for expr in values {
                collect_expr_columns(expr, columns)?;
            }
        }
        Named { expr: inner, .. } => collect_expr_columns(inner, columns)?,
        Interval(interval) => collect_expr_columns(&interval.value, columns)?,
        MatchAgainst {
            columns: expr_cols, ..
        } => {
            for ident in expr_cols {
                columns.insert(ident.value.clone());
            }
        }
        AggregateExpressionWithFilter {
            expr: inner,
            filter,
        } => {
            collect_expr_columns(inner, columns)?;
            collect_expr_columns(filter, columns)?;
        }
        Function(function) => collect_function_columns(function, columns)?,
        Exists { .. }
        | Subquery(_)
        | ArraySubquery(_)
        | ListAgg(_)
        | ArrayAgg(_)
        | GroupingSets(_)
        | Cube(_)
        | Rollup(_)
        | OuterJoin(_) => {
            // These constructs either operate on nested queries or represent complex
            // read patterns; treat them as touching the full row set.
            columns.insert("*".into());
        }
    }

    Ok(())
}

fn collect_function_columns(
    function: &Function,
    columns: &mut BTreeSet<String>,
) -> PlannerResult<()> {
    for arg in &function.args {
        collect_function_arg_columns(arg, columns)?;
    }

    if let Some(filter) = &function.filter {
        collect_expr_columns(filter, columns)?;
    }

    if let Some(window) = &function.over {
        if let WindowType::WindowSpec(spec) = window {
            for expr in &spec.partition_by {
                collect_expr_columns(expr, columns)?;
            }
            for expr in &spec.order_by {
                collect_expr_columns(&expr.expr, columns)?;
            }
        }
    }

    for expr in &function.order_by {
        collect_expr_columns(&expr.expr, columns)?;
    }

    Ok(())
}

fn collect_function_arg_columns(
    arg: &FunctionArg,
    columns: &mut BTreeSet<String>,
) -> PlannerResult<()> {
    match arg {
        FunctionArg::Unnamed(expr) => match expr {
            FunctionArgExpr::Expr(expr) => collect_expr_columns(expr, columns),
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => {
                columns.insert("*".into());
                Ok(())
            }
        },
        FunctionArg::Named { arg, .. } => match arg {
            FunctionArgExpr::Expr(expr) => collect_expr_columns(expr, columns),
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => {
                columns.insert("*".into());
                Ok(())
            }
        },
    }
}

fn extract_table_name(table: &TableWithJoins) -> PlannerResult<String> {
    if !table.joins.is_empty() {
        return Err(PlannerError::Unsupported(
            "JOINs are not supported yet".into(),
        ));
    }

    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(PlannerError::Unsupported(
            "only direct table references are supported for now".into(),
        )),
    }
}

fn column_to_spec(column: &ColumnDef) -> ColumnSpec {
    ColumnSpec::new(column.name.value.clone(), column.data_type.to_string())
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn merge_tables(tables: Vec<TableAccess>) -> QueryPlan {
    let mut merged: BTreeMap<String, TableAccess> = BTreeMap::new();
    for table in tables {
        merged
            .entry(table.table_name.clone())
            .and_modify(|existing| existing.merge_from(&table))
            .or_insert(table);
    }
    QueryPlan::new(merged.into_values().collect())
}
