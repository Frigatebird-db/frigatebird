use crate::metadata_store::{CatalogError, ColumnDefinition, PageDirectory, TableDefinition};
use crate::sql::CreateTablePlan;

pub fn create_table_from_plan(
    directory: &PageDirectory,
    plan: &CreateTablePlan,
) -> Result<(), CatalogError> {
    let columns: Vec<ColumnDefinition> = plan
        .columns
        .iter()
        .map(|spec| ColumnDefinition::new(spec.name.clone(), spec.data_type.clone()))
        .collect();
    let definition = TableDefinition::new(plan.table_name.clone(), columns, plan.order_by.clone());
    match directory.register_table(definition) {
        Ok(_) => Ok(()),
        Err(CatalogError::TableExists(_)) if plan.if_not_exists => Ok(()),
        Err(err) => Err(err),
    }
}
