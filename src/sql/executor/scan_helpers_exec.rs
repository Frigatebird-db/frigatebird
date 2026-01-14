use super::{SqlExecutionError, SqlExecutor};
use crate::metadata_store::ColumnCatalog;
use crate::sql::executor::batch::ColumnarBatch;
use crate::pipeline::planner::SortKeyPrefix;
use crate::sql::executor::scan_stream::{
    BatchStream, PipelineBatchStream, PipelineScanBuilder, SingleBatchStream,
};
use crate::sql::executor::values::compare_strs;
use crate::sql::physical_plan::PhysicalExpr;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

impl SqlExecutor {
    pub(crate) fn locate_rows_by_sort_prefixes(
        &self,
        table: &str,
        sort_columns: &[ColumnCatalog],
        prefixes: &[SortKeyPrefix],
    ) -> Result<Vec<u64>, SqlExecutionError> {
        let mut result = Vec::new();
        for prefix in prefixes {
            if prefix.values.is_empty() || prefix.values.len() > sort_columns.len() {
                continue;
            }
            let columns_slice = &sort_columns[..prefix.len()];
            let rows = self.locate_rows_by_sort_tuple(table, columns_slice, &prefix.values)?;
            result.extend(rows);
        }
        Ok(result)
    }

    pub(crate) fn locate_rows_by_sort_tuple(
        &self,
        table: &str,
        sort_columns: &[ColumnCatalog],
        key_values: &[String],
    ) -> Result<Vec<u64>, SqlExecutionError> {
        if sort_columns.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let first_column = &sort_columns[0].name;
        let descriptors = self.page_directory.pages_for_in_table(table, first_column);
        let target = &key_values[0];
        let mut base = 0u64;
        let mut continue_on_equal_tail = false;

        for descriptor in descriptors {
            let page = self
                .page_handler
                .get_page(descriptor.clone())
                .ok_or_else(|| SqlExecutionError::OperationFailed("unable to load page".into()))?;
            let disk_page = page.page.as_disk_page();
            let entries = disk_page.entries;
            if entries.is_empty() {
                continue;
            }

            let first_cmp = compare_strs(entries.first().unwrap().get_data(), target);
            let last_cmp = compare_strs(entries.last().unwrap().get_data(), target);

            if first_cmp == Ordering::Greater && !continue_on_equal_tail {
                break;
            }
            if last_cmp == Ordering::Less {
                base += entries.len() as u64;
                continue_on_equal_tail = false;
                continue;
            }

            if let Ok(pos) =
                entries.binary_search_by(|entry| compare_strs(entry.get_data(), target))
            {
                let mut idx = pos;
                while idx > 0
                    && compare_strs(entries[idx - 1].get_data(), target) == Ordering::Equal
                {
                    idx -= 1;
                }
                while idx < entries.len()
                    && compare_strs(entries[idx].get_data(), target) == Ordering::Equal
                {
                    result.push(base + idx as u64);
                    idx += 1;
                }
            }

            base += entries.len() as u64;
            continue_on_equal_tail = last_cmp == Ordering::Equal;
            if last_cmp == Ordering::Greater {
                break;
            }
        }

        if sort_columns.len() == 1 {
            return Ok(result);
        }

        let mut refined = Vec::new();
        'outer: for row_idx in result {
            for (column, expected) in sort_columns.iter().zip(key_values.iter()) {
                let entry = self
                    .page_handler
                    .read_entry_at(table, &column.name, row_idx)
                    .ok_or_else(|| {
                        SqlExecutionError::OperationFailed(format!(
                            "unable to read row {row_idx} for column {table}.{}",
                            column.name
                        ))
                    })?;
                if compare_strs(entry.get_data(), expected) != Ordering::Equal {
                    continue 'outer;
                }
            }
            refined.push(row_idx);
        }

        Ok(refined)
    }

    pub(crate) fn build_scan_stream(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        scan_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<Box<dyn BatchStream>, SqlExecutionError> {
        let effective_selection = if row_ids.is_some() {
            None
        } else {
            selection_expr
        };
        if let Some(stream) = self.build_pipeline_scan_stream(
            table,
            columns,
            scan_ordinals,
            effective_selection,
            row_ids,
        )? {
            return Ok(Box::new(stream));
        }
        Ok(Box::new(SingleBatchStream::new(ColumnarBatch::new())))
    }

    pub(crate) fn build_pipeline_scan_stream(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        scan_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        row_ids: Option<Vec<u64>>,
    ) -> Result<Option<PipelineBatchStream>, SqlExecutionError> {
        PipelineScanBuilder::new(
            Arc::clone(&self.page_handler),
            table,
            columns,
            scan_ordinals,
            selection_expr,
            row_ids.map(Arc::new),
        )
        .build()
    }
}
