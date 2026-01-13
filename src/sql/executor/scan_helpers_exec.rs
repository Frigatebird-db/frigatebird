use super::{SqlExecutionError, SqlExecutor};
use crate::metadata_store::ColumnCatalog;
use crate::sql::executor::batch::{Bitmap, ColumnarBatch};
use crate::sql::executor::physical_evaluator::PhysicalEvaluator;
use crate::sql::executor::scan_helpers::SortKeyPrefix;
use crate::sql::executor::scan_stream::{
    BatchStream, PipelineBatchStream, PipelineScanBuilder, RowIdBatchStream, SingleBatchStream,
};
use crate::sql::executor::values::compare_strs;
use crate::sql::physical_plan::PhysicalExpr;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
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

    pub(crate) fn scan_matching_rows_vectorized(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        column_ordinals: &HashMap<String, usize>,
        rows_per_page_group: u64,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        let mut rows = Vec::new();
        let driving_col_ordinal = if let Some(&ord) = required_ordinals.iter().next() {
            ord
        } else {
            return Ok(rows);
        };
        let driving_col = &columns[driving_col_ordinal];

        // Collect all column names needed
        let mut required_names = Vec::with_capacity(required_ordinals.len());
        for &ordinal in required_ordinals {
            required_names.push(columns[ordinal].name.clone());
        }

        // Snapshot all metadata at once
        let all_pages = self.page_handler.get_column_pages(table, &required_names);
        let descriptors = all_pages
            .get(&driving_col.name)
            .cloned()
            .unwrap_or_default();

        for (page_idx, _) in descriptors.iter().enumerate() {
            let mut batch_slice = ColumnarBatch::with_capacity(required_ordinals.len());
            let mut pages_keepalive = Vec::with_capacity(required_ordinals.len());
            let mut num_rows = 0;
            let mut segment_valid = true;

            for &ordinal in required_ordinals {
                let column = &columns[ordinal];
                let col_descriptors = all_pages.get(&column.name);
                if let Some(desc_list) = col_descriptors {
                    if let Some(desc) = desc_list.get(page_idx) {
                        let page_arc =
                            self.page_handler.get_page(desc.clone()).ok_or_else(|| {
                                SqlExecutionError::OperationFailed("page load failed".into())
                            })?;
                        num_rows = page_arc.page.num_rows;
                        pages_keepalive.push((ordinal, page_arc));
                    } else {
                        segment_valid = false;
                        break;
                    }
                } else {
                    segment_valid = false;
                    break;
                }
            }

            if !segment_valid || num_rows == 0 {
                continue;
            }

            for (ordinal, page_arc) in &pages_keepalive {
                batch_slice.columns.insert(*ordinal, page_arc.page.clone());
            }
            batch_slice.num_rows = num_rows;
            batch_slice.aliases = column_ordinals.clone();

            let bitmap = if let Some(expr) = selection_expr {
                PhysicalEvaluator::evaluate_filter(expr, &batch_slice)
            } else {
                let mut bm = Bitmap::new(num_rows);
                bm.fill(true);
                bm
            };

            if bitmap.count_ones() == 0 {
                continue;
            }

            let base = (page_idx as u64) * rows_per_page_group;
            for idx in bitmap.iter_ones() {
                rows.push(base + idx as u64);
            }
        }

        Ok(rows)
    }

    pub(crate) fn scan_rows_rowwise(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        column_ordinals: &HashMap<String, usize>,
        rows_per_page_group: u64,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        self.scan_matching_rows_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
            rows_per_page_group,
        )
    }

    pub(crate) fn scan_rows_via_full_table(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        required_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        column_ordinals: &HashMap<String, usize>,
        rows_per_page_group: u64,
    ) -> Result<Vec<u64>, SqlExecutionError> {
        match self.scan_matching_rows_vectorized(
            table,
            columns,
            required_ordinals,
            selection_expr,
            column_ordinals,
            rows_per_page_group,
        ) {
            Ok(rows) => Ok(rows),
            Err(SqlExecutionError::Unsupported(_)) => self.scan_rows_rowwise(
                table,
                columns,
                required_ordinals,
                selection_expr,
                column_ordinals,
                rows_per_page_group,
            ),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn build_scan_stream(
        &self,
        table: &str,
        columns: &[ColumnCatalog],
        scan_ordinals: &BTreeSet<usize>,
        selection_expr: Option<&PhysicalExpr>,
        column_ordinals: &HashMap<String, usize>,
        rows_per_page_group: u64,
        row_ids: Option<Vec<u64>>,
    ) -> Result<Box<dyn BatchStream>, SqlExecutionError> {
        if let Some(row_ids) = row_ids {
            let ordinals = scan_ordinals.iter().copied().collect();
            return Ok(Box::new(RowIdBatchStream::new(
                Arc::clone(&self.page_handler),
                table.to_string(),
                columns.to_vec(),
                ordinals,
                row_ids,
                rows_per_page_group,
                column_ordinals.clone(),
            )));
        }
        if let Some(stream) =
            self.build_pipeline_scan_stream(table, columns, scan_ordinals, selection_expr)?
        {
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
    ) -> Result<Option<PipelineBatchStream>, SqlExecutionError> {
        PipelineScanBuilder::new(
            Arc::clone(&self.page_handler),
            table,
            columns,
            scan_ordinals,
            selection_expr,
        )
        .build()
    }
}
