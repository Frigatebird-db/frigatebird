use super::batch::{Bitmap, BytesColumn, ColumnData, ColumnarBatch, ColumnarPage};
use std::collections::HashSet;

#[derive(Hash, PartialEq, Eq)]
enum DistinctValue {
    Null,
    Int(i64),
    Float(u64),
    Text(String),
    Boolean(bool),
    Timestamp(i64),
}

#[derive(Hash, PartialEq, Eq)]
struct DistinctKey {
    values: Vec<DistinctValue>,
}
fn build_distinct_key(
    batch: &ColumnarBatch,
    row_idx: usize,
    column_count: usize,
) -> DistinctKey {
    let mut values = Vec::with_capacity(column_count);
    for ordinal in 0..column_count {
        let value = match batch.columns.get(&ordinal) {
            Some(page) => match &page.data {
                ColumnData::Int64(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Int(data[row_idx])
                    }
                }
                ColumnData::Float64(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Float(data[row_idx].to_bits())
                    }
                }
                ColumnData::Text(col) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Text(col.get_string(row_idx))
                    }
                }
                ColumnData::Boolean(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Boolean(data[row_idx])
                    }
                }
                ColumnData::Timestamp(data) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Timestamp(data[row_idx])
                    }
                }
                ColumnData::Dictionary(dict) => {
                    if page.null_bitmap.is_set(row_idx) {
                        DistinctValue::Null
                    } else {
                        DistinctValue::Text(dict.get_string(row_idx))
                    }
                }
            },
            None => DistinctValue::Null,
        };
        values.push(value);
    }
    DistinctKey { values }
}

pub(crate) fn deduplicate_batches(
    batches: Vec<ColumnarBatch>,
    column_count: usize,
) -> Vec<ColumnarBatch> {
    if batches.is_empty() || column_count == 0 {
        return batches;
    }

    let mut seen: HashSet<DistinctKey> = HashSet::new();
    let mut deduped: Vec<ColumnarBatch> = Vec::new();

    for batch in batches.into_iter() {
        if batch.num_rows == 0 {
            continue;
        }
        let mut indices: Vec<usize> = Vec::new();
        for row_idx in 0..batch.num_rows {
            let key = build_distinct_key(&batch, row_idx, column_count);
            if seen.insert(key) {
                indices.push(row_idx);
            }
        }
        if !indices.is_empty() {
            deduped.push(batch.gather(&indices));
        }
    }
    deduped
}

pub(crate) fn chunk_batch(batch: &ColumnarBatch, chunk_size: usize) -> Vec<ColumnarBatch> {
    if batch.num_rows == 0 {
        return Vec::new();
    }
    if batch.num_rows <= chunk_size {
        return vec![batch.clone()];
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < batch.num_rows {
        let end = (start + chunk_size).min(batch.num_rows);
        chunks.push(batch.slice(start, end));
        start = end;
    }
    chunks
}

pub(crate) fn merge_batches(mut batches: Vec<ColumnarBatch>) -> ColumnarBatch {
    if batches.is_empty() {
        return ColumnarBatch::new();
    }
    let mut merged = batches.remove(0);
    for batch in batches {
        merged.append(&batch);
    }
    merged
}

fn strings_to_text_column(values: Vec<Option<String>>) -> ColumnarPage {
    let len = values.len();
    let mut null_bitmap = Bitmap::new(len);
    let mut col = BytesColumn::with_capacity(len, len * 16);
    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Some(text) => col.push(&text),
            None => {
                null_bitmap.set(idx);
                col.push("");
            }
        }
    }
    ColumnarPage {
        page_metadata: String::new(),
        data: ColumnData::Text(col),
        null_bitmap,
        num_rows: len,
    }
}

pub(super) fn rows_to_batch(rows: Vec<Vec<Option<String>>>) -> ColumnarBatch {
    if rows.is_empty() {
        return ColumnarBatch::new();
    }

    let column_count = rows[0].len();
    let mut batch = ColumnarBatch::with_capacity(column_count);
    batch.num_rows = rows.len();
    batch.row_ids = (0..rows.len() as u64).collect();
    for column_idx in 0..column_count {
        let mut column_values = Vec::with_capacity(rows.len());
        for row in &rows {
            column_values.push(row.get(column_idx).cloned().unwrap_or(None));
        }
        batch
            .columns
            .insert(column_idx, strings_to_text_column(column_values));
    }
    batch
}
