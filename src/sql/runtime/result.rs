use super::batch::ColumnarBatch;
use std::fmt;

#[derive(Clone)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub batches: Vec<ColumnarBatch>,
}

impl SelectResult {
    pub fn row_iter(&self) -> RowIter<'_> {
        RowIter {
            result: self,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|batch| batch.num_rows == 0)
    }
}

impl fmt::Debug for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectResult")
            .field("columns", &self.columns)
            .field("row_count", &self.row_count())
            .finish()
    }
}

impl fmt::Display for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            writeln!(f, "(0 columns)")?;
        } else {
            for (idx, column) in self.columns.iter().enumerate() {
                if idx > 0 {
                    write!(f, " | ")?;
                }
                write!(f, "{column}")?;
            }
            writeln!(f)?;
        }

        let mut row_count = 0usize;
        for batch in &self.batches {
            for row_idx in 0..batch.num_rows {
                row_count += 1;
                for col_idx in 0..self.columns.len() {
                    if col_idx > 0 {
                        write!(f, " | ")?;
                    }
                    let value = batch
                        .columns
                        .get(&col_idx)
                        .ok_or(fmt::Error)?
                        .value_as_string(row_idx);
                    match value {
                        Some(text) => write!(f, "{text}")?,
                        None => write!(f, "NULL")?,
                    }
                }
                writeln!(f)?;
            }
        }

        writeln!(f, "({row_count} rows)")
    }
}

pub struct RowIter<'a> {
    result: &'a SelectResult,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = RowView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.batch_idx < self.result.batches.len() {
            let batch = &self.result.batches[self.batch_idx];
            if self.row_idx >= batch.num_rows {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }
            let view = RowView {
                batch,
                row_idx: self.row_idx,
                column_count: self.result.columns.len(),
            };
            self.row_idx += 1;
            return Some(view);
        }
        None
    }
}

pub struct RowView<'a> {
    batch: &'a ColumnarBatch,
    row_idx: usize,
    column_count: usize,
}

impl<'a> RowView<'a> {
    pub fn value_as_string(&self, column_idx: usize) -> Option<String> {
        self.batch
            .columns
            .get(&column_idx)
            .and_then(|page| page.value_as_string(self.row_idx))
    }

    pub fn to_vec(&self) -> Vec<Option<String>> {
        let mut row = Vec::with_capacity(self.column_count);
        for column_idx in 0..self.column_count {
            row.push(
                self.batch
                    .columns
                    .get(&column_idx)
                    .and_then(|page| page.value_as_string(self.row_idx)),
            );
        }
        row
    }
}
