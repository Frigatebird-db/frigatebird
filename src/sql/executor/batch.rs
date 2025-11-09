use crate::entry::Entry;
use crate::page::Page;

use super::values::{encode_null, format_float, is_encoded_null};

/// Bitmap used for null tracking and predicate evaluation.
#[derive(Clone, Debug)]
pub struct Bitmap {
    bits: Vec<u64>,
    len: usize,
}

impl Bitmap {
    pub fn new(len: usize) -> Self {
        let words = if len == 0 { 0 } else { (len + 63) / 64 };
        Self {
            bits: vec![0; words],
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn set(&mut self, idx: usize) {
        if idx >= self.len {
            return;
        }
        let (word_idx, bit_idx) = Self::bit_position(idx);
        if word_idx >= self.bits.len() {
            self.bits.resize(word_idx + 1, 0);
        }
        self.bits[word_idx] |= 1u64 << bit_idx;
    }

    pub fn clear(&mut self, idx: usize) {
        if idx >= self.len {
            return;
        }
        let (word_idx, bit_idx) = Self::bit_position(idx);
        if let Some(word) = self.bits.get_mut(word_idx) {
            *word &= !(1u64 << bit_idx);
        }
    }

    pub fn is_set(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let (word_idx, bit_idx) = Self::bit_position(idx);
        self.bits
            .get(word_idx)
            .map(|word| (word & (1u64 << bit_idx)) != 0)
            .unwrap_or(false)
    }

    pub fn fill(&mut self, value: bool) {
        let fill_word = if value { u64::MAX } else { 0 };
        for word in &mut self.bits {
            *word = fill_word;
        }
        if value {
            self.mask_unused_bits();
        }
    }

    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|word| word.count_ones() as usize).sum()
    }

    pub fn and(&mut self, other: &Bitmap) {
        self.ensure_compatible(other);
        for (lhs, rhs) in self.bits.iter_mut().zip(other.bits.iter()) {
            *lhs &= *rhs;
        }
    }

    pub fn or(&mut self, other: &Bitmap) {
        self.ensure_compatible(other);
        for (lhs, rhs) in self.bits.iter_mut().zip(other.bits.iter()) {
            *lhs |= *rhs;
        }
    }

    pub fn invert(&mut self) {
        for word in &mut self.bits {
            *word = !*word;
        }
        self.mask_unused_bits();
    }

    pub fn ones_indices(&self) -> Vec<usize> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(self.count_ones());
        for (word_idx, word) in self.bits.iter().enumerate() {
            let mut bits = *word;
            while bits != 0 {
                let tz = bits.trailing_zeros() as usize;
                let idx = word_idx * 64 + tz;
                if idx >= self.len {
                    break;
                }
                result.push(idx);
                bits &= bits - 1;
            }
        }
        result
    }

    fn ensure_compatible(&mut self, other: &Bitmap) {
        if self.bits.len() < other.bits.len() {
            self.bits.resize(other.bits.len(), 0);
        }
        if other.bits.len() < self.bits.len() {
            // no-op; `and`/`or` will ignore trailing words
        }
    }

    fn bit_position(idx: usize) -> (usize, usize) {
        (idx / 64, idx % 64)
    }

    fn mask_unused_bits(&mut self) {
        if self.bits.is_empty() {
            return;
        }
        let remainder = self.len % 64;
        if remainder == 0 {
            return;
        }
        let mask = (1u64 << remainder) - 1;
        if let Some(last) = self.bits.last_mut() {
            *last &= mask;
        }
    }
}

#[derive(Clone, Debug)]
pub enum ColumnData {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Text(Vec<String>),
}

impl ColumnData {
    fn len(&self) -> usize {
        match self {
            ColumnData::Int64(values) => values.len(),
            ColumnData::Float64(values) => values.len(),
            ColumnData::Text(values) => values.len(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnarPage {
    pub page_metadata: String,
    pub data: ColumnData,
    pub null_bitmap: Bitmap,
    pub num_rows: usize,
}

impl ColumnarPage {
    pub fn empty() -> Self {
        Self {
            page_metadata: String::new(),
            data: ColumnData::Text(Vec::new()),
            null_bitmap: Bitmap::new(0),
            num_rows: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn from_disk_page(page: Page) -> Self {
        let num_rows = page.entries.len();
        let mut null_bitmap = Bitmap::new(num_rows);
        let mut raw_values: Vec<Option<String>> = Vec::with_capacity(num_rows);

        for (idx, entry) in page.entries.into_iter().enumerate() {
            let data = entry.get_data().to_string();
            if data.eq_ignore_ascii_case("null") || is_encoded_null(&data) {
                null_bitmap.set(idx);
                raw_values.push(None);
            } else {
                raw_values.push(Some(data));
            }
        }

        let data = infer_column_data(&raw_values);

        Self {
            page_metadata: page.page_metadata,
            data,
            null_bitmap,
            num_rows,
        }
    }

    pub fn as_disk_page(&self) -> Page {
        self.clone().into_disk_page()
    }

    pub fn into_disk_page(self) -> Page {
        let mut page = Page::new();
        page.page_metadata = self.page_metadata.clone();
        for idx in 0..self.num_rows {
            let value = if self.null_bitmap.is_set(idx) {
                encode_null()
            } else {
                match &self.data {
                    ColumnData::Int64(values) => values[idx].to_string(),
                    ColumnData::Float64(values) => format_float(values[idx]),
                    ColumnData::Text(values) => values[idx].clone(),
                }
            };
            page.add_entry(Entry::new(&value));
        }
        page
    }

    pub fn value_as_string(&self, idx: usize) -> Option<String> {
        if idx >= self.num_rows || self.null_bitmap.is_set(idx) {
            return None;
        }
        Some(match &self.data {
            ColumnData::Int64(values) => values[idx].to_string(),
            ColumnData::Float64(values) => format_float(values[idx]),
            ColumnData::Text(values) => values[idx].clone(),
        })
    }

    pub fn replace_with_disk_page(&mut self, page: Page) {
        *self = ColumnarPage::from_disk_page(page);
    }

    pub fn entry_at(&self, idx: usize) -> Option<Entry> {
        if idx >= self.num_rows {
            return None;
        }
        let value = self
            .value_as_string(idx)
            .unwrap_or_else(|| encode_null());
        Some(Entry::new(&value))
    }
}

fn infer_column_data(values: &[Option<String>]) -> ColumnData {
    if values.iter().all(|opt| opt.as_ref().map(|s| s.parse::<i64>().is_ok()).unwrap_or(true)) {
        return ColumnData::Int64(
            values
                .iter()
                .map(|opt| opt.as_ref().and_then(|s| s.parse::<i64>().ok()).unwrap_or_default())
                .collect(),
        );
    }

    if values.iter().all(|opt| opt.as_ref().map(|s| s.parse::<f64>().is_ok()).unwrap_or(true)) {
        return ColumnData::Float64(
            values
                .iter()
                .map(|opt| opt.as_ref().and_then(|s| s.parse::<f64>().ok()).unwrap_or_default())
                .collect(),
        );
    }

    ColumnData::Text(
        values
            .iter()
            .map(|opt| opt.clone().unwrap_or_default())
            .collect(),
    )
}
