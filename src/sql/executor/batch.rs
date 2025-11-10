use crate::entry::Entry;
use crate::page::Page;

use super::values::{encode_null, format_float, is_encoded_null};
use std::collections::HashMap;

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
        self.bits
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
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
        self.iter_ones().collect()
    }

    pub fn iter_ones(&self) -> BitmapOnesIter<'_> {
        BitmapOnesIter {
            bits: &self.bits,
            len: self.len,
            word_idx: 0,
            current_word: self.bits.get(0).copied().unwrap_or(0),
        }
    }

    pub fn extend_from(&mut self, other: &Bitmap) {
        if other.len == 0 {
            return;
        }
        let original_len = self.len;
        let new_len = original_len + other.len;
        let new_words = if new_len == 0 { 0 } else { (new_len + 63) / 64 };
        if self.bits.len() < new_words {
            self.bits.resize(new_words, 0);
        }
        self.len = new_len;
        for idx in 0..other.len {
            if other.is_set(idx) {
                self.set(original_len + idx);
            }
        }
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

pub struct BitmapOnesIter<'a> {
    bits: &'a [u64],
    len: usize,
    word_idx: usize,
    current_word: u64,
}

impl<'a> Iterator for BitmapOnesIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.word_idx < self.bits.len() {
            if self.current_word == 0 {
                self.word_idx += 1;
                if self.word_idx < self.bits.len() {
                    self.current_word = self.bits[self.word_idx];
                }
                continue;
            }
            let tz = self.current_word.trailing_zeros() as usize;
            let idx = self.word_idx * 64 + tz;
            self.current_word &= self.current_word - 1;
            if idx >= self.len {
                return None;
            }
            return Some(idx);
        }
        None
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

#[derive(Clone, Debug, Default)]
pub struct ColumnarBatch {
    pub columns: HashMap<usize, ColumnarPage>,
    pub num_rows: usize,
    pub aliases: HashMap<String, usize>,
}

impl ColumnarBatch {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            num_rows: 0,
            aliases: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: HashMap::with_capacity(capacity),
            num_rows: 0,
            aliases: HashMap::new(),
        }
    }

    pub fn gather(&self, indices: &[usize]) -> Self {
        if indices.is_empty() {
            return ColumnarBatch::new();
        }
        let mut columns = HashMap::with_capacity(self.columns.len());
        for (ordinal, page) in &self.columns {
            columns.insert(*ordinal, page.gather(indices));
        }
        ColumnarBatch {
            columns,
            num_rows: indices.len(),
            aliases: self.aliases.clone(),
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        if start >= end || start >= self.num_rows {
            return ColumnarBatch::new();
        }
        let end = end.min(self.num_rows);
        let mut columns = HashMap::with_capacity(self.columns.len());
        for (ordinal, page) in &self.columns {
            columns.insert(*ordinal, page.slice(start, end));
        }
        ColumnarBatch {
            columns,
            num_rows: end - start,
            aliases: self.aliases.clone(),
        }
    }

    pub fn append(&mut self, other: &Self) {
        if other.num_rows == 0 {
            return;
        }
        if self.columns.is_empty() {
            *self = other.clone();
            return;
        }
        for (ordinal, page) in &other.columns {
            self.columns
                .entry(*ordinal)
                .and_modify(|existing| existing.append(page))
                .or_insert_with(|| page.clone());
        }
        self.num_rows += other.num_rows;
        if self.aliases != other.aliases {
            for (alias, ordinal) in &other.aliases {
                self.aliases.insert(alias.clone(), *ordinal);
            }
        }
    }

    pub fn filter_by_bitmap(&self, bitmap: &Bitmap) -> Self {
        if bitmap.count_ones() == self.num_rows {
            return self.clone();
        }
        let mut columns = HashMap::with_capacity(self.columns.len());
        for (ordinal, page) in &self.columns {
            columns.insert(*ordinal, page.filter_by_bitmap(bitmap));
        }
        ColumnarBatch {
            columns,
            num_rows: bitmap.count_ones(),
            aliases: self.aliases.clone(),
        }
    }
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
        let value = self.value_as_string(idx).unwrap_or_else(|| encode_null());
        Some(Entry::new(&value))
    }

    pub fn get_value_as_string(&self, idx: usize) -> Option<String> {
        self.value_as_string(idx)
    }

    pub fn empty_like(&self) -> Self {
        let data = match &self.data {
            ColumnData::Int64(_) => ColumnData::Int64(Vec::new()),
            ColumnData::Float64(_) => ColumnData::Float64(Vec::new()),
            ColumnData::Text(_) => ColumnData::Text(Vec::new()),
        };
        ColumnarPage {
            page_metadata: String::new(),
            data,
            null_bitmap: Bitmap::new(0),
            num_rows: 0,
        }
    }

    pub fn filter_by_bitmap(&self, bitmap: &Bitmap) -> Self {
        if self.num_rows == 0 {
            return self.empty_like();
        }

        let selected_len = bitmap
            .iter_ones()
            .take_while(|&idx| idx < self.num_rows)
            .count();
        if selected_len == 0 {
            return self.empty_like();
        }

        let mut new_null_bitmap = Bitmap::new(selected_len);
        let data = match &self.data {
            ColumnData::Int64(values) => {
                let mut filtered = Vec::with_capacity(selected_len);
                for (out_idx, row_idx) in bitmap
                    .iter_ones()
                    .take_while(|&idx| idx < self.num_rows)
                    .enumerate()
                {
                    filtered.push(values[row_idx]);
                    if self.null_bitmap.is_set(row_idx) {
                        new_null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Int64(filtered)
            }
            ColumnData::Float64(values) => {
                let mut filtered = Vec::with_capacity(selected_len);
                for (out_idx, row_idx) in bitmap
                    .iter_ones()
                    .take_while(|&idx| idx < self.num_rows)
                    .enumerate()
                {
                    filtered.push(values[row_idx]);
                    if self.null_bitmap.is_set(row_idx) {
                        new_null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Float64(filtered)
            }
            ColumnData::Text(values) => {
                let mut filtered = Vec::with_capacity(selected_len);
                for (out_idx, row_idx) in bitmap
                    .iter_ones()
                    .take_while(|&idx| idx < self.num_rows)
                    .enumerate()
                {
                    filtered.push(values[row_idx].clone());
                    if self.null_bitmap.is_set(row_idx) {
                        new_null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(filtered)
            }
        };

        ColumnarPage {
            page_metadata: self.page_metadata.clone(),
            data,
            null_bitmap: new_null_bitmap,
            num_rows: selected_len,
        }
    }

    pub fn append(&mut self, other: &Self) {
        if other.num_rows == 0 {
            return;
        }
        match (&mut self.data, &other.data) {
            (ColumnData::Int64(lhs), ColumnData::Int64(rhs)) => lhs.extend(rhs.iter().copied()),
            (ColumnData::Float64(lhs), ColumnData::Float64(rhs)) => lhs.extend(rhs.iter().copied()),
            (ColumnData::Text(lhs), ColumnData::Text(rhs)) => lhs.extend(rhs.iter().cloned()),
            _ => panic!("attempted to append mismatched column types"),
        }
        self.null_bitmap.extend_from(&other.null_bitmap);
        self.num_rows += other.num_rows;
    }

    pub fn gather(&self, indices: &[usize]) -> Self {
        if indices.is_empty() {
            return self.empty_like();
        }
        let mut null_bitmap = Bitmap::new(indices.len());
        let data = match &self.data {
            ColumnData::Int64(values) => {
                let mut collected = Vec::with_capacity(indices.len());
                for (out_idx, &idx) in indices.iter().enumerate() {
                    collected.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Int64(collected)
            }
            ColumnData::Float64(values) => {
                let mut collected = Vec::with_capacity(indices.len());
                for (out_idx, &idx) in indices.iter().enumerate() {
                    collected.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Float64(collected)
            }
            ColumnData::Text(values) => {
                let mut collected = Vec::with_capacity(indices.len());
                for (out_idx, &idx) in indices.iter().enumerate() {
                    collected.push(values[idx].clone());
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(collected)
            }
        };

        ColumnarPage {
            page_metadata: self.page_metadata.clone(),
            data,
            null_bitmap,
            num_rows: indices.len(),
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        if start >= end || start >= self.num_rows {
            return self.empty_like();
        }
        let end = end.min(self.num_rows);
        let len = end - start;
        let mut null_bitmap = Bitmap::new(len);
        let data = match &self.data {
            ColumnData::Int64(values) => {
                let mut sliced = Vec::with_capacity(len);
                for (out_idx, idx) in (start..end).enumerate() {
                    sliced.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Int64(sliced)
            }
            ColumnData::Float64(values) => {
                let mut sliced = Vec::with_capacity(len);
                for (out_idx, idx) in (start..end).enumerate() {
                    sliced.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Float64(sliced)
            }
            ColumnData::Text(values) => {
                let mut sliced = Vec::with_capacity(len);
                for (out_idx, idx) in (start..end).enumerate() {
                    sliced.push(values[idx].clone());
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(sliced)
            }
        };
        ColumnarPage {
            page_metadata: self.page_metadata.clone(),
            data,
            null_bitmap,
            num_rows: len,
        }
    }

    pub fn from_literal_f64(value: f64, num_rows: usize) -> Self {
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Float64(vec![value; num_rows]),
            null_bitmap: Bitmap::new(num_rows),
            num_rows,
        }
    }

    pub fn from_literal_i64(value: i64, num_rows: usize) -> Self {
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Int64(vec![value; num_rows]),
            null_bitmap: Bitmap::new(num_rows),
            num_rows,
        }
    }

    pub fn from_literal_text(value: &str, num_rows: usize) -> Self {
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Text(vec![value.to_string(); num_rows]),
            null_bitmap: Bitmap::new(num_rows),
            num_rows,
        }
    }

    pub fn from_literal_bool(value: bool, num_rows: usize) -> Self {
        let literal = if value { "true" } else { "false" };
        Self::from_literal_text(literal, num_rows)
    }

    pub fn from_nulls(num_rows: usize) -> Self {
        let mut bitmap = Bitmap::new(num_rows);
        for idx in 0..num_rows {
            bitmap.set(idx);
        }
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Text(vec![String::new(); num_rows]),
            null_bitmap: bitmap,
            num_rows,
        }
    }
}

fn infer_column_data(values: &[Option<String>]) -> ColumnData {
    if values.iter().all(|opt| {
        opt.as_ref()
            .map(|s| s.parse::<i64>().is_ok())
            .unwrap_or(true)
    }) {
        return ColumnData::Int64(
            values
                .iter()
                .map(|opt| {
                    opt.as_ref()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or_default()
                })
                .collect(),
        );
    }

    if values.iter().all(|opt| {
        opt.as_ref()
            .map(|s| s.parse::<f64>().is_ok())
            .unwrap_or(true)
    }) {
        return ColumnData::Float64(
            values
                .iter()
                .map(|opt| {
                    opt.as_ref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or_default()
                })
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
