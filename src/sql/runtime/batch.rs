use crate::entry::Entry;
use crate::page::Page;
use crate::sql::types::DataType;
use crate::sql::utils::{parse_bool, parse_datetime};

use super::values::{encode_null, format_float, format_timestamp_micros, is_encoded_null};
use std::collections::HashMap;

/// Bitmap used for null tracking and predicate evaluation.
#[derive(Clone, Debug)]
pub struct Bitmap {
    bits: Vec<u64>,
    len: usize,
}

impl Bitmap {
    pub fn new(len: usize) -> Self {
        let words = if len == 0 { 0 } else { len.div_ceil(64) };
        Self {
            bits: vec![0; words],
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn words(&self) -> &[u64] {
        &self.bits
    }

    pub(crate) fn words_mut(&mut self) -> &mut [u64] {
        &mut self.bits
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
            current_word: self.bits.first().copied().unwrap_or(0),
        }
    }

    pub fn extend_from(&mut self, other: &Bitmap) {
        if other.len == 0 {
            return;
        }
        let original_len = self.len;
        let new_len = original_len + other.len;
        let new_words = if new_len == 0 {
            0
        } else {
            new_len.div_ceil(64)
        };
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

/// Contiguous byte storage for variable-width string columns.
///
/// Uses Arrow-style layout: a single `data` buffer containing all string bytes
/// concatenated, plus an `offsets` array marking boundaries.
///
/// This layout eliminates per-string heap allocations and enables:
/// - Cache-friendly sequential scans
/// - Length-first filtering (compare integers before touching string bytes)
/// - Reduced allocator pressure during page loads
#[derive(Clone, Debug)]
pub struct BytesColumn {
    /// Contiguous blob of all string data (valid UTF-8)
    pub data: Vec<u8>,
    /// Offsets marking string boundaries. Row i spans data[offsets[i]..offsets[i+1]].
    /// Invariant: offsets.len() == num_rows + 1, offsets[0] == 0
    pub offsets: Vec<u32>,
}

impl BytesColumn {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: vec![0],
        }
    }

    pub fn with_capacity(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            data: Vec::with_capacity(bytes),
            offsets,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a string value.
    /// UTF-8 validity is guaranteed by the &str type.
    #[inline]
    pub fn push(&mut self, val: &str) {
        self.data.extend_from_slice(val.as_bytes());
        let new_end = u32::try_from(self.data.len()).expect("BytesColumn exceeded 4GB");
        self.offsets.push(new_end);
    }

    /// Returns the byte slice for row `idx`.
    ///
    /// # Safety
    /// Uses unchecked indexing in release builds for performance.
    /// Debug builds include bounds checking via debug_assert.
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> &[u8] {
        debug_assert!(
            idx + 1 < self.offsets.len(),
            "BytesColumn index out of bounds: {} >= {}",
            idx,
            self.len()
        );
        unsafe {
            let start = *self.offsets.get_unchecked(idx) as usize;
            let end = *self.offsets.get_unchecked(idx + 1) as usize;
            self.data.get_unchecked(start..end)
        }
    }

    /// Returns the length of string at row `idx` without accessing string data.
    /// This enables fast length-first filtering.
    #[inline]
    pub fn get_len(&self, idx: usize) -> usize {
        debug_assert!(idx + 1 < self.offsets.len());
        unsafe {
            let start = *self.offsets.get_unchecked(idx);
            let end = *self.offsets.get_unchecked(idx + 1);
            (end - start) as usize
        }
    }

    /// Returns the string at row `idx`. Allocates a new String.
    ///
    /// Use sparingly - prefer `get_bytes()` for comparisons.
    /// This exists for compatibility with legacy code paths (ordering, aggregates).
    #[inline]
    pub fn get_string(&self, idx: usize) -> String {
        let bytes = self.get_bytes(idx);
        // Safe because push() only accepts &str (valid UTF-8)
        unsafe { String::from_utf8_unchecked(bytes.to_vec()) }
    }

    /// Filters rows based on a selection bitmap.
    pub fn filter_by_bitmap(&self, bitmap: &Bitmap) -> Self {
        let selected_count = bitmap.count_ones();
        if selected_count == 0 {
            return Self::new();
        }

        // Estimate output size based on selection ratio
        let ratio = selected_count as f64 / self.len().max(1) as f64;
        let estimated_bytes = (self.data.len() as f64 * ratio) as usize;

        let mut result = Self::with_capacity(selected_count, estimated_bytes);

        for idx in bitmap.iter_ones() {
            if idx < self.len() {
                let bytes = self.get_bytes(idx);
                result.data.extend_from_slice(bytes);
                result.offsets.push(result.data.len() as u32);
            }
        }
        result
    }

    /// Gathers rows by indices (random access copy).
    /// Used for sorting - unavoidably loses locality benefits.
    pub fn gather(&self, indices: &[usize]) -> Self {
        if indices.is_empty() {
            return Self::new();
        }
        // Heuristic: assume 8 bytes average string length
        let mut result = Self::with_capacity(indices.len(), indices.len() * 8);
        for &idx in indices {
            debug_assert!(idx < self.len(), "gather index out of bounds");
            let bytes = self.get_bytes(idx);
            result.data.extend_from_slice(bytes);
            result.offsets.push(result.data.len() as u32);
        }
        result
    }

    /// Appends another BytesColumn to this one.
    pub fn append(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        let base_offset = self.data.len() as u32;
        self.data.extend_from_slice(&other.data);

        // Skip the leading 0 from other.offsets, adjust remaining by base_offset
        self.offsets.reserve(other.len());
        for &off in other.offsets.iter().skip(1) {
            self.offsets.push(base_offset + off);
        }
    }

    /// Returns a contiguous slice of rows.
    pub fn slice(&self, start: usize, end: usize) -> Self {
        let len = self.len();
        if start >= end || start >= len {
            return Self::new();
        }
        let end = end.min(len);
        let count = end - start;

        let byte_start = self.offsets[start] as usize;
        let byte_end = self.offsets[end] as usize;

        // Re-base offsets to start from 0
        let mut new_offsets = Vec::with_capacity(count + 1);
        new_offsets.push(0);
        let base = self.offsets[start];
        for i in (start + 1)..=end {
            new_offsets.push(self.offsets[i] - base);
        }

        Self {
            data: self.data[byte_start..byte_end].to_vec(),
            offsets: new_offsets,
        }
    }

    /// Creates a BytesColumn with the same value repeated.
    pub fn from_literal(value: &str, num_rows: usize) -> Self {
        if num_rows == 0 {
            return Self::new();
        }
        let val_bytes = value.as_bytes();
        let val_len = val_bytes.len();
        let total_bytes = val_len * num_rows;

        let mut data = Vec::with_capacity(total_bytes);
        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0);

        for i in 0..num_rows {
            data.extend_from_slice(val_bytes);
            offsets.push(((i + 1) * val_len) as u32);
        }

        Self { data, offsets }
    }
}

impl Default for BytesColumn {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over string slices in a BytesColumn
pub struct BytesColumnIter<'a> {
    col: &'a BytesColumn,
    idx: usize,
}

impl<'a> Iterator for BytesColumnIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.col.len() {
            return None;
        }
        let bytes = self.col.get_bytes(self.idx);
        self.idx += 1;
        Some(bytes)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.col.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for BytesColumnIter<'a> {}

impl BytesColumn {
    /// Returns an iterator over byte slices.
    pub fn iter(&self) -> BytesColumnIter<'_> {
        BytesColumnIter { col: self, idx: 0 }
    }

    /// Returns an iterator yielding (index, bytes) pairs.
    pub fn iter_enumerated(&self) -> impl Iterator<Item = (usize, &[u8])> {
        self.iter().enumerate()
    }
}

/// Dictionary-encoded string column for low-cardinality data.
///
/// Stores unique string values once in a dictionary, then references them
/// by integer keys. This provides:
/// - Massive memory savings for repetitive data (Region, Status, Category)
/// - Integer comparisons instead of memcmp for filtering
/// - SIMD-friendly scan patterns
///
/// Example: 1 billion rows of ["US", "EU", "AP"] uses ~1GB instead of ~2GB,
/// and filters via integer comparison instead of string comparison.
#[derive(Clone, Debug)]
pub struct DictionaryColumn {
    /// Integer keys referencing dictionary entries. Row i has value `values[keys[i]]`.
    pub keys: Vec<u16>,
    /// Unique string values (the dictionary).
    pub values: BytesColumn,
}

impl DictionaryColumn {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: BytesColumn::new(),
        }
    }

    pub fn with_capacity(rows: usize) -> Self {
        Self {
            keys: Vec::with_capacity(rows),
            values: BytesColumn::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[inline]
    pub fn cardinality(&self) -> usize {
        self.values.len()
    }

    /// Returns the byte slice for row `idx`.
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> &[u8] {
        let key = self.keys[idx] as usize;
        self.values.get_bytes(key)
    }

    /// Returns the string for row `idx`. Allocates.
    #[inline]
    pub fn get_string(&self, idx: usize) -> String {
        let key = self.keys[idx] as usize;
        self.values.get_string(key)
    }

    /// Looks up a value in the dictionary, returns its key if found.
    #[inline]
    pub fn find_key(&self, value: &[u8]) -> Option<u16> {
        for i in 0..self.values.len() {
            if self.values.get_bytes(i) == value {
                return Some(i as u16);
            }
        }
        None
    }

    /// Filters rows based on a selection bitmap.
    pub fn filter_by_bitmap(&self, bitmap: &Bitmap) -> Self {
        let selected_count = bitmap.count_ones();
        if selected_count == 0 {
            return Self::new();
        }

        let mut result = Self::with_capacity(selected_count);
        result.values = self.values.clone(); // Dictionary stays the same

        for idx in bitmap.iter_ones() {
            if idx < self.len() {
                result.keys.push(self.keys[idx]);
            }
        }
        result
    }

    /// Gathers rows by indices.
    pub fn gather(&self, indices: &[usize]) -> Self {
        if indices.is_empty() {
            return Self::new();
        }
        let mut result = Self::with_capacity(indices.len());
        result.values = self.values.clone(); // Dictionary stays the same
        for &idx in indices {
            result.keys.push(self.keys[idx]);
        }
        result
    }

    /// Appends another DictionaryColumn.
    /// Note: This merges dictionaries, which may be expensive for large dictionaries.
    pub fn append(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }

        // Build a mapping from other's keys to our keys
        let mut key_map: Vec<u16> = Vec::with_capacity(other.values.len());

        for i in 0..other.values.len() {
            let other_bytes = other.values.get_bytes(i);
            // Check if this value already exists in our dictionary
            if let Some(existing_key) = self.find_key(other_bytes) {
                key_map.push(existing_key);
            } else {
                // Add to our dictionary
                let new_key = self.values.len() as u16;
                self.values
                    .push(unsafe { std::str::from_utf8_unchecked(other_bytes) });
                key_map.push(new_key);
            }
        }

        // Remap and append other's keys
        for &other_key in &other.keys {
            self.keys.push(key_map[other_key as usize]);
        }
    }

    /// Returns a contiguous slice of rows.
    pub fn slice(&self, start: usize, end: usize) -> Self {
        let len = self.len();
        if start >= end || start >= len {
            return Self::new();
        }
        let end = end.min(len);

        let mut result = Self::with_capacity(end - start);
        result.values = self.values.clone(); // Dictionary stays the same
        result.keys = self.keys[start..end].to_vec();
        result
    }

    /// Converts this dictionary column to a BytesColumn (expands all values).
    pub fn to_bytes_column(&self) -> BytesColumn {
        let mut result = BytesColumn::with_capacity(self.len(), self.len() * 8);
        for &key in &self.keys {
            let bytes = self.values.get_bytes(key as usize);
            result.data.extend_from_slice(bytes);
            result.offsets.push(result.data.len() as u32);
        }
        result
    }

    /// Builds a HashMap for O(1) key lookup. Call once, reuse for multiple lookups.
    #[inline]
    pub fn build_key_lookup(&self) -> HashMap<Vec<u8>, u16> {
        let mut map = HashMap::with_capacity(self.values.len());
        for i in 0..self.values.len() {
            map.insert(self.values.get_bytes(i).to_vec(), i as u16);
        }
        map
    }

    /// Pre-parses all dictionary values as f64. Returns None for unparseable values.
    /// Call once per column, then use for fast numeric comparisons.
    #[inline]
    pub fn build_numeric_cache(&self) -> Vec<Option<f64>> {
        (0..self.values.len())
            .map(|i| {
                let bytes = self.values.get_bytes(i);
                // SAFETY: dictionary values are valid UTF-8
                let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                s.parse::<f64>().ok()
            })
            .collect()
    }

    /// Returns the numeric value for row `idx` using a pre-built cache.
    /// Much faster than get_string().parse() for repeated access.
    #[inline]
    pub fn get_cached_numeric(&self, idx: usize, cache: &[Option<f64>]) -> Option<f64> {
        let key = self.keys[idx] as usize;
        cache.get(key).copied().flatten()
    }
}

impl Default for DictionaryColumn {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum cardinality before falling back to BytesColumn.
/// u16::MAX = 65535 unique values.
pub const DICTIONARY_CARDINALITY_LIMIT: usize = 65535;

/// Cardinality ratio threshold: if unique values exceed this fraction of rows,
/// dictionary encoding is not beneficial.
pub const DICTIONARY_RATIO_THRESHOLD: f64 = 0.5;

/// Global flag to enable/disable dictionary encoding.
/// Currently disabled due to edge cases in null handling and empty strings.
pub const ENABLE_DICTIONARY_ENCODING: bool = false;

#[derive(Clone, Debug)]
pub enum ColumnData {
    Boolean(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Timestamp(Vec<i64>),
    Text(BytesColumn),
    Dictionary(DictionaryColumn),
}

impl ColumnData {
    fn len(&self) -> usize {
        match self {
            ColumnData::Boolean(v) => v.len(),
            ColumnData::Int64(values) => values.len(),
            ColumnData::Float64(values) => values.len(),
            ColumnData::Timestamp(values) => values.len(),
            ColumnData::Text(col) => col.len(),
            ColumnData::Dictionary(dict) => dict.len(),
        }
    }

    pub fn get_as_string(&self, idx: usize) -> String {
        match self {
            ColumnData::Boolean(v) => v[idx].to_string(),
            ColumnData::Int64(v) => v[idx].to_string(),
            ColumnData::Float64(v) => format_float(v[idx]),
            ColumnData::Timestamp(v) => {
                format_timestamp_micros(v[idx]).unwrap_or_else(|| v[idx].to_string())
            }
            ColumnData::Text(col) => col.get_string(idx),
            ColumnData::Dictionary(dict) => dict.get_string(idx),
        }
    }

    /// Returns true if this is a text-like column (Text or Dictionary).
    pub fn is_text_like(&self) -> bool {
        matches!(self, ColumnData::Text(_) | ColumnData::Dictionary(_))
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
    pub row_ids: Vec<u64>,
}

impl ColumnarBatch {
    fn assert_row_ids_invariant(&self) {
        debug_assert!(
            self.row_ids.is_empty() || self.row_ids.len() == self.num_rows,
            "row_ids must be empty or match num_rows"
        );
    }

    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            num_rows: 0,
            aliases: HashMap::new(),
            row_ids: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: HashMap::with_capacity(capacity),
            num_rows: 0,
            aliases: HashMap::new(),
            row_ids: Vec::new(),
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
        let row_ids = if self.row_ids.is_empty() {
            indices.iter().map(|&idx| idx as u64).collect()
        } else {
            indices
                .iter()
                .filter_map(|&idx| self.row_ids.get(idx).copied())
                .collect()
        };
        let batch = ColumnarBatch {
            columns,
            num_rows: indices.len(),
            aliases: self.aliases.clone(),
            row_ids,
        };
        batch.assert_row_ids_invariant();
        batch
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
        let row_ids = if self.row_ids.is_empty() {
            (start as u64..end as u64).collect()
        } else {
            self.row_ids[start..end].to_vec()
        };
        let batch = ColumnarBatch {
            columns,
            num_rows: end - start,
            aliases: self.aliases.clone(),
            row_ids,
        };
        batch.assert_row_ids_invariant();
        batch
    }

    pub fn append(&mut self, other: &Self) {
        if other.num_rows == 0 {
            return;
        }
        if self.columns.is_empty() {
            *self = other.clone();
            self.assert_row_ids_invariant();
            return;
        }
        for (ordinal, page) in &other.columns {
            self.columns
                .entry(*ordinal)
                .and_modify(|existing| existing.append(page))
                .or_insert_with(|| page.clone());
        }
        self.num_rows += other.num_rows;
        if !self.row_ids.is_empty() || !other.row_ids.is_empty() {
            if self.row_ids.is_empty() {
                self.row_ids = (0..self.num_rows as u64 - other.num_rows as u64).collect();
            }
            if other.row_ids.is_empty() {
                let start = self.row_ids.len() as u64;
                self.row_ids.extend(start..start + other.num_rows as u64);
            } else {
                self.row_ids.extend_from_slice(&other.row_ids);
            }
        }
        self.assert_row_ids_invariant();
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
        let row_ids = if self.row_ids.is_empty() {
            bitmap
                .iter_ones()
                .take_while(|&idx| idx < self.num_rows)
                .map(|idx| idx as u64)
                .collect()
        } else {
            bitmap
                .iter_ones()
                .take_while(|&idx| idx < self.num_rows)
                .filter_map(|idx| self.row_ids.get(idx).copied())
                .collect()
        };
        let batch = ColumnarBatch {
            columns,
            num_rows: bitmap.count_ones(),
            aliases: self.aliases.clone(),
            row_ids,
        };
        batch.assert_row_ids_invariant();
        batch
    }
}

impl ColumnarPage {
    pub fn empty() -> Self {
        Self {
            page_metadata: String::new(),
            data: ColumnData::Text(BytesColumn::new()),
            null_bitmap: Bitmap::new(0),
            num_rows: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn load(page: Page, dtype: DataType) -> Self {
        let num_rows = page.entries.len();
        let mut null_bitmap = Bitmap::new(num_rows);

        let data = match dtype {
            DataType::Boolean => {
                let mut vals = Vec::with_capacity(num_rows);
                for (i, e) in page.entries.iter().enumerate() {
                    let s = e.get_data();
                    if is_null_string_for_type(s, DataType::Boolean) {
                        null_bitmap.set(i);
                        vals.push(false);
                    } else {
                        vals.push(parse_bool(s).unwrap_or(false));
                    }
                }
                ColumnData::Boolean(vals)
            }
            DataType::Int64 => {
                let mut vals = Vec::with_capacity(num_rows);
                for (i, e) in page.entries.iter().enumerate() {
                    let s = e.get_data();
                    if is_null_string_for_type(s, DataType::Int64) {
                        null_bitmap.set(i);
                        vals.push(0);
                    } else {
                        vals.push(s.parse::<i64>().unwrap_or(0));
                    }
                }
                ColumnData::Int64(vals)
            }
            DataType::Float64 => {
                let mut vals = Vec::with_capacity(num_rows);
                for (i, e) in page.entries.iter().enumerate() {
                    let s = e.get_data();
                    if is_null_string_for_type(s, DataType::Float64) {
                        null_bitmap.set(i);
                        vals.push(0.0);
                    } else {
                        vals.push(s.parse::<f64>().unwrap_or(0.0));
                    }
                }
                ColumnData::Float64(vals)
            }
            DataType::Timestamp => {
                let mut vals = Vec::with_capacity(num_rows);
                for (i, e) in page.entries.iter().enumerate() {
                    let s = e.get_data();
                    if is_null_string_for_type(s, DataType::Timestamp) {
                        null_bitmap.set(i);
                        vals.push(0);
                    } else {
                        vals.push(parse_datetime(s).unwrap_or(0));
                    }
                }
                ColumnData::Timestamp(vals)
            }
            DataType::String | DataType::Uuid | DataType::IpAddr => {
                // Smart cardinality detection: try dictionary encoding first
                use std::collections::HashMap;

                let mut unique_map: HashMap<&str, u16> = HashMap::new();
                let mut dict_values = BytesColumn::new();
                let mut keys = Vec::with_capacity(num_rows);
                let mut use_dictionary = ENABLE_DICTIONARY_ENCODING;
                let cardinality_threshold = (num_rows as f64 * DICTIONARY_RATIO_THRESHOLD) as usize;

                // First pass: try to build dictionary
                for (i, e) in page.entries.iter().enumerate() {
                    let s = e.get_data();

                    if is_null_string_for_type(s, DataType::String) {
                        null_bitmap.set(i);
                        // For nulls, we still need a valid key - use key 0 pointing to empty string
                        if unique_map.is_empty() {
                            unique_map.insert("", 0);
                            dict_values.push("");
                        }
                        keys.push(*unique_map.get("").unwrap_or(&0));
                    } else if use_dictionary {
                        if let Some(&key) = unique_map.get(s) {
                            keys.push(key);
                        } else {
                            let new_key = unique_map.len() as u16;
                            // Check limits: cardinality exceeds u16 max OR exceeds ratio threshold
                            if new_key as usize >= DICTIONARY_CARDINALITY_LIMIT
                                || unique_map.len() >= cardinality_threshold
                            {
                                // Abandon dictionary encoding - high cardinality detected
                                use_dictionary = false;
                            } else {
                                unique_map.insert(s, new_key);
                                dict_values.push(s);
                                keys.push(new_key);
                            }
                        }
                    }

                    if !use_dictionary {
                        break;
                    }
                }

                if use_dictionary && num_rows > 0 {
                    // Dictionary encoding is beneficial
                    ColumnData::Dictionary(DictionaryColumn {
                        keys,
                        values: dict_values,
                    })
                } else {
                    // Fall back to BytesColumn for high cardinality
                    let mut col = BytesColumn::with_capacity(num_rows, num_rows * 16);
                    for (i, e) in page.entries.iter().enumerate() {
                        let s = e.get_data();
                        if is_null_string_for_type(s, DataType::String) {
                            null_bitmap.set(i);
                            col.push("");
                        } else {
                            col.push(s);
                        }
                    }
                    ColumnData::Text(col)
                }
            }
            DataType::Null => {
                null_bitmap.fill(true);
                ColumnData::Text(BytesColumn::from_literal("", num_rows))
            }
        };

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
                    ColumnData::Boolean(v) => v[idx].to_string(),
                    ColumnData::Int64(values) => values[idx].to_string(),
                    ColumnData::Float64(values) => format_float(values[idx]),
                    ColumnData::Timestamp(v) => {
                        format_timestamp_micros(v[idx]).unwrap_or_else(|| v[idx].to_string())
                    }
                    ColumnData::Text(col) => col.get_string(idx),
                    ColumnData::Dictionary(dict) => dict.get_string(idx),
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
        Some(self.data.get_as_string(idx))
    }

    pub fn replace_with_disk_page(&mut self, page: Page, dtype: DataType) {
        *self = ColumnarPage::load(page, dtype);
    }

    pub fn entry_at(&self, idx: usize) -> Option<Entry> {
        if idx >= self.num_rows {
            return None;
        }
        let value = self.value_as_string(idx).unwrap_or_else(encode_null);
        Some(Entry::new(&value))
    }

    pub fn get_value_as_string(&self, idx: usize) -> Option<String> {
        self.value_as_string(idx)
    }

    pub fn empty_like(&self) -> Self {
        let data = match &self.data {
            ColumnData::Boolean(_) => ColumnData::Boolean(Vec::new()),
            ColumnData::Int64(_) => ColumnData::Int64(Vec::new()),
            ColumnData::Float64(_) => ColumnData::Float64(Vec::new()),
            ColumnData::Timestamp(_) => ColumnData::Timestamp(Vec::new()),
            ColumnData::Text(_) => ColumnData::Text(BytesColumn::new()),
            ColumnData::Dictionary(_) => ColumnData::Dictionary(DictionaryColumn::new()),
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
            ColumnData::Boolean(values) => {
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
                ColumnData::Boolean(filtered)
            }
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
            ColumnData::Timestamp(values) => {
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
                ColumnData::Timestamp(filtered)
            }
            ColumnData::Text(col) => {
                // Use BytesColumn's optimized filter
                let filtered = col.filter_by_bitmap(bitmap);
                // Copy null bitmap for selected rows
                for (out_idx, row_idx) in bitmap
                    .iter_ones()
                    .take_while(|&idx| idx < self.num_rows)
                    .enumerate()
                {
                    if self.null_bitmap.is_set(row_idx) {
                        new_null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(filtered)
            }
            ColumnData::Dictionary(dict) => {
                // Use DictionaryColumn's optimized filter (just filters keys)
                let filtered = dict.filter_by_bitmap(bitmap);
                for (out_idx, row_idx) in bitmap
                    .iter_ones()
                    .take_while(|&idx| idx < self.num_rows)
                    .enumerate()
                {
                    if self.null_bitmap.is_set(row_idx) {
                        new_null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Dictionary(filtered)
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
            (ColumnData::Boolean(lhs), ColumnData::Boolean(rhs)) => lhs.extend(rhs.iter().copied()),
            (ColumnData::Int64(lhs), ColumnData::Int64(rhs)) => lhs.extend(rhs.iter().copied()),
            (ColumnData::Float64(lhs), ColumnData::Float64(rhs)) => lhs.extend(rhs.iter().copied()),
            (ColumnData::Timestamp(lhs), ColumnData::Timestamp(rhs)) => {
                lhs.extend(rhs.iter().copied())
            }
            (ColumnData::Text(lhs), ColumnData::Text(rhs)) => lhs.append(rhs),
            (ColumnData::Dictionary(lhs), ColumnData::Dictionary(rhs)) => lhs.append(rhs),
            // Cross-type append between Text and Dictionary: convert to common format
            (ColumnData::Text(lhs), ColumnData::Dictionary(rhs)) => {
                lhs.append(&rhs.to_bytes_column());
            }
            (ColumnData::Dictionary(lhs), ColumnData::Text(rhs)) => {
                // Convert self to Text, then append
                let mut text_col = lhs.to_bytes_column();
                text_col.append(rhs);
                *lhs = DictionaryColumn::new(); // Will be replaced
                // This is a type change - we need to handle it specially
                // For now, panic as this shouldn't happen with same-schema appends
                panic!("Cannot append Text to Dictionary - schema mismatch");
            }
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
            ColumnData::Boolean(values) => {
                let mut collected = Vec::with_capacity(indices.len());
                for (out_idx, &idx) in indices.iter().enumerate() {
                    collected.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Boolean(collected)
            }
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
            ColumnData::Timestamp(values) => {
                let mut collected = Vec::with_capacity(indices.len());
                for (out_idx, &idx) in indices.iter().enumerate() {
                    collected.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Timestamp(collected)
            }
            ColumnData::Text(col) => {
                let gathered = col.gather(indices);
                for (out_idx, &idx) in indices.iter().enumerate() {
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(gathered)
            }
            ColumnData::Dictionary(dict) => {
                let gathered = dict.gather(indices);
                for (out_idx, &idx) in indices.iter().enumerate() {
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Dictionary(gathered)
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
            ColumnData::Boolean(values) => {
                let mut sliced = Vec::with_capacity(len);
                for (out_idx, idx) in (start..end).enumerate() {
                    sliced.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Boolean(sliced)
            }
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
            ColumnData::Timestamp(values) => {
                let mut sliced = Vec::with_capacity(len);
                for (out_idx, idx) in (start..end).enumerate() {
                    sliced.push(values[idx]);
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Timestamp(sliced)
            }
            ColumnData::Text(col) => {
                let sliced = col.slice(start, end);
                for (out_idx, idx) in (start..end).enumerate() {
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Text(sliced)
            }
            ColumnData::Dictionary(dict) => {
                let sliced = dict.slice(start, end);
                for (out_idx, idx) in (start..end).enumerate() {
                    if self.null_bitmap.is_set(idx) {
                        null_bitmap.set(out_idx);
                    }
                }
                ColumnData::Dictionary(sliced)
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
            data: ColumnData::Text(BytesColumn::from_literal(value, num_rows)),
            null_bitmap: Bitmap::new(num_rows),
            num_rows,
        }
    }

    pub fn from_literal_bool(value: bool, num_rows: usize) -> Self {
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Boolean(vec![value; num_rows]),
            null_bitmap: Bitmap::new(num_rows),
            num_rows,
        }
    }

    pub fn from_nulls(num_rows: usize) -> Self {
        let mut bitmap = Bitmap::new(num_rows);
        bitmap.fill(true);
        ColumnarPage {
            page_metadata: String::new(),
            data: ColumnData::Text(BytesColumn::from_literal("", num_rows)),
            null_bitmap: bitmap,
            num_rows,
        }
    }
}

fn is_null_string_for_type(s: &str, dtype: DataType) -> bool {
    match dtype {
        DataType::String => s.eq_ignore_ascii_case("null") || is_encoded_null(s),
        _ => s.is_empty() || s.eq_ignore_ascii_case("null") || is_encoded_null(s),
    }
}
