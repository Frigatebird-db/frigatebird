//! SIMD-friendly comparison utilities for vectorized predicate evaluation.
//!
//! These functions are designed to auto-vectorize well on modern compilers.
//! They process data in chunks and use branchless patterns where possible.

use super::batch::Bitmap;

/// Process size for SIMD-friendly loops (8 i64s = 64 bytes = cache line)
const CHUNK_SIZE: usize = 8;

/// Compares i64 values against a target, producing a bitmap.
/// Uses branchless comparison and processes 8 values at a time for SIMD.
#[inline]
pub fn compare_i64_eq(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v == target)
}

#[inline]
pub fn compare_i64_ne(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v != target)
}

#[inline]
pub fn compare_i64_gt(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v > target)
}

#[inline]
pub fn compare_i64_ge(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v >= target)
}

#[inline]
pub fn compare_i64_lt(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v < target)
}

#[inline]
pub fn compare_i64_le(values: &[i64], target: i64, null_bitmap: &Bitmap) -> Bitmap {
    compare_i64_generic(values, null_bitmap, |v| v <= target)
}

/// Generic i64 comparison with a predicate function.
/// Optimized for auto-vectorization.
#[inline]
fn compare_i64_generic<F>(values: &[i64], null_bitmap: &Bitmap, pred: F) -> Bitmap
where
    F: Fn(i64) -> bool,
{
    let num_rows = values.len();
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();

    // Process 64 values per word (64 bits)
    for (word_idx, out_word) in out_words.iter_mut().enumerate() {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);

        // Process in chunks of 8 for better vectorization
        let full_chunks = count / CHUNK_SIZE;
        let mut result = 0u64;

        for chunk in 0..full_chunks {
            let chunk_base = base + chunk * CHUNK_SIZE;
            // Unrolled loop for 8 values - compiler can vectorize this
            let bits = compare_chunk_i64(&values[chunk_base..chunk_base + CHUNK_SIZE], &pred);
            result |= (bits as u64) << (chunk * CHUNK_SIZE);
        }

        // Handle remaining values
        let remainder_start = full_chunks * CHUNK_SIZE;
        for bit in remainder_start..count {
            let idx = base + bit;
            let cond = pred(values[idx]) as u64;
            result |= cond << bit;
        }

        // Apply null mask (branchless)
        *out_word = result & !null_word;
    }

    bitmap
}

/// Compare 8 i64 values, return 8 bits packed into a u8.
/// This is the hot inner loop - designed for SIMD auto-vectorization.
#[inline(always)]
fn compare_chunk_i64<F>(values: &[i64], pred: &F) -> u8
where
    F: Fn(i64) -> bool,
{
    // Explicit unroll helps compiler vectorize
    let b0 = pred(values[0]) as u8;
    let b1 = pred(values[1]) as u8;
    let b2 = pred(values[2]) as u8;
    let b3 = pred(values[3]) as u8;
    let b4 = pred(values[4]) as u8;
    let b5 = pred(values[5]) as u8;
    let b6 = pred(values[6]) as u8;
    let b7 = pred(values[7]) as u8;

    b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
}

/// Compares f64 values against a target, producing a bitmap.
#[inline]
pub fn compare_f64_eq(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| (v - target).abs() < f64::EPSILON)
}

#[inline]
pub fn compare_f64_ne(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| (v - target).abs() >= f64::EPSILON)
}

#[inline]
pub fn compare_f64_gt(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| v > target)
}

#[inline]
pub fn compare_f64_ge(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| v >= target)
}

#[inline]
pub fn compare_f64_lt(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| v < target)
}

#[inline]
pub fn compare_f64_le(values: &[f64], target: f64, null_bitmap: &Bitmap) -> Bitmap {
    compare_f64_generic(values, null_bitmap, |v| v <= target)
}

/// Generic f64 comparison with a predicate function.
#[inline]
fn compare_f64_generic<F>(values: &[f64], null_bitmap: &Bitmap, pred: F) -> Bitmap
where
    F: Fn(f64) -> bool,
{
    let num_rows = values.len();
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();

    for (word_idx, out_word) in out_words.iter_mut().enumerate() {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);

        let full_chunks = count / CHUNK_SIZE;
        let mut result = 0u64;

        for chunk in 0..full_chunks {
            let chunk_base = base + chunk * CHUNK_SIZE;
            let bits = compare_chunk_f64(&values[chunk_base..chunk_base + CHUNK_SIZE], &pred);
            result |= (bits as u64) << (chunk * CHUNK_SIZE);
        }

        let remainder_start = full_chunks * CHUNK_SIZE;
        for bit in remainder_start..count {
            let idx = base + bit;
            let cond = pred(values[idx]) as u64;
            result |= cond << bit;
        }

        *out_word = result & !null_word;
    }

    bitmap
}

#[inline(always)]
fn compare_chunk_f64<F>(values: &[f64], pred: &F) -> u8
where
    F: Fn(f64) -> bool,
{
    let b0 = pred(values[0]) as u8;
    let b1 = pred(values[1]) as u8;
    let b2 = pred(values[2]) as u8;
    let b3 = pred(values[3]) as u8;
    let b4 = pred(values[4]) as u8;
    let b5 = pred(values[5]) as u8;
    let b6 = pred(values[6]) as u8;
    let b7 = pred(values[7]) as u8;

    b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
}

/// Compares u16 dictionary keys against a target key.
/// Optimized for dictionary column filtering.
#[inline]
pub fn compare_u16_eq(values: &[u16], target: u16, null_bitmap: &Bitmap) -> Bitmap {
    let num_rows = values.len();
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();

    for (word_idx, out_word) in out_words.iter_mut().enumerate() {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);

        // Process 16 u16 values at a time (32 bytes)
        let full_chunks = count / 16;
        let mut result = 0u64;

        for chunk in 0..full_chunks {
            let chunk_base = base + chunk * 16;
            let bits = compare_chunk_u16_eq(&values[chunk_base..chunk_base + 16], target);
            result |= (bits as u64) << (chunk * 16);
        }

        let remainder_start = full_chunks * 16;
        for bit in remainder_start..count {
            let idx = base + bit;
            let cond = (values[idx] == target) as u64;
            result |= cond << bit;
        }

        *out_word = result & !null_word;
    }

    bitmap
}

#[inline(always)]
fn compare_chunk_u16_eq(values: &[u16], target: u16) -> u16 {
    let mut result = 0u16;
    // Unroll for 16 values
    result |= (values[0] == target) as u16;
    result |= ((values[1] == target) as u16) << 1;
    result |= ((values[2] == target) as u16) << 2;
    result |= ((values[3] == target) as u16) << 3;
    result |= ((values[4] == target) as u16) << 4;
    result |= ((values[5] == target) as u16) << 5;
    result |= ((values[6] == target) as u16) << 6;
    result |= ((values[7] == target) as u16) << 7;
    result |= ((values[8] == target) as u16) << 8;
    result |= ((values[9] == target) as u16) << 9;
    result |= ((values[10] == target) as u16) << 10;
    result |= ((values[11] == target) as u16) << 11;
    result |= ((values[12] == target) as u16) << 12;
    result |= ((values[13] == target) as u16) << 13;
    result |= ((values[14] == target) as u16) << 14;
    result |= ((values[15] == target) as u16) << 15;
    result
}

/// SIMD-friendly bitmap AND operation.
/// Processes 4 words at a time for better throughput.
#[inline]
pub fn bitmap_and_fast(dst: &mut Bitmap, src: &Bitmap) {
    let dst_words = dst.words_mut();
    let src_words = src.words();
    let len = dst_words.len().min(src_words.len());

    // Process 4 words at a time
    let chunks = len / 4;
    for i in 0..chunks {
        let base = i * 4;
        dst_words[base] &= src_words[base];
        dst_words[base + 1] &= src_words[base + 1];
        dst_words[base + 2] &= src_words[base + 2];
        dst_words[base + 3] &= src_words[base + 3];
    }

    // Handle remaining
    for i in (chunks * 4)..len {
        dst_words[i] &= src_words[i];
    }
}

/// SIMD-friendly bitmap OR operation.
#[inline]
pub fn bitmap_or_fast(dst: &mut Bitmap, src: &Bitmap) {
    let dst_words = dst.words_mut();
    let src_words = src.words();
    let len = dst_words.len().min(src_words.len());

    let chunks = len / 4;
    for i in 0..chunks {
        let base = i * 4;
        dst_words[base] |= src_words[base];
        dst_words[base + 1] |= src_words[base + 1];
        dst_words[base + 2] |= src_words[base + 2];
        dst_words[base + 3] |= src_words[base + 3];
    }

    for i in (chunks * 4)..len {
        dst_words[i] |= src_words[i];
    }
}

/// Evaluates dictionary numeric comparison using pre-built cache.
/// Much faster than per-row string parsing.
#[inline]
pub fn compare_dict_numeric(
    keys: &[u16],
    cache: &[Option<f64>],
    target: f64,
    null_bitmap: &Bitmap,
    num_rows: usize,
    cmp: fn(f64, f64) -> bool,
) -> Bitmap {
    let mut bitmap = Bitmap::new(num_rows);
    let out_words = bitmap.words_mut();
    let null_words = null_bitmap.words();

    for (word_idx, out_word) in out_words.iter_mut().enumerate() {
        let base = word_idx * 64;
        let remaining = num_rows.saturating_sub(base);
        let count = remaining.min(64);
        let null_word = *null_words.get(word_idx).unwrap_or(&0);

        let mut result = 0u64;
        for bit in 0..count {
            let idx = base + bit;
            let key = keys[idx] as usize;
            // Branchless: if cache lookup fails, comparison is false
            let cond = cache
                .get(key)
                .copied()
                .flatten()
                .map(|v| cmp(v, target))
                .unwrap_or(false) as u64;
            result |= cond << bit;
        }

        *out_word = result & !null_word;
    }

    bitmap
}
