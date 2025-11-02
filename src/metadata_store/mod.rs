use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Descriptor for the physical location of a column page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageDescriptor {
    pub id: String,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
}

impl PageDescriptor {
    fn new(
        id: String,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> Self {
        PageDescriptor {
            id,
            disk_path,
            offset,
            alloc_len,
            actual_len,
            entry_count,
        }
    }
}

#[derive(Clone, Debug)]
struct ColumnChain {
    pages: Vec<PageDescriptor>,
    prefix_entries: Vec<u64>,
}

impl ColumnChain {
    fn new() -> Self {
        ColumnChain {
            pages: Vec::with_capacity(8),
            prefix_entries: Vec::with_capacity(8),
        }
    }

    fn push(&mut self, descriptor: PageDescriptor) {
        let prev = self.prefix_entries.last().copied().unwrap_or(0);
        let next = prev + descriptor.entry_count;
        self.pages.push(descriptor);
        self.prefix_entries.push(next);
    }

    fn replace_last(&mut self, descriptor: PageDescriptor) {
        if self.pages.is_empty() {
            self.push(descriptor);
            return;
        }

        let old_count = self.pages.last().map(|p| p.entry_count).unwrap_or(0);
        let delta = descriptor.entry_count as i64 - old_count as i64;

        if let Some(last_page) = self.pages.last_mut() {
            *last_page = descriptor;
        }

        if delta != 0 {
            let len = self.prefix_entries.len();
            apply_delta_suffix(&mut self.prefix_entries[len - 1..], delta);
        }

        let new_entry_count = self.pages.last().map(|p| p.entry_count).unwrap_or(0);
        let base = if self.prefix_entries.len() >= 2 {
            self.prefix_entries[self.prefix_entries.len() - 2]
        } else {
            0
        };

        if let Some(last_prefix) = self.prefix_entries.last_mut() {
            *last_prefix = base + new_entry_count;
        }
    }

    fn total_entries(&self) -> u64 {
        self.prefix_entries.last().copied().unwrap_or(0)
    }

    fn find_page_index(&self, row: u64) -> Option<usize> {
        let total = self.total_entries();
        if row >= total {
            return None;
        }
        let mut lo = 0;
        let mut hi = self.prefix_entries.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.prefix_entries[mid] <= row {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo)
    }

    fn last(&self) -> Option<PageDescriptor> {
        self.pages.last().cloned()
    }

    fn all_pages(&self) -> Vec<PageDescriptor> {
        self.pages.clone()
    }

    fn locate_row(&self, row: u64) -> Option<RowLocation> {
        let page_index = self.find_page_index(row)?;
        let page = self.pages[page_index].clone();
        let previous_prefix = if page_index == 0 {
            0
        } else {
            self.prefix_entries[page_index - 1]
        };
        let page_row_index = row.saturating_sub(previous_prefix);
        Some(RowLocation {
            descriptor: page,
            page_row_index,
        })
    }

    fn locate_range(&self, start_row: u64, end_row: u64) -> Vec<PageSlice> {
        if self.pages.is_empty() || start_row > end_row {
            return Vec::new();
        }

        let total = self.total_entries();
        if total == 0 || start_row >= total {
            return Vec::new();
        }

        let clamped_end = end_row.min(total.saturating_sub(1));
        let start_idx = match self.find_page_index(start_row) {
            Some(idx) => idx,
            None => return Vec::new(),
        };
        let end_idx = match self.find_page_index(clamped_end) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let mut slices = Vec::with_capacity(end_idx - start_idx + 1);
        for idx in start_idx..=end_idx {
            let prev = if idx == 0 {
                0
            } else {
                self.prefix_entries[idx - 1]
            };
            let page_start = if idx == start_idx {
                start_row.saturating_sub(prev)
            } else {
                0
            };
            let page_end_exclusive = if idx == end_idx {
                (clamped_end.saturating_sub(prev)) + 1
            } else {
                self.pages[idx].entry_count
            };
            slices.push(PageSlice {
                descriptor: self.pages[idx].clone(),
                start_row_offset: page_start,
                end_row_offset: page_end_exclusive.min(self.pages[idx].entry_count),
            });
        }

        slices
    }
}

/// Describes where a logical row falls within a page.
#[derive(Clone, Debug)]
pub struct RowLocation {
    pub descriptor: PageDescriptor,
    pub page_row_index: u64,
}

/// Represents a trimmed slice of a page participating in a range scan.
#[derive(Clone, Debug)]
pub struct PageSlice {
    pub descriptor: PageDescriptor,
    pub start_row_offset: u64,
    pub end_row_offset: u64,
}

/// Batch append description used to publish new page versions.
#[derive(Clone, Debug)]
pub struct PendingPage {
    pub column: String,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
    pub replace_last: bool,
}

/// In-memory catalog mapping columns to their latest page chain.
pub struct TableMetaStore {
    columns: HashMap<String, ColumnChain>,
    page_index: HashMap<String, PageDescriptor>,
    next_page_id: u64,
}

impl TableMetaStore {
    pub fn new() -> Self {
        TableMetaStore {
            columns: HashMap::new(),
            page_index: HashMap::new(),
            next_page_id: 1,
        }
    }

    fn allocate_descriptor(
        &mut self,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> PageDescriptor {
        let id = format!("{:016x}", self.next_page_id);
        self.next_page_id = self.next_page_id.wrapping_add(1);
        PageDescriptor::new(id, disk_path, offset, alloc_len, actual_len, entry_count)
    }

    pub fn latest(&self, column: &str) -> Option<PageDescriptor> {
        self.columns.get(column).and_then(|chain| chain.last())
    }

    pub fn pages_for_column(&self, column: &str) -> Vec<PageDescriptor> {
        self.columns
            .get(column)
            .map(|chain| chain.all_pages())
            .unwrap_or_default()
    }

    pub fn lookup(&self, id: &str) -> Option<PageDescriptor> {
        self.page_index.get(id).cloned()
    }

    pub fn locate_row(&self, column: &str, row: u64) -> Option<RowLocation> {
        self.columns
            .get(column)
            .and_then(|chain| chain.locate_row(row))
    }

    pub fn locate_range(&self, column: &str, start_row: u64, end_row: u64) -> Vec<PageSlice> {
        self.columns
            .get(column)
            .map(|chain| chain.locate_range(start_row, end_row))
            .unwrap_or_default()
    }

    pub fn register_page(
        &mut self,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> PageDescriptor {
        let count = if entry_count == 0 { 1 } else { entry_count };
        let descriptor = self.allocate_descriptor(disk_path, offset, alloc_len, actual_len, count);
        self.page_index
            .insert(descriptor.id.clone(), descriptor.clone());
        self.columns
            .entry(column.to_string())
            .or_insert_with(ColumnChain::new)
            .push(descriptor.clone());
        descriptor
    }

    pub fn register_batch(&mut self, pages: &[PendingPage]) -> Vec<PageDescriptor> {
        let mut registered = Vec::with_capacity(pages.len());
        for page in pages {
            let descriptor = self.allocate_descriptor(
                page.disk_path.clone(),
                page.offset,
                page.alloc_len,
                page.actual_len,
                page.entry_count,
            );

            let mut old_id: Option<String> = None;
            {
                let chain = self
                    .columns
                    .entry(page.column.clone())
                    .or_insert_with(ColumnChain::new);

                if page.replace_last {
                    old_id = chain.last().map(|p| p.id.clone());
                    if old_id.is_some() {
                        chain.replace_last(descriptor.clone());
                    } else {
                        chain.push(descriptor.clone());
                    }
                } else {
                    chain.push(descriptor.clone());
                }
            }

            if let Some(old) = old_id {
                self.page_index.remove(&old);
            }

            self.page_index
                .insert(descriptor.id.clone(), descriptor.clone());

            registered.push(descriptor);
        }
        registered
    }
}

impl Default for TableMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe façade for the metadata store.
pub struct PageDirectory {
    store: Arc<RwLock<TableMetaStore>>,
}

impl PageDirectory {
    pub fn new(store: Arc<RwLock<TableMetaStore>>) -> Self {
        Self { store }
    }

    pub fn latest(&self, column: &str) -> Option<PageDescriptor> {
        let guard = self.store.read().ok()?;
        guard.latest(column)
    }

    pub fn pages_for_column(&self, column: &str) -> Vec<PageDescriptor> {
        let guard = match self.store.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.pages_for_column(column)
    }

    pub fn lookup(&self, id: &str) -> Option<PageDescriptor> {
        let guard = self.store.read().ok()?;
        guard.lookup(id)
    }

    pub fn locate_row(&self, column: &str, row: u64) -> Option<RowLocation> {
        let guard = self.store.read().ok()?;
        guard.locate_row(column, row)
    }

    pub fn locate_range(&self, column: &str, start_row: u64, end_row: u64) -> Vec<PageSlice> {
        let guard = match self.store.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.locate_range(column, start_row, end_row)
    }

    pub fn range(
        &self,
        column: &str,
        l_bound: u64,
        r_bound: u64,
        _commit_time_upper_bound: u64,
    ) -> Vec<PageDescriptor> {
        if r_bound == 0 || l_bound >= r_bound {
            return Vec::new();
        }
        let end = r_bound - 1;
        self.locate_range(column, l_bound, end)
            .into_iter()
            .map(|slice| slice.descriptor)
            .collect()
    }

    pub fn register_page(
        &self,
        column: &str,
        disk_path: String,
        offset: u64,
    ) -> Option<PageDescriptor> {
        self.register_page_with_sizes(column, disk_path, offset, 0, 1, 1)
    }

    pub fn register_page_with_size(
        &self,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
    ) -> Option<PageDescriptor> {
        self.register_page_with_sizes(column, disk_path, offset, alloc_len, alloc_len, 1)
    }

    pub fn register_page_with_sizes(
        &self,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> Option<PageDescriptor> {
        let mut guard = self.store.write().ok()?;
        Some(guard.register_page(
            column,
            disk_path,
            offset,
            alloc_len,
            actual_len,
            entry_count,
        ))
    }

    pub fn register_batch(&self, pages: &[PendingPage]) -> Vec<PageDescriptor> {
        let mut guard = match self.store.write() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.register_batch(pages)
    }
}

#[inline]
fn apply_delta_suffix(entries: &mut [u64], delta: i64) {
    if delta == 0 || entries.is_empty() {
        return;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") && entries.len() >= 8 {
            unsafe {
                add_delta_suffix_avx2(entries, delta);
            }
            return;
        }
    }

    apply_delta_suffix_scalar(entries, delta);
}

#[inline]
fn apply_delta_suffix_scalar(entries: &mut [u64], delta: i64) {
    if delta >= 0 {
        let delta_u = delta as u64;
        for value in entries {
            *value = value.wrapping_add(delta_u);
        }
    } else {
        let delta_u = (-delta) as u64;
        for value in entries {
            *value = value.wrapping_sub(delta_u);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn add_delta_suffix_avx2(entries: &mut [u64], delta: i64) {
    use std::arch::x86_64::{
        __m256i, _mm256_add_epi64, _mm256_loadu_si256, _mm256_set1_epi64x, _mm256_storeu_si256,
    };

    let delta_vec = _mm256_set1_epi64x(delta);
    let mut chunks = entries.chunks_exact_mut(4);
    for chunk in &mut chunks {
        let ptr = chunk.as_mut_ptr() as *mut __m256i;
        let current = _mm256_loadu_si256(ptr as *const __m256i);
        let updated = _mm256_add_epi64(current, delta_vec);
        _mm256_storeu_si256(ptr, updated);
    }

    let remainder = chunks.into_remainder();
    apply_delta_suffix_scalar(remainder, delta);
}
