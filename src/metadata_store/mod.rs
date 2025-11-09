use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub const DEFAULT_TABLE: &str = "_default";
pub const ROWS_PER_PAGE_GROUP: u64 = 1024;

fn split_table_column(identifier: &str) -> (&str, &str) {
    if let Some(pos) = identifier.find('.') {
        let table = &identifier[..pos];
        let column = &identifier[pos + 1..];
        if table.is_empty() || column.is_empty() {
            (DEFAULT_TABLE, identifier)
        } else {
            (table, column)
        }
    } else {
        (DEFAULT_TABLE, identifier)
    }
}

/// Identifies a column within a specific table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TableColumnKey {
    table: String,
    column: String,
}

impl TableColumnKey {
    fn new(table: &str, column: &str) -> Self {
        TableColumnKey {
            table: table.to_string(),
            column: column.to_string(),
        }
    }
}

/// Definition supplied when registering a table.
#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        ColumnDefinition {
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableDefinition {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub sort_key: Vec<String>,
    pub rows_per_page_group: u64,
}

impl TableDefinition {
    pub fn new(
        name: impl Into<String>,
        columns: Vec<ColumnDefinition>,
        sort_key: Vec<String>,
    ) -> Self {
        TableDefinition {
            name: name.into(),
            columns,
            sort_key,
            rows_per_page_group: ROWS_PER_PAGE_GROUP,
        }
    }
}

/// Catalog entry describing a single column.
#[derive(Clone, Debug)]
pub struct ColumnCatalog {
    pub name: String,
    pub data_type: String,
    pub ordinal: usize,
}

/// Catalog entry describing a table schema.
#[derive(Clone, Debug)]
pub struct TableCatalog {
    pub name: String,
    columns: Vec<ColumnCatalog>,
    column_index: HashMap<String, usize>,
    sort_key_ordinals: Vec<usize>,
    pub rows_per_page_group: u64,
}

impl TableCatalog {
    fn new(
        name: String,
        columns: Vec<ColumnCatalog>,
        column_index: HashMap<String, usize>,
        sort_key_ordinals: Vec<usize>,
        rows_per_page_group: u64,
    ) -> Self {
        TableCatalog {
            name,
            columns,
            column_index,
            sort_key_ordinals,
            rows_per_page_group,
        }
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ColumnCatalog> {
        self.column_index
            .get(name)
            .and_then(|idx| self.columns.get(*idx))
    }

    pub fn sort_key(&self) -> Vec<&ColumnCatalog> {
        self.sort_key_ordinals
            .iter()
            .filter_map(|idx| self.columns.get(*idx))
            .collect()
    }

    fn insert_column(&mut self, name: String, data_type: String) -> bool {
        if self.column_index.contains_key(&name) {
            return false;
        }
        let ordinal = self.columns.len();
        self.column_index.insert(name.clone(), ordinal);
        self.columns.push(ColumnCatalog {
            name,
            data_type,
            ordinal,
        });
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    TableExists(String),
    UnknownTable(String),
    UnknownColumn { table: String, column: String },
    DuplicateColumn { table: String, column: String },
    DuplicateSortKey { table: String, column: String },
    StoreUnavailable,
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableExists(table) => {
                write!(f, "table already exists: {table}")
            }
            CatalogError::UnknownTable(table) => write!(f, "unknown table: {table}"),
            CatalogError::UnknownColumn { table, column } => {
                write!(f, "unknown column {table}.{column}")
            }
            CatalogError::DuplicateColumn { table, column } => {
                write!(f, "duplicate column {table}.{column}")
            }
            CatalogError::DuplicateSortKey { table, column } => {
                write!(f, "duplicate column in ORDER BY: {table}.{column}")
            }
            CatalogError::StoreUnavailable => write!(f, "metadata store unavailable"),
        }
    }
}

impl std::error::Error for CatalogError {}

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
}

impl ColumnChain {
    fn new() -> Self {
        ColumnChain {
            pages: Vec::with_capacity(8),
        }
    }

    fn push(&mut self, descriptor: PageDescriptor) {
        self.pages.push(descriptor);
    }

    fn replace_last(&mut self, descriptor: PageDescriptor) {
        if self.pages.is_empty() {
            self.push(descriptor);
            return;
        }

        if let Some(last_page) = self.pages.last_mut() {
            *last_page = descriptor;
        }
    }

    fn total_entries(&self, rows_per_page_group: u64) -> u64 {
        if self.pages.is_empty() {
            return 0;
        }

        let full_groups = self.pages.len().saturating_sub(1) as u64;
        let last_page_entries = self.pages.last().map(|p| p.entry_count).unwrap_or(0);
        full_groups * rows_per_page_group + last_page_entries
    }

    fn last(&self) -> Option<PageDescriptor> {
        self.pages.last().cloned()
    }

    fn all_pages(&self) -> Vec<PageDescriptor> {
        self.pages.clone()
    }

    fn update_last_entry_count(&mut self, entry_count: u64) -> Option<PageDescriptor> {
        if self.pages.is_empty() {
            return None;
        }
        let last_idx = self.pages.len() - 1;
        self.pages[last_idx].entry_count = entry_count;
        self.pages.last().cloned()
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
    pub table: String,
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
    tables: HashMap<String, TableCatalog>,
    column_chains: HashMap<TableColumnKey, ColumnChain>,
    page_index: HashMap<String, PageDescriptor>,
    next_page_id: u64,
}

impl TableMetaStore {
    pub fn new() -> Self {
        TableMetaStore {
            tables: HashMap::new(),
            column_chains: HashMap::new(),
            page_index: HashMap::new(),
            next_page_id: 1,
        }
    }

    fn ensure_default_table_column(&mut self, column: &str) {
        if !self.tables.contains_key(DEFAULT_TABLE) {
            let mut column_index = HashMap::new();
            column_index.insert(column.to_string(), 0);
            let catalog = TableCatalog::new(
                DEFAULT_TABLE.to_string(),
                vec![ColumnCatalog {
                    name: column.to_string(),
                    data_type: "String".to_string(),
                    ordinal: 0,
                }],
                column_index,
                Vec::new(),
                ROWS_PER_PAGE_GROUP,
            );
            self.tables.insert(DEFAULT_TABLE.to_string(), catalog);
            self.column_chains.insert(
                TableColumnKey::new(DEFAULT_TABLE, column),
                ColumnChain::new(),
            );
            return;
        }

        if let Some(catalog) = self.tables.get_mut(DEFAULT_TABLE) {
            if catalog.column(column).is_none() {
                if catalog.insert_column(column.to_string(), "String".to_string()) {
                    self.column_chains.insert(
                        TableColumnKey::new(DEFAULT_TABLE, column),
                        ColumnChain::new(),
                    );
                }
            }
        }
    }

    pub fn register_table(&mut self, definition: TableDefinition) -> Result<(), CatalogError> {
        let TableDefinition {
            name,
            columns,
            sort_key,
            rows_per_page_group,
        } = definition;

        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableExists(name.clone()));
        }

        let mut column_index: HashMap<String, usize> = HashMap::with_capacity(columns.len());
        let mut catalog_columns: Vec<ColumnCatalog> = Vec::with_capacity(columns.len());
        for (ordinal, column) in columns.into_iter().enumerate() {
            if column_index.contains_key(&column.name) {
                return Err(CatalogError::DuplicateColumn {
                    table: name.clone(),
                    column: column.name,
                });
            }
            column_index.insert(column.name.clone(), ordinal);
            catalog_columns.push(ColumnCatalog {
                name: column.name,
                data_type: column.data_type,
                ordinal,
            });
        }

        let mut seen_sort = HashSet::with_capacity(sort_key.len());
        let mut sort_key_ordinals = Vec::with_capacity(sort_key.len());
        for key in sort_key {
            if !seen_sort.insert(key.clone()) {
                return Err(CatalogError::DuplicateSortKey {
                    table: name.clone(),
                    column: key,
                });
            }
            let ordinal =
                column_index
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownColumn {
                        table: name.clone(),
                        column: key.clone(),
                    })?;
            sort_key_ordinals.push(ordinal);
        }

        for column in &catalog_columns {
            let key = TableColumnKey::new(&name, &column.name);
            debug_assert!(
                !self.column_chains.contains_key(&key),
                "column chain unexpectedly exists for {}.{}",
                &name,
                &column.name
            );
            self.column_chains.insert(key, ColumnChain::new());
        }

        let catalog = TableCatalog::new(
            name.clone(),
            catalog_columns,
            column_index,
            sort_key_ordinals,
            rows_per_page_group,
        );
        self.tables.insert(name, catalog);
        Ok(())
    }

    pub fn add_columns(
        &mut self,
        table: &str,
        columns: Vec<ColumnDefinition>,
    ) -> Result<(), CatalogError> {
        let catalog = self
            .tables
            .get_mut(table)
            .ok_or_else(|| CatalogError::UnknownTable(table.to_string()))?;

        for column in columns {
            if catalog.insert_column(column.name.clone(), column.data_type) {
                let key = TableColumnKey::new(table, &column.name);
                self.column_chains
                    .entry(key)
                    .or_insert_with(ColumnChain::new);
            }
        }

        Ok(())
    }

    pub fn table(&self, name: &str) -> Option<&TableCatalog> {
        self.tables.get(name)
    }

    pub fn table_catalog(&self, name: &str) -> Option<TableCatalog> {
        self.tables.get(name).cloned()
    }

    pub fn column_defined(&self, table: &str, column: &str) -> bool {
        let key = TableColumnKey::new(table, column);
        self.column_chains.contains_key(&key)
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

    pub fn latest(&self, table: &str, column: &str) -> Option<PageDescriptor> {
        let key = TableColumnKey::new(table, column);
        self.column_chains.get(&key).and_then(|chain| chain.last())
    }

    pub fn pages_for_column(&self, table: &str, column: &str) -> Vec<PageDescriptor> {
        let key = TableColumnKey::new(table, column);
        self.column_chains
            .get(&key)
            .map(|chain| chain.all_pages())
            .unwrap_or_default()
    }

    pub fn lookup(&self, id: &str) -> Option<PageDescriptor> {
        self.page_index.get(id).cloned()
    }

    pub fn locate_row(&self, table: &str, column: &str, row: u64) -> Option<RowLocation> {
        let catalog = self.tables.get(table)?;
        let rows_per_page_group = catalog.rows_per_page_group;

        let key = TableColumnKey::new(table, column);
        let chain = self.column_chains.get(&key)?;

        if row >= chain.total_entries(rows_per_page_group) {
            return None;
        }

        let page_index = (row / rows_per_page_group) as usize;
        let descriptor = chain.pages.get(page_index)?.clone();
        let page_row_index = row % rows_per_page_group;

        Some(RowLocation {
            descriptor,
            page_row_index,
        })
    }

    pub fn locate_range(
        &self,
        table: &str,
        column: &str,
        start_row: u64,
        end_row: u64,
    ) -> Vec<PageSlice> {
        let catalog = match self.tables.get(table) {
            Some(c) => c,
            None => return Vec::new(),
        };
        let rows_per_page_group = catalog.rows_per_page_group;

        let key = TableColumnKey::new(table, column);
        let chain = match self.column_chains.get(&key) {
            Some(c) => c,
            None => return Vec::new(),
        };

        if chain.pages.is_empty() || start_row > end_row {
            return Vec::new();
        }

        let total = chain.total_entries(rows_per_page_group);
        if total == 0 || start_row >= total {
            return Vec::new();
        }

        let clamped_end_row = end_row.min(total.saturating_sub(1));
        let start_page_idx = (start_row / rows_per_page_group) as usize;
        let end_page_idx = (clamped_end_row / rows_per_page_group) as usize;

        let mut slices = Vec::with_capacity(end_page_idx - start_page_idx + 1);
        for idx in start_page_idx..=end_page_idx {
            let descriptor = match chain.pages.get(idx) {
                Some(p) => p.clone(),
                None => continue,
            };

            let slice_start_offset = if idx == start_page_idx {
                start_row % rows_per_page_group
            } else {
                0
            };

            let mut slice_end_offset = if idx == end_page_idx {
                (clamped_end_row % rows_per_page_group) + 1
            } else {
                rows_per_page_group
            };
            slice_end_offset = slice_end_offset.min(descriptor.entry_count);

            slices.push(PageSlice {
                descriptor,
                start_row_offset: slice_start_offset,
                end_row_offset: slice_end_offset,
            });
        }

        slices
    }

    pub fn register_page(
        &mut self,
        table: &str,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> Option<PageDescriptor> {
        let key = TableColumnKey::new(table, column);
        if !self.column_chains.contains_key(&key) {
            if table == DEFAULT_TABLE {
                self.ensure_default_table_column(column);
            }
        }
        if !self.column_chains.contains_key(&key) {
            return None;
        }
        let descriptor =
            self.allocate_descriptor(disk_path, offset, alloc_len, actual_len, entry_count);
        {
            let chain = self
                .column_chains
                .get_mut(&key)
                .expect("column chain must exist after contains_key check");
            chain.push(descriptor.clone());
        }
        self.page_index
            .insert(descriptor.id.clone(), descriptor.clone());
        Some(descriptor)
    }

    pub fn register_batch(&mut self, pages: &[PendingPage]) -> Vec<PageDescriptor> {
        let mut registered = Vec::with_capacity(pages.len());
        for page in pages {
            let key = TableColumnKey::new(&page.table, &page.column);
            if !self.column_chains.contains_key(&key) {
                if page.table == DEFAULT_TABLE {
                    self.ensure_default_table_column(&page.column);
                }
                if !self.column_chains.contains_key(&key) {
                    continue;
                }
            }
            let descriptor = self.allocate_descriptor(
                page.disk_path.clone(),
                page.offset,
                page.alloc_len,
                page.actual_len,
                page.entry_count,
            );
            let mut old_id: Option<String> = None;

            if let Some(chain) = self.column_chains.get_mut(&key) {
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
            } else {
                continue;
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

    pub fn update_latest_entry_count(
        &mut self,
        table: &str,
        column: &str,
        entry_count: u64,
    ) -> Result<(), CatalogError> {
        let key = TableColumnKey::new(table, column);
        let chain =
            self.column_chains
                .get_mut(&key)
                .ok_or_else(|| CatalogError::UnknownColumn {
                    table: table.to_string(),
                    column: column.to_string(),
                })?;

        let descriptor = chain.update_last_entry_count(entry_count).ok_or_else(|| {
            CatalogError::UnknownColumn {
                table: table.to_string(),
                column: column.to_string(),
            }
        })?;

        self.page_index.insert(descriptor.id.clone(), descriptor);
        Ok(())
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

    pub fn register_table(&self, definition: TableDefinition) -> Result<(), CatalogError> {
        let mut guard = self
            .store
            .write()
            .map_err(|_| CatalogError::StoreUnavailable)?;
        guard.register_table(definition)
    }

    pub fn add_columns_to_table(
        &self,
        table: &str,
        columns: Vec<ColumnDefinition>,
    ) -> Result<(), CatalogError> {
        let mut guard = self
            .store
            .write()
            .map_err(|_| CatalogError::StoreUnavailable)?;
        guard.add_columns(table, columns)
    }

    pub fn table_catalog(&self, name: &str) -> Option<TableCatalog> {
        let guard = self.store.read().ok()?;
        guard.table_catalog(name)
    }

    pub fn latest_in_table(&self, table: &str, column: &str) -> Option<PageDescriptor> {
        let guard = self.store.read().ok()?;
        guard.latest(table, column)
    }

    pub fn latest(&self, column: &str) -> Option<PageDescriptor> {
        let (table, column) = split_table_column(column);
        self.latest_in_table(table, column)
    }

    pub fn pages_for_in_table(&self, table: &str, column: &str) -> Vec<PageDescriptor> {
        let guard = match self.store.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.pages_for_column(table, column)
    }

    pub fn pages_for_column(&self, column: &str) -> Vec<PageDescriptor> {
        let (table, column) = split_table_column(column);
        self.pages_for_in_table(table, column)
    }

    pub fn lookup(&self, id: &str) -> Option<PageDescriptor> {
        let guard = self.store.read().ok()?;
        guard.lookup(id)
    }

    pub fn locate_row_in_table(&self, table: &str, column: &str, row: u64) -> Option<RowLocation> {
        let guard = self.store.read().ok()?;
        guard.locate_row(table, column, row)
    }

    pub fn locate_row(&self, column: &str, row: u64) -> Option<RowLocation> {
        let (table, column) = split_table_column(column);
        self.locate_row_in_table(table, column, row)
    }

    pub fn locate_range_in_table(
        &self,
        table: &str,
        column: &str,
        start_row: u64,
        end_row: u64,
    ) -> Vec<PageSlice> {
        let guard = match self.store.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.locate_range(table, column, start_row, end_row)
    }

    pub fn locate_range(&self, column: &str, start_row: u64, end_row: u64) -> Vec<PageSlice> {
        let (table, column) = split_table_column(column);
        self.locate_range_in_table(table, column, start_row, end_row)
    }

    pub fn range_in_table(
        &self,
        table: &str,
        column: &str,
        l_bound: u64,
        r_bound: u64,
        _commit_time_upper_bound: u64,
    ) -> Vec<PageDescriptor> {
        if r_bound == 0 || l_bound >= r_bound {
            return Vec::new();
        }
        let end = r_bound - 1;
        self.locate_range_in_table(table, column, l_bound, end)
            .into_iter()
            .map(|slice| slice.descriptor)
            .collect()
    }

    pub fn range(
        &self,
        column: &str,
        l_bound: u64,
        r_bound: u64,
        commit_time_upper_bound: u64,
    ) -> Vec<PageDescriptor> {
        let (table, column) = split_table_column(column);
        self.range_in_table(table, column, l_bound, r_bound, commit_time_upper_bound)
    }

    pub fn register_page_in_table(
        &self,
        table: &str,
        column: &str,
        disk_path: String,
        offset: u64,
    ) -> Option<PageDescriptor> {
        self.register_page_in_table_with_sizes(table, column, disk_path, offset, 0, 1, 1)
    }

    pub fn register_page(
        &self,
        column: &str,
        disk_path: String,
        offset: u64,
    ) -> Option<PageDescriptor> {
        let (table, column) = split_table_column(column);
        self.register_page_in_table(table, column, disk_path, offset)
    }

    pub fn register_page_in_table_with_size(
        &self,
        table: &str,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
    ) -> Option<PageDescriptor> {
        self.register_page_in_table_with_sizes(
            table, column, disk_path, offset, alloc_len, alloc_len, 1,
        )
    }

    pub fn register_page_with_size(
        &self,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
    ) -> Option<PageDescriptor> {
        let (table, column) = split_table_column(column);
        self.register_page_in_table_with_size(table, column, disk_path, offset, alloc_len)
    }

    pub fn register_page_in_table_with_sizes(
        &self,
        table: &str,
        column: &str,
        disk_path: String,
        offset: u64,
        alloc_len: u64,
        actual_len: u64,
        entry_count: u64,
    ) -> Option<PageDescriptor> {
        let mut guard = self.store.write().ok()?;
        guard.register_page(
            table,
            column,
            disk_path,
            offset,
            alloc_len,
            actual_len,
            entry_count,
        )
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
        let (table, column) = split_table_column(column);
        self.register_page_in_table_with_sizes(
            table,
            column,
            disk_path,
            offset,
            alloc_len,
            actual_len,
            entry_count,
        )
    }

    pub fn register_batch(&self, pages: &[PendingPage]) -> Vec<PageDescriptor> {
        let mut guard = match self.store.write() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        guard.register_batch(pages)
    }

    pub fn update_latest_entry_count(
        &self,
        table: &str,
        column: &str,
        entry_count: u64,
    ) -> Result<(), CatalogError> {
        let mut guard = self
            .store
            .write()
            .map_err(|_| CatalogError::StoreUnavailable)?;
        guard.update_latest_entry_count(table, column, entry_count)
    }
}
