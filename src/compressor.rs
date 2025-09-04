use crate::page_io::{read_from_path,write_to_path};
use crate::metadata_store::TableMetaStoreWrapper;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use bincode;
use crate::page::Page;
use crate::page_cache::{PageCacheEntryUncompressed,PageCacheEntryCompressed, PageCacheWrapper};

/*
reads and writes stuff via page_io
and also deals with insertions in uncompressed page cache
*/
pub struct Compressor {}

impl Compressor {
    pub fn new() -> Self {
        Compressor {}
    }

    // this compresses some page from cache and flushes it btw
    // jsyk there are multiple race conditions with this shit btw
    pub fn compress(&self, tablemeta: &TableMetaStoreWrapper, uncompressed_cache: &PageCacheWrapper<PageCacheEntryUncompressed>, id: &str) -> Option<bool>{
        // reads something from un-compressed page cache , compresses and write to disk 
        // also invalidates the compressed page cache
        let mut uc_guard = uncompressed_cache.page_cache.write().unwrap();
        let page = &uc_guard.get(id).unwrap().page.page;
        let raw = bincode::serialize(page).expect("serialize Page failed");
        let compressed = compress_prepend_size(&raw);

        let tm_guard = tablemeta.table_meta_store.read().unwrap();
        let smth = tm_guard.get_page_path_and_offset(id).unwrap();
        let _ = write_to_path(&smth.disk_path, smth.offset, compressed);
        Some(true)
    }

    pub fn decompress(&self, tablemeta: &TableMetaStoreWrapper, compressed_cache: &PageCacheWrapper<PageCacheEntryCompressed>, id: &str) {
        let tm_guard = tablemeta.table_meta_store.read().unwrap();
        let smth = tm_guard.get_page_path_and_offset(id).unwrap();
        let compressed_data = read_from_path(&smth.disk_path, smth.offset);
        let mut cc_guard = compressed_cache.page_cache.write().unwrap();
        cc_guard.add(id,PageCacheEntryCompressed {page: compressed_data});
    }

    pub fn decompress_from_cache(&self, _tablemeta: &TableMetaStoreWrapper, compressed_cache: &PageCacheWrapper<PageCacheEntryCompressed>, uncompressed_cache: &PageCacheWrapper<PageCacheEntryUncompressed>, id: &str) {
        let mut cc_guard = compressed_cache.page_cache.write().unwrap();
        let compressed_data = &cc_guard.get(id).unwrap().page.page;
        let decompressed_data_raw = decompress_size_prepended(compressed_data).expect("decompress failed");
        let decompressed_data: Page = bincode::deserialize(&decompressed_data_raw).expect("deserialize failed");
        drop(cc_guard);
        let mut uc_guard = uncompressed_cache.page_cache.write().unwrap();
        uc_guard.add(id, PageCacheEntryUncompressed {page: decompressed_data});
    }

    
}