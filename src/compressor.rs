use crate::{page_cache::CombinedCache, page_io::{read_from_path,write_to_path}};
use crate::metadata_store;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use bincode;
use crate::page::Page;
use crate::page_cache::{PageCacheEntryUncompressed,PageCacheEntryCompressed};

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
    pub fn compress(&self, tablemeta: metadata_store::TableMetaStore,mut cache: CombinedCache, id: &str) -> Option<bool>{
        // reads something from un-compressed page cache , compresses and write to disk 
        // also invalidates the compressed page cache
        let page = &cache.uncompressed_pages.get(id).unwrap().page.page;
        let raw = bincode::serialize(page).expect("serialize Page failed");
        let compressed = compress_prepend_size(&raw);

        let (path,offset) = tablemeta.get_page_path_and_offset(id).unwrap();
        let _ = write_to_path(path, offset, compressed);
        Some(true)
    }

    pub fn decompress(&self, tablemeta: &metadata_store::TableMetaStore ,mut cache: CombinedCache,id: &str) -> CombinedCache{
        let (path,offset) = tablemeta.get_page_path_and_offset(id).unwrap();
        let compressed_data = read_from_path(path, offset);
        let decompressed_data = decompress_size_prepended(&compressed_data).expect("decompress failed");
        cache.compressed_pages.add(id,PageCacheEntryCompressed {page: decompressed_data});
        cache
    }

    pub fn decompress_from_cache(&self, tablemeta: &metadata_store::TableMetaStore ,mut cache: CombinedCache,id: &str) -> CombinedCache{
        let compressed_data = &cache.compressed_pages.get(id).unwrap().page.page;
        let decompressed_data_raw = decompress_size_prepended(compressed_data).expect("decompress failed");
        let decompressed_data: Page = bincode::deserialize(&decompressed_data_raw).expect("deserialize failed");
        cache.uncompressed_pages.add(id, PageCacheEntryUncompressed {page: decompressed_data});
        cache
    }

    
}