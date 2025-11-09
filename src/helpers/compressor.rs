use crate::cache::page_cache::{PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::page::Page;
use crate::sql::executor::batch::ColumnarPage;
use bincode;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::sync::Arc;

/*
a dumb helper, nothing else
*/
pub struct Compressor {}

impl Compressor {
    pub fn new() -> Self {
        Compressor {}
    }

    pub fn compress(&self, data: Arc<PageCacheEntryUncompressed>) -> PageCacheEntryCompressed {
        let disk_page: Page = data.as_ref().page.as_disk_page();
        let serialized: Vec<u8> = bincode::serialize(&disk_page).unwrap();
        let compressed: Vec<u8> = compress_prepend_size(&serialized);
        PageCacheEntryCompressed { page: compressed }
    }

    pub fn decompress(&self, data: Arc<PageCacheEntryCompressed>) -> ColumnarPage {
        let decompressed: Vec<u8> = decompress_size_prepended(&data.as_ref().page).unwrap();
        let page: Page = bincode::deserialize(&decompressed).unwrap();
        ColumnarPage::from_disk_page(page)
    }
}
