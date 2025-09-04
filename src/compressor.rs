use std::sync::Arc;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use bincode;
use crate::{page::Page, page_cache::{PageCacheEntryCompressed, PageCacheEntryUncompressed}};

/*
a dumb helper, nothing else
*/
pub struct Compressor {}

impl Compressor {
    pub fn new() -> Self {
        Compressor {}
    }

    pub fn compress(&self, data: Arc<PageCacheEntryUncompressed>) -> PageCacheEntryCompressed {
        // serialize Page then compress
        let serialized: Vec<u8> = bincode::serialize(&data.as_ref().page).unwrap();
        let compressed: Vec<u8> = compress_prepend_size(&serialized);
        PageCacheEntryCompressed { page: compressed }
    }

    pub fn decompress(&self,data: Arc<PageCacheEntryCompressed>) -> PageCacheEntryUncompressed {
        // decompress then deserialize back to Page
        let decompressed: Vec<u8> = decompress_size_prepended(&data.as_ref().page).unwrap();
        let page: Page = bincode::deserialize(&decompressed).unwrap();
        PageCacheEntryUncompressed { page }
    }
}