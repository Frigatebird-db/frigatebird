use crate::cache::page_cache::{PageCacheEntryCompressed, PageCacheEntryUncompressed};
use crate::page::Page;
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
        let serialized: Vec<u8> =
            bincode::serialize(&disk_page).expect("failed to serialize page for compression");
        let compressed: Vec<u8> = compress_prepend_size(&serialized);
        PageCacheEntryCompressed { page: compressed }
    }

    pub fn decompress(&self, data: Arc<PageCacheEntryCompressed>) -> Page {
        let payload = &data.as_ref().page;
        if payload.is_empty() {
            return Page::new();
        }

        match decompress_size_prepended(payload) {
            Ok(bytes) => {
                let page: Page =
                    bincode::deserialize(&bytes).expect("failed to deserialize decompressed page");
                page
            }
            Err(_) => {
                // Fallback for legacy/uncompressed pages or truncated buffers.
                match bincode::deserialize::<Page>(payload) {
                    Ok(page) => page,
                    Err(err) => panic!("failed to decompress page payload: {err}"),
                }
            }
        }
    }
}
