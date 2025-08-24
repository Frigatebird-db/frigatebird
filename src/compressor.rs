use crate::{page_cache::CombinedCache, page_io::{read_from_path,write_to_path}};
use crate::metadata_store;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use bincode;

/*
reads and writes stuff via page_io
and also deals with insertions in uncompressed page cache
*/
struct Compressor {}

impl Compressor {
    fn new() -> Self {
        Compressor {}
    }

    // jsyk there are multiple race conditions with this shit btw
    fn compress(tablemeta: metadata_store::TableMetaStore  ,cache: CombinedCache, id: &str) -> Option<bool>{
        // reads something from un-compressed page cache , compresses and write to disk 
        // also invalidates the compressed page cache
        let page = &cache.uncompressed_pages.get(id).unwrap().page.page;
        let raw = bincode::serialize(page).expect("serialize Page failed");
        let compressed = compress_prepend_size(&raw);

        let (path,offset) = tablemeta.get_page_path_and_offset(id).unwrap();
        // todo: get the path and offset from table metadata store
        let _ = write_to_path(path, offset, compressed);
        Some(true)
    }

    fn decompress(tablemeta: metadata_store::TableMetaStore ,cache: CombinedCache,id: &str) -> Vec<u8> {
        // read from this path, decompress and stores those decompressed bytes into uncompressed page cache
        // todo: get the path and offset from table metadata store
        let (path,offset) = tablemeta.get_page_path_and_offset(id).unwrap();
        let compressed_data = read_from_path(path, offset);
        let decompressed_data = decompress_size_prepended(&compressed_data).expect("decompress failed");
        // todo: instead of returning, deserialize the decompressed_data and insert it into compressed_data
        decompressed_data
    }
}