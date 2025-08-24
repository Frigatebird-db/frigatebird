use crate::entry::Entry;
use crate::metadata_store::TableMetaStore;
use crate::page_cache::CombinedCache;

fn upsert_row_into_column(meta_store: TableMetaStore, cache: CombinedCache, col: &str,data: &str) -> Result<(), Box<dyn std::error::Error>> {
    // get the latest page for this col from meta store
    let latest_page_meta = meta_store.get_latest_page_meta(col).unwrap();

    // todo: make an entry out of this row object

    // check if page in uncompressed cache, if yes, just update and flush 
    if cache.uncompressed_pages.has(&latest_page_meta.id) {
        // update and flush
        return Ok(())
    }
    
    // check if page in compressed cache, if yes, decompress and put into compressed cache and do the above
    if cache.compressed_pages.has(&latest_page_meta.id) {
        // decompresses, update and flush
        return Ok(())
    }

    // if not in any cache, just fetch from disk and do both above stuffs


    Ok(())


}