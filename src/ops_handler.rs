use crate::entry::Entry;
use crate::metadata_store::TableMetaStore;
use crate::page_cache::CombinedCache;
use crate::compressor::Compressor;

fn upsert_data_into_column(entry: Entry, compressor: Compressor, meta_store: TableMetaStore, mut cache: CombinedCache, col: &str,data: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let latest_page_meta = meta_store.get_latest_page_meta(col).unwrap();
    let page_id = latest_page_meta.id.clone();
    let entry = Entry::new(data);

    // check if page in uncompressed cache, if yes, just update and flush 
    if cache.uncompressed_pages.has(&latest_page_meta.id) {
        cache.uncompressed_pages.get(&page_id).unwrap().page.page.add_entry(entry);
        // note tha the below stuff might totally own the cache and drain it once gone out of scope
        let ok = compressor.compress(meta_store,cache,&page_id).unwrap();
        return Ok((ok))
    }
    
    // check if page in compressed cache, if yes, decompress and put into compressed cache and do the above
    if cache.compressed_pages.has(&latest_page_meta.id) {
        cache = compressor.decompress_from_cache(&meta_store,cache,&page_id);
        // fetches from compressed cache ,decompresses, inserts in the uncompressed page cache, then does the sames as above
        cache.uncompressed_pages.get(&page_id).unwrap().page.page.add_entry(entry);
        let ok = compressor.compress(meta_store,cache,&page_id).unwrap();
        return Ok((ok))
    }

    // fetches from disk, then does the same as above
    cache = compressor.decompress(&meta_store,cache,&page_id);
    cache = compressor.decompress_from_cache(&meta_store,cache,&page_id);
    cache.uncompressed_pages.get(&page_id).unwrap().page.page.add_entry(entry);
    let ok = compressor.compress(meta_store,cache,&page_id).unwrap();

    Ok((ok))


}