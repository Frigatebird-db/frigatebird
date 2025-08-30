use crate::entry::Entry;
use crate::metadata_store::TableMetaStore;
use crate::page_cache::CombinedCache;
use crate::compressor::Compressor;


// TODO: we also have to update the (l,r) ranges whenever we upsert something into it and there certainly has to be a better way to do it along with updating metadata in one shot
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

// I hope the below stuff is correct, im fried rn
fn update_column_entry(entry: Entry, compressor: Compressor, meta_store: TableMetaStore, mut cache: CombinedCache, col: &str,data: &str, row: u64) -> Result<bool, Box<dyn std::error::Error>> {
    let latest_page_meta = meta_store.get_latest_page_meta(col).unwrap();
    let page_id = latest_page_meta.id.clone();
    let entry = Entry::new(data);

    // check if page in uncompressed cache, if yes, just update and flush 
    if cache.uncompressed_pages.has(&latest_page_meta.id) {
        cache.uncompressed_pages.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
        // note tha the below stuff might totally own the cache and drain it once gone out of scope
        let ok = compressor.compress(meta_store,cache,&page_id).unwrap();
        return Ok((ok))
    }
    
    // check if page in compressed cache, if yes, decompress and put into compressed cache and do the above
    if cache.compressed_pages.has(&latest_page_meta.id) {
        cache = compressor.decompress_from_cache(&meta_store,cache,&page_id);
        // fetches from compressed cache ,decompresses, inserts in the uncompressed page cache, then does the sames as above
        cache.uncompressed_pages.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
        let ok = compressor.compress(meta_store,cache,&page_id).unwrap();
        return Ok((ok))
    }

    // fetches from disk, then does the same as above
    cache = compressor.decompress(&meta_store,cache,&page_id);
    cache = compressor.decompress_from_cache(&meta_store,cache,&page_id);
    // cache.uncompressed_pages.get(&page_id).unwrap().page.page.add_entry(entry);
    cache.uncompressed_pages.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
    let ok = compressor.compress(meta_store,cache,&page_id).unwrap();

    Ok((ok))


}

fn read_single_column_entry(col: String, row: u64) {

}

fn range_scan_column_entry(col: String, l_row: u64,r_row: u64) {
    /*
    quickly find the internal (l,r) groupings and the latest pages needed for it and put an lock on them so they dont perish
    

    query them parallely fast 

    holy fuck, we need some sort of lock thingy per column lol

    yep, something... like, the thing is , our (l,r) keeper thingy isnt really MVCCing and I dont really want it to be that way

    once we know that "nothing will change in between me figuring out the bounds", we can figure things out with one binary search

    soo... figure out the bounds, and get their latest page IDs 

    okay, so thinking about it, our Pages are kinda versioned nicely because yesterday's nubskr was kind to us

    so we don't need to worry about that... 

    so.. just figure out how to get those (l,r) bounds in one shot and call it a day

    also note that you cant just lock stuff as that would involve "waiting" into the game which we dont want


    okay, so for now we just take a read lock and get an immutable snapshot of the data, can we do that ??

    umm... just get a read lock, run a fast binary search, get the index bounds of stuff you need and just grab the latest page metas of them, that's fucking it

    I mean, we can kinda just.... clone after taking a read lock and never have to do a call after that ever again for this request

    okay, so we kinda need a very very fast way to get the latest page metas for a contigious group of (l,r) bounds

    okay, whateer, lets keep it simple, make all that internal stuff be done my metadata store, and it would return us with just relevant page_ids

    and after that we would be done with it and just deal with page cache store to actually get the stuffs
    */
    

}

