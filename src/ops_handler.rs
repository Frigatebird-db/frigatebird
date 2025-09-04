// use crate::entry::Entry;
// use crate::page_cache::{PageCacheWrapper, PageCacheEntryCompressed, PageCacheEntryUncompressed};
// use crate::compressor::Compressor;
// use crate::entry::current_epoch_millis;


// // TODO: we also have to update the (l,r) ranges whenever we upsert something into it and there certainly has to be a better way to do it along with updating metadata in one shot
// fn upsert_data_into_column(_entry: Entry, compressor: Compressor, meta_store: &TableMetaStoreWrapper, compressed_cache: &PageCacheWrapper<PageCacheEntryCompressed>, uncompressed_cache: &PageCacheWrapper<PageCacheEntryUncompressed>, col: &str, data: &str) -> Result<bool, Box<dyn std::error::Error>> {
//     let page_id = {
//         let meta_guard = meta_store.table_meta_store.read().unwrap();
//         let latest_page_meta = meta_guard.get_latest_page_meta(col).unwrap();
//         latest_page_meta.id.clone()
//     };
//     let entry = Entry::new(data);

//     // check if page in uncompressed cache, if yes, just update and flush 
//     let in_uncompressed = { let guard = uncompressed_cache.page_cache.read().unwrap(); guard.has(&page_id) };
//     if in_uncompressed {
//         let mut guard = uncompressed_cache.page_cache.write().unwrap();
//         guard.get(&page_id).unwrap().page.page.add_entry(entry);
//         let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();
//         return Ok(ok)
//     }
    
//     // check if page in compressed cache, if yes, decompress and put into compressed cache and do the above
//     let in_compressed = { let guard = compressed_cache.page_cache.read().unwrap(); guard.has(&page_id) };
//     if in_compressed {
//         compressor.decompress_from_cache(meta_store, compressed_cache, uncompressed_cache, &page_id);
//         // fetches from compressed cache ,decompresses, inserts in the uncompressed page cache, then does the sames as above
//         let mut guard = uncompressed_cache.page_cache.write().unwrap();
//         guard.get(&page_id).unwrap().page.page.add_entry(entry);
//         let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();
//         return Ok(ok)
//     }

//     // fetches from disk, then does the same as above
//     compressor.decompress(meta_store, compressed_cache, &page_id);
//     compressor.decompress_from_cache(meta_store, compressed_cache, uncompressed_cache, &page_id);
//     let mut guard = uncompressed_cache.page_cache.write().unwrap();
//     guard.get(&page_id).unwrap().page.page.add_entry(entry);
//     drop(guard);
//     let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();

//     Ok(ok)


// }

// // I hope the below stuff is correct, im fried rn
// fn update_column_entry(_entry: Entry, compressor: Compressor, meta_store: &TableMetaStoreWrapper, compressed_cache: &PageCacheWrapper<PageCacheEntryCompressed>, uncompressed_cache: &PageCacheWrapper<PageCacheEntryUncompressed>, col: &str, data: &str, row: u64) -> Result<bool, Box<dyn std::error::Error>> {
//     let page_id = {
//         let meta_guard = meta_store.table_meta_store.read().unwrap();
//         let latest_page_meta = meta_guard.get_latest_page_meta(col).unwrap();
//         latest_page_meta.id.clone()
//     };
//     let entry = Entry::new(data);

//     // check if page in uncompressed cache, if yes, just update and flush 
//     let in_uncompressed = { let guard = uncompressed_cache.page_cache.read().unwrap(); guard.has(&page_id) };
//     if in_uncompressed {
//         let mut guard = uncompressed_cache.page_cache.write().unwrap();
//         guard.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
//         let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();
//         return Ok(ok)
//     }
    
//     // check if page in compressed cache, if yes, decompress and put into compressed cache and do the above
//     let in_compressed = { let guard = compressed_cache.page_cache.read().unwrap(); guard.has(&page_id) };
//     if in_compressed {
//         compressor.decompress_from_cache(meta_store, compressed_cache, uncompressed_cache, &page_id);
//         // fetches from compressed cache ,decompresses, inserts in the uncompressed page cache, then does the sames as above
//         let mut guard = uncompressed_cache.page_cache.write().unwrap();
//         guard.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
//         let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();
//         return Ok(ok)
//     }

//     // fetches from disk, then does the same as above
//     compressor.decompress(meta_store, compressed_cache, &page_id);
//     compressor.decompress_from_cache(meta_store, compressed_cache, uncompressed_cache, &page_id);
//     let mut guard = uncompressed_cache.page_cache.write().unwrap();
//     guard.get(&page_id).unwrap().page.page.entries[row as usize] = entry;
//     drop(guard);
//     let ok = compressor.compress(meta_store, uncompressed_cache, &page_id).unwrap();

//     Ok(ok)


// }

// fn read_single_column_entry(col: String, row: u64) {

// }

// fn range_scan_column_entry(meta_store: &TableMetaStoreWrapper, col: String, l_row: u64,r_row: u64) {
//     /*
//     quickly find the internal (l,r) groupings and the latest pages needed for it and put an lock on them so they dont perish
    

//     query them parallely fast 

//     holy fuck, we need some sort of lock thingy per column lol

//     yep, something... like, the thing is , our (l,r) keeper thingy isnt really MVCCing and I dont really want it to be that way

//     once we know that "nothing will change in between me figuring out the bounds", we can figure things out with one binary search

//     soo... figure out the bounds, and get their latest page IDs 

//     okay, so thinking about it, our Pages are kinda versioned nicely because yesterday's nubskr was kind to us

//     so we don't need to worry about that... 

//     so.. just figure out how to get those (l,r) bounds in one shot and call it a day

//     also note that you cant just lock stuff as that would involve "waiting" into the game which we dont want


//     okay, so for now we just take a read lock and get an immutable snapshot of the data, can we do that ??

//     umm... just get a read lock, run a fast binary search, get the index bounds of stuff you need and just grab the latest page metas of them, that's fucking it

//     I mean, we can kinda just.... clone after taking a read lock and never have to do a call after that ever again for this request

//     okay, so we kinda need a very very fast way to get the latest page metas for a contigious group of (l,r) bounds

//     okay, whateer, lets keep it simple, make all that internal stuff be done my metadata store, and it would return us with just relevant page_ids

//     and after that we would be done with it and just deal with page cache store to actually get the stuffs
//     */
    

//     /*
//     okay, modern day nubskr here, the above one was my peanut brained counterpart;
//     we:
//     - take a read lock on meta store, clone stuff maybe and remove lock afap, idk, how big can... oh wait, its just a columnar stuff huh.. interesting, might be feasible
//     - quick binary search to find (l,r) grouping bounds
//     - then quickly grab the latest page meta for each of those and RELEASE THE FUCKING RLOCK, cloning and immediately releasing seems more appropriate tbh, writes would go brrrr and we would have natural in-memory MVCC, like when one meta for some column is cloned for some read, a lot of those reads can be done in parallel, and there would be minimal contention for writes, we can also ensure that the whole query only sees a "consistent" state of the whole thing during its lifetime, oh wait, we would need to.. kinda syncronize this across columns, like, we can parallize this, hmm, its a Map<col_name,Arc<RWLock<outShit>>> bro, ughhh, wait arent we using a wrapper over it anyway ??? yeah ig, yeah, so that does it then ig, hmm
//      */

//     // so we use get_ranged_pages_meta for all the metas, then pass it along to page_cache to fetch us stuff
//     let query_start_time = current_epoch_millis();
//     let relevant_page_metas = &meta_store.table_meta_store.read().unwrap().get_ranged_pages_meta(&col, l_row, r_row, query_start_time).unwrap();


//     // now we gotta fetch these pages from page_cache, btw our page cache is pretty dumb and wont fetch stuff from disk itself




// }

// // this does the whole darn query for multiple columns, note that we need syncronization here
// fn range_scan_columns_entries() {
//     // can we just parallelize the above function by just calling it for multiple columns as they are independent ?? AHAHAHAHAHAHAH idk how, threads are sparse
//     // we need a thread pool scheduling thingy good ser
// }