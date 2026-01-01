# Project 42


## Cache(s) ? 

we have two layers of caches

UPC (Uncompressed Page Cache):

    Hot Page structs for mutations/reads
    Eviction → compress → insert into CPC

CPC (Compressed Page Cache):

    Compressed blobs ready for disk
    Eviction → lookup via PageDirectory → flush to disk


so the general path of some fetch operation looks like:

Request page "p42"
    │
    ├─ UPC (HashMap) hit?
    │      └─▶ YES ──▶ return Arc<PageCacheEntryUncompressed>
    │
    ├─ CPC hit?
    │      ├─▶ YES ──▶ decompress
    │      │          └─▶ insert into UPC
    │      │              └─▶ return Arc<PageCacheEntryUncompressed>
    │
    └─ Fetch from disk
            │
            ├─▶ PageIO::read_from_path(path, offset)
            ├─▶ copy into compressed bytes into CPC
            └─▶ decompress + seed UPC
                └─▶ return Arc<PageCacheEntryUncompressed>


## Scheduler

