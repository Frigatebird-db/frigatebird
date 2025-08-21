/*
how the hell do we apply updates to a page

prereqs:
- it needs to be in cache
- the update patch needs to be available, how ??
    - what if the patch is simply bigger, 
        - like, can we somemhow manage a bit uneven size ? 



so what are the things like right now ?

first of all, cache heirarchies need to be present

like, if we have a page decompressed in PageCache , when it needs to be evicted, we would need to evict all its entries:
like: a Page is one thing, its entries is(are) multiple things

the moment a page is decompressed, we need to evict its compressed version from the `Compressed Page Cache`

Page Cache:
[Compressed Page Cache]
[Decompressed Page Cache]

Any cache in general should be:
- able to quickly check if a Page(whether compressed or uncompressed) is in memory or not

we can kinda use compressed Page references with the metadata store
*/