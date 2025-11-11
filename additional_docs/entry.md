Entry:

{[prefix meta] -- [actual data] -- [suffix meta]}
ps: seriously dont remember why we need suffix meta btw, 
was it for reverse iteration ? its skipped in the implementation
for now


add something to a column ->
    check if that column exists from the table metadata ->
        if it doesnt, create a new page and insert the data in there
        if it does, find out from the table metadata itself in which page is the latest column entry kept

worry only about adding new entries to columns now, like a timeful append only KV store



create column -> add a new page at the end of file 
add something to a column -> find the current page for that column from the table metadata store and 