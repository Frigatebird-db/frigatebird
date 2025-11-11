okay, this works and is good enough for now, works for v0.1.0


so, I want to add a new module, called Writer, basically, it sits at the end of pipeline executor, 


I want to add a writer module at the end,something which gets the rows to affect after all filers are applied and works for the write operations, so what that thing does is, it figures out which page is affected for the rows we need to change, gets its metadata from the meta store, fetches the actual page(s) and makes updates on them, and then makes a new page out of them(we need a page allocator ; make a dummy for now which just returns some random ass file path and offset and buffer for now) and make a new version of the page and update the meta store for it

now the thing is, we cant just let the darn pipeline threads update the meta store because the fucking verions need to be maintained and serialized, imagine two queries making updates to same pages, that shit would need to be serialized so that we dont fuck up ordering of updates, so to counter this,

let's call each update a query has to do a 'Update Job', an job can have multiple blocks to be updated for different columns

we can make a separate module call Writer, it will have its own thread pool and it gets these `Update Jobs` and it does these things one by one for now, like, take a job, read meta store, fetch pages, update them, make new pages out of them and update the meta store

this ensures that there are no torn writes that readers see because there is only one thread doing the update part, and this would also ensure that the updates on same shit are written in order and each update actually sees the updated column which the last job updated
