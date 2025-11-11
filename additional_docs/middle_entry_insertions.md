# middle entry insertions

AHAHAHHAHAAH, I HAVE TRULY OUTDONE MYSELF THIS TIME, HOLY FUCKING SHIT

hear me out, 

## Problem:

In the current state, assume we have to insert some new row entry(s) somewhere in the middle(non appends) for some column, we would need to accomodate it in some previous (l,r) and then update that (l,r) entry meta to adjust bounds AND for each (l,r) entry after this entry till the very end, we would need to update it too to reflect the changes, THIS WOULD KILL PERFORMANCE as a write lock that big would fuck it all up


## Solution:

We get rid of (l,r) at all, so currently we have:

```
[                    |
                                    | 
                    (l0,r0) -> [Page_id_at_x < Page_id_at_y < Page_id_at_z....],
                    (l1,r1) -> ...
                    (l2,r2)
                    (l3,r3)
                    ...
                    ...
                ] 
```

we just make it:

```
[                    |
                                    | 
                    [Page_id_at_x < Page_id_at_y < Page_id_at_z....], // this is one entry
                    ... // this is another
                    ... // ...
                    ...
                    ...
                    ... // lets call this `n` for now
                ] 
```

and introduce a new Prefix sum array with same size as `n` in col_meta and it would be the prefix sum of the no. of entries till point i, more formally: 

```
entry_psum[i] = no. of entries till now
```

but... now you might ask: "but even now WONT we have the same problem of having to add something each entry after we insert some new row entry ??" yes, you are right. BUT, with decoupling a new integer array for psum, we can use SIMD vectorisation to... ABUSE our single threaded col operation to range update directly to the very end of the entry_psum array from some i where we add that entry AHAHAHAHAHAHAH

and even with this we can still make our `get_ranged_pages_meta` work with binary search over that prefix sum array with minimal/negligible performance affect, AHAHAHAHQHAHHA, holy fuck nubskr, you piece of work