this shit is hella slow, absolutely shit

we need to build an abstraction around strings so we can use char arrays, get rid of `chasing the pointer` issues and make things better for vectorization, basically what I want is, currently in the pipeline


we only go one step at a time, this is too slow, we need to make it faster, man, I am losing my motivation for this not gonna lie :((

we should stop dealing with single rows and start dealing with row groups, the thing is, each column Page has its own 


some fucking lessons:
- get rid of pointer chasing, make shit in one Page actually sit next to each other instead of having a pointer to another struct
- represent strings as char arrays, strings are slow


some of the biggest things:
- we really need to change the Page structure, basically

right now I have column based sized based pages, each column has its own variable sized pages basically

 Table X
|--------|
| A  B  C|
||| || |||
||| || |||
||| || |||
||| || |||
||| || |||
|--------|
| ...    |
|________|

we can have Page wise statistics so that query optimizer can do stuff, this way

nice,

man, we need to do a rearchitecture :((

holy fuck lmao

but the thing is, this is a lot easier to reason about, and that is what I need, I can understand thigns very well now, perfecto


so the thing is


on a bigger level it would look like this:

Table A
[Row Chunk 1] -> sizes can be variable, row chunk has nothing but pointers to columnar stuff, that's all it is
[Row Chunk 2]  and columnar stuff is actually columnar, like literally right next to each other, no heap allocated
[Row Chunk 3]  pointer chasing bullshit, one cache line, one group of rows for a column , that's it

		we will use char arrays instead of Strings, 


absolutely legendary, this way we will only need one prefix sum per table which is a relief as hell



how would mvcc work here, mvcc can work exactly the way it has worked before :)) yeah


---


the important question is, how would the pipeline work now, so current what we have is:

[filters on column1] - [filters on column2] - [] - ...


so the thing is, we can prefetch more aggressively here





I can build small stuff and then scale out from that, yeah, small things, benchmark them heavily, I mean, think about it

what the hell do I need to get a proof of concept, once I have something small which I know works, I can scale out to a lot more too, 

so what I need is:

- a small char array thingy which can represent strings
- skip pagewise stats, the thing which we can do with it is... we can prefetch pages and avoid busy work

[page] -> [page metadata]

all attached to one thing, all fit in one cache line, zero pointer chasing bullshit

do things to the 

; this makes a lot of sense, yeah.. this can make my system a lot better

I also need to kinda ensure that ingestion(both appends and random ass inserts) are fast as hell, a tiny system can help with this..

yeah.. hell yeah

imma destory everything that exists, mog every last thing

makes sense, yeah, a lot of sense, i can see it, destory everything


a small proof of concept which is crazy fast, things which matter right now:

- scanning shit
- vectorization shit
- pure appends
- random ass insertions

take the current executor and make it better, its a baby already in a way
