lets do the dumbest thing for now

this shit gets some query plan and needs to execute it, thats all


query plan tells which column to scan from what table and what filter to apply to it

[query planner] -> [executor] -> result

what do we need at the bare minimum to do that

we definitely need a bunch of workers, note that the shit here is both I/O(fetching the blocks from disk) and CPU(decompressing and doing range scan over those shits) bound,

so we cant just use a pure thread pool in that sense as it would keep waiting blocked on IO causing starvation

okay, so the query planner would give us some orderings of operations, we just have to follow that as fast as possible

basically, it would be like:

## step 1 -> scan column X from (l,r) rows, put filter A on it, the shits which pass let them go to the next step
## step 2 -> take the rows being trickled down from the above step, for those rows, scan column Y from (l,r) rows and put filter B on it, trickle the results down
## repeat step 2 with different columns and filters

that's all it is

so we would need a multistage pipeline builder for this

basically we just give the darn pipeline some count of steps and it would just handle shit for us

so, talking for a single query, we have one pipeline and a bunch of workers working on it, I still dont know what the darn workers are here

all the pipelines use the same set of caches here, we need to make sure that it doesnt becomes the bottleneck, but that's a problem from another day, today is all about building the machine which builds the pipelines and makes water flow through them

we need to ensure the executor never touches the darn meta store


what would the worker pool look like man, we need something basic, something which just:
- parks a worker when its blocked
- pull it back up when its back

fuck tokio, thread pool all the way, we use crossbeam channels as they're faster as verified by load testing