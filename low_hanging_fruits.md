so we need some basic modules, a sql parser and a basic query planner (a dumb one for now)

## sql parser

we can use sqlparser crate for that

## query planner

well, this is a more complicated one, for now, let's just make it so that we know what table(s) the query wants to operate now and for each table in there we want to know which column the query wants to operate on and which columns it wants to write on and which to just read ; forget joins for now,

now that we have the above stuff, we need:

## Pipeline builder

this guy gets the planned query from query planner and makes a pipeline, basically, for now it just:
- make a step by step thingy on how to execute the filtering part of the plan, so if we have to apply some filters to a column in a table, we break it into steps, for example:

step 1: [apply filters to column X, check all rows]
step 2: [from the rows that passed the filters from step 1, apply the filters on them for column Y] 
Repeat step 2 untill all filters have been applied

for now, let's just make it random, like we just randomly make steps from the query plan, no optimizations about which filter should come first as of now

---

okay, now we need the darn Executor, holy shit man 


## changes to be made in Job object:

add a usize called 'cost' in Job which is just the size of the PipelineSteps in Job
add PartialOrd for Job by cost
add an public method to Job called execute(), for now, just put an infinite loop there

## Pipeline executor

make a new module named executor for this in src/

okay, so now let's talk about pipeline executor, this thing uses the worker pool defined in /src/pool/scheduler.rs

so what we need is:


two worker pools:

1. Main workers
2. Reserver workers

pipeline executor gets initialized with N threads, it gives 85 percent of its threads to Main workers and 15 percent of its threads to Reserve workers

so each main worker gets a crossbeam MPMC channel receiver through which it receives a Job object

we also have something called a `JobBoard` in executor, it's essentially just a crossbeam_skiplist set

so when a main worker consumes a Job, it:
1. inserts the Job in JobBoard
2. publishes an lightweight wakeupcall(just a bool) to an MPMC producer whose receiver the Reserve worker holds
3. calls the get_next() on Job

Reserve worker blocks on that receiver, when it gets the wakeup call from the producer, it:
1. checks the JobBoard(which is sorted by Job cost) and looks at the most expensive job, and removes it from there and calls get_next() on it
(reserve workes help the main workers in heavy jobs, its intended)