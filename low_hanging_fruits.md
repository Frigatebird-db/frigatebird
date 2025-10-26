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