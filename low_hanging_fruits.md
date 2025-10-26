so we need some basic modules, a sql parser and a basic query planner (a dumb one for now)

## sql parser

we can use sqlparser crate for that

## query planner

well, this is a more complicated one, for now, let's just make it so that we know what table(s) the query wants to operate now and for each table in there we want to know which column the query wants to operate on and which columns it wants to write on and which to just read ; forget joins for now,

now that we have the above stuff, we need:

Pipeline builder