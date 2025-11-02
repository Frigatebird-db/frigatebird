we need to add table stuff which is totally non existent right now, basically we need to support create table queries,

for that in the meta store we would need to add a table meta part too which just keeps track of what tables we have and what columns those tables have and what order to sort them in,

something like:

CREATE TABLE items (
    id      UUID,
    name    String,
    created DateTime
)
ORDER BY (id,created);

this is a performance critical part

how can we go on about doing that minimally, wdyt

before making any changes, please tell me how we can do that

