okay, so since we allow order by while creating a new table, this adds a new complexity when updating or inserting a row in the Writer step, check in @src/ currently for any page, we only support in place update and appending stuff to a page

the thing is, this works fine for most columns, but if we were to insert or update an entry for the ORDER BY column for that table, we would need to actually move that entry to a new place too, 

this adds a lot of complications, the entry might be needed to move to a completely different page... or would need rearrangement within the current page, this is tricky as fuck

I think we need a darn comparator in such scenarios, something which realizes that ordery by column is being modified for some table and then for the entries which are to be affected (basically the resulting rows after the filtering steps) , it would kinda need to figure out where to put them in the table AND also delete from their original position

I think once we have this one settled, everything is just a breeze

this way we could also add support for the DELETE sql stuff lol
