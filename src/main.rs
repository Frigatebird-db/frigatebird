use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
mod entry_keeper;
struct TableMetaStore {
    data: HashMap<String,Vec<u64>>
}

impl TableMetaStore {
    fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    fn get_current_page(&self, column: &str) -> Option<u64> {
        self.data.get(column)?.last().copied()

    }
}

struct Page {
    page_metadata: String,
    entries: Vec<entry_keeper::Entry>,
}

impl Page {
    fn new() -> Self {
        Self {
            page_metadata: "".to_string(),
            entries: vec![],
        }
    }

    fn add_entry(&mut self, entry: entry_keeper::Entry) {
        // what does it means to add an entry in a page
        self.entries.push(entry);

        // now its done in memory, we need to do it on disk as well
    }
}


// makes a new page and returns its offset
fn make_new_page_at_EOF() -> io::Result<(u64)>{
    // make a new page at EOF
    
    // return the offset
    Ok((69))
}

fn append_to_column(tableMetaStore: TableMetaStore, column: &str, data: &str) -> io::Result<()> {
    // find out the current page from table meta store
    let pageOffset = tableMetaStore.get_current_page(column);
    
    // create entry


    // check if that page has enough space to accomodate the entry

    // if not, create a new page

    Ok(())
}

fn do_shit_to_file(data: &[u8], fd: &mut File) -> io::Result<String> {
    // append data to that file dumbly
    
    // seek to the bottom of the file
    fd.seek(SeekFrom::End(0))?;
    fd.write_all(data)?;

    // append data to it
    Ok("UwU".to_string())

}

fn main() -> io::Result<()> {
    println!("Hello, world!");

    let mut fd = File::options().read(true).write(true).open("./innocent_file.txt")?;
    let data = b"hii";

    print!("{}", do_shit_to_file(data,&mut fd)?);

    Ok(())
}

/*
just write stuff to a file

just make a function which does this:

takes a file descriptor
appends shit to the file it belongs to, that's it


*/






























