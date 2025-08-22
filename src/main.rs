use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Seek, SeekFrom, Write};
mod entry_keeper;
mod page_io;
mod table_metadata_store;
mod ops_handler;
mod page_cache;
mod page;
mod context;

// makes a new page and returns its offset
fn make_new_page_at_EOF() -> io::Result<(u64)>{
    // make a new page at EOF
    
    // return the offset
    Ok((69))
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






























