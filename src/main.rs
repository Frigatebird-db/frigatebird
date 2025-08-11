use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

struct Entry {
    prefix_meta: String,
    data: String,
    suffix_meta: String,
}

impl Entry {
    fn new() -> Self {
        Self {
            prefix_meta: "".to_string(),
            data: "dummy".to_string(),
            suffix_meta: "".to_string(),
        }
    }
}

struct Page {
    page_metadata: String,
    entries: Vec<Entry>,
}

impl Page {
    fn new() -> Self {
        Self {
            page_metadata: "".to_string(),
            entries: vec![],
        }
    }

    fn add_entry() {
        // hmm, what now
    }
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