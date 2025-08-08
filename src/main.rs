use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

fn do_shit_to_file(data: &[u8], fd: &mut File) -> io::Result<()> {
    // append data to that file dumbly
    
    // seek to the bottom of the file
    fd.seek(SeekFrom::End(0))?;
    fd.write_all(data)?;

    // append data to it
    Ok(())

}

fn main() -> io::Result<()> {
    println!("Hello, world!");

    let mut fd = File::options().read(true).write(true).open("./innocent_file.txt")?;
    let data = b"hii";

    do_shit_to_file(data,&mut fd)?;

    Ok(())
}

/*
just write stuff to a file

just make a function which does this:

takes a file descriptor
appends shit to the file it belongs to, that's it


*/