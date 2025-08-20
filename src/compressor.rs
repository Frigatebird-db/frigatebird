// keep shit compressed

// handle all compression and decompression or whatever

// reads a bunch of fucking entries from somewhere and compresses them and decompresses when asked to

struct Compressor {

}

impl Compressor {
    fn new() -> Self {
        Compressor {}
    }

    fn compress(path: String, offset: u64) -> Result<bool>{
        // compress and write at this path
        Ok(true)
    }

    fn decompress(path: String, offset: u64) {
        // read from this path,decompress and return the decompressed Page
        let compressed_data = read_from_path(path, offset);
        let decompressed_data = decompress_data(compressed_data);
        return decompressed_data;
    }
}