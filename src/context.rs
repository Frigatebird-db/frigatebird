use crate::page_cache::CombinedCache;
use crate::page_io::IOHandler;

pub struct Context {
    pub cache: CombinedCache,
    pub io_handler: IOHandler,
}