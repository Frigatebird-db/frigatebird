use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const ALIGN_4K: u64 = 4096;
const BLOCK_SIZE: u64 = 256 * 1024;
const FILE_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DATA_DIR: &str = "storage";

/// Describes a new physical location for a rewritten page.
#[derive(Debug, Clone)]
pub struct PageAllocation {
    #[allow(dead_code)]
    pub file_id: u32,
    pub path: String,
    pub offset: u64,
    #[allow(dead_code)]
    pub actual_len: u64,
    pub alloc_len: u64,
}

/// Abstraction over page allocation so we can upgrade the allocator later.
pub trait PageAllocator: Send + Sync {
    fn allocate(&self, actual_len: u64) -> io::Result<PageAllocation>;
}

struct FileState {
    file_id: u32,
    offset: u64,
    file: File,
    max_size: u64,
    data_dir: String,
}

impl FileState {
    fn open(file_id: u32, data_dir: String) -> io::Result<Self> {
        let file = open_data_file(file_id, &data_dir)?;
        Ok(FileState {
            file_id,
            offset: 0,
            file,
            max_size: FILE_MAX_BYTES,
            data_dir,
        })
    }

    fn ensure_capacity(&mut self, size: u64) -> io::Result<()> {
        if self.offset + size <= self.max_size {
            return Ok(());
        }

        self.file_id += 1;
        self.file = open_data_file(self.file_id, &self.data_dir)?;
        self.offset = 0;
        Ok(())
    }

    fn allocate(&mut self, actual_len: u64, alloc_len: u64) -> io::Result<PageAllocation> {
        self.ensure_capacity(alloc_len)?;
        let allocation = PageAllocation {
            file_id: self.file_id,
            path: data_file_path(self.file_id, &self.data_dir)
                .to_string_lossy()
                .to_string(),
            offset: self.offset,
            actual_len,
            alloc_len,
        };
        self.offset += alloc_len;
        Ok(allocation)
    }
}

fn data_file_path(file_id: u32, data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(format!("data.{file_id:05}"))
}

fn open_data_file(file_id: u32, data_dir: &str) -> io::Result<File> {
    fs::create_dir_all(data_dir)?;
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    #[cfg(target_os = "linux")]
    {
        opts.custom_flags(libc::O_DIRECT);
    }
    opts.open(data_file_path(file_id, data_dir))
}

fn round_up_4k(len: u64) -> u64 {
    (len + (ALIGN_4K - 1)) & !(ALIGN_4K - 1)
}

fn compute_alloc_len(actual_len: u64) -> u64 {
    if actual_len == 0 {
        return ALIGN_4K;
    }
    let full_blocks = actual_len / BLOCK_SIZE;
    let tail = actual_len % BLOCK_SIZE;

    let mut alloc_len = full_blocks * BLOCK_SIZE;
    if tail > 0 {
        alloc_len += round_up_4k(tail);
    }
    alloc_len
}

/// Direct I/O friendly block allocator.
pub struct DirectBlockAllocator {
    state: Mutex<FileState>,
}

impl DirectBlockAllocator {
    pub fn new() -> io::Result<Self> {
        Self::with_data_dir(DATA_DIR)
    }

    pub fn with_data_dir(data_dir: impl Into<String>) -> io::Result<Self> {
        if !BLOCK_SIZE.is_multiple_of(ALIGN_4K) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BLOCK_SIZE must be 4K aligned",
            ));
        }
        let state = FileState::open(0, data_dir.into())?;
        Ok(DirectBlockAllocator {
            state: Mutex::new(state),
        })
    }
}

impl PageAllocator for DirectBlockAllocator {
    fn allocate(&self, actual_len: u64) -> io::Result<PageAllocation> {
        let mut guard = self.state.lock().unwrap();
        let alloc_len = compute_alloc_len(actual_len);
        guard.allocate(actual_len, alloc_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn state_with(max_size: u64, offset: u64) -> (FileState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_string_lossy().to_string();
        let file = open_data_file(0, &data_dir).unwrap();
        let state = FileState {
            file_id: 0,
            offset,
            file,
            max_size,
            data_dir,
        };
        (state, temp_dir)
    }

    #[test]
    fn round_up_4k_aligns_lengths() {
        assert_eq!(round_up_4k(0), 0);
        assert_eq!(round_up_4k(ALIGN_4K - 1), ALIGN_4K);
        assert_eq!(round_up_4k(ALIGN_4K), ALIGN_4K);
        assert_eq!(round_up_4k(ALIGN_4K + 1), ALIGN_4K * 2);
    }

    #[test]
    fn compute_alloc_len_handles_full_blocks_and_tail() {
        assert_eq!(compute_alloc_len(0), ALIGN_4K);
        assert_eq!(compute_alloc_len(BLOCK_SIZE), BLOCK_SIZE);
        assert_eq!(compute_alloc_len(BLOCK_SIZE + 1), BLOCK_SIZE + ALIGN_4K);
        assert_eq!(
            compute_alloc_len(BLOCK_SIZE * 3 + 113_568),
            BLOCK_SIZE * 3 + round_up_4k(113_568)
        );
    }

    #[test]
    fn file_state_allocates_monotonically() {
        let (mut state, _tmp_dir) = state_with(BLOCK_SIZE * 2, 0);

        let first = state.allocate(BLOCK_SIZE, BLOCK_SIZE).unwrap();
        assert_eq!(first.file_id, 0);
        assert_eq!(first.offset, 0);
        assert_eq!(state.offset, BLOCK_SIZE);

        let tail = round_up_4k(ALIGN_4K + 128);
        let second = state.allocate(ALIGN_4K + 128, tail).unwrap();
        assert_eq!(second.file_id, 0);
        assert_eq!(second.offset, BLOCK_SIZE);
        assert_eq!(state.offset, BLOCK_SIZE + tail);
    }

    #[test]
    fn file_state_rotates_when_full() {
        let (mut state, _tmp_dir) = state_with(BLOCK_SIZE, BLOCK_SIZE - ALIGN_4K);

        let tail = round_up_4k(ALIGN_4K);
        let alloc = state.allocate(ALIGN_4K, tail).unwrap();
        assert_eq!(alloc.file_id, 0);

        let next_tail = round_up_4k(1024);
        let next = state.allocate(1024, next_tail).unwrap();
        assert_eq!(next.file_id, 1);
        assert_eq!(next.offset, 0);
    }
}
