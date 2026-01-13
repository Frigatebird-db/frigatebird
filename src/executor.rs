use crate::pipeline::Job;
use crate::pool::scheduler::ThreadPool;
use crossbeam::channel::{self, Receiver, Sender};
use crossbeam_skiplist::SkipSet;
use std::cmp::Ordering;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};

/// Executes Jobs across dedicated worker pools, ensuring high-cost jobs
/// can be picked up by reserve workers when main workers signal pressure.
pub struct PipelineExecutor {
    job_tx: Sender<Arc<Job>>,
    #[allow(dead_code)]
    wake_tx: Sender<()>,
    #[allow(dead_code)]
    job_board: Arc<SkipSet<JobHandle>>,
    #[allow(dead_code)]
    seq: Arc<AtomicU64>,
    #[allow(dead_code)]
    main_pool: ThreadPool,
    #[allow(dead_code)]
    reserve_pool: ThreadPool,
}

impl PipelineExecutor {
    /// Build an executor with the given total number of threads.
    pub fn new(total_threads: usize) -> Self {
        let total = total_threads.max(2);
        let (main_threads, reserve_threads) = split_threads(total);

        let (job_tx, job_rx) = channel::unbounded::<Arc<Job>>();
        let (wake_tx, wake_rx) = channel::unbounded::<()>();
        let job_board = Arc::new(SkipSet::new());
        let seq = Arc::new(AtomicU64::new(0));

        let main_pool = ThreadPool::new(main_threads);
        for _ in 0..main_threads {
            let rx = job_rx.clone();
            let board = Arc::clone(&job_board);
            let seq_clone = Arc::clone(&seq);
            let wake_clone = wake_tx.clone();
            main_pool.execute(move || run_main_worker(rx, board, seq_clone, wake_clone));
        }

        let reserve_pool = ThreadPool::new(reserve_threads);
        for _ in 0..reserve_threads {
            let rx = wake_rx.clone();
            let board = Arc::clone(&job_board);
            reserve_pool.execute(move || run_reserve_worker(rx, board));
        }

        PipelineExecutor {
            job_tx,
            wake_tx,
            job_board,
            seq,
            main_pool,
            reserve_pool,
        }
    }

    /// Submit a job to be processed by the executor.
    pub fn submit(&self, job: Job) {
        // Best effort: if receiver side dropped we treat it as executor shutdown.
        let _ = self.job_tx.send(Arc::new(job));
    }
}

fn run_main_worker(
    job_rx: Receiver<Arc<Job>>,
    board: Arc<SkipSet<JobHandle>>,
    seq: Arc<AtomicU64>,
    wake_tx: Sender<()>,
) {
    while let Ok(job) = job_rx.recv() {
        let handle = JobHandle::new(Arc::clone(&job), seq.fetch_add(1, AtomicOrdering::Relaxed));
        board.insert(handle);
        let _ = wake_tx.send(());
        job.get_next();
    }
}

fn run_reserve_worker(wake_rx: Receiver<()>, board: Arc<SkipSet<JobHandle>>) {
    while wake_rx.recv().is_ok() {
        if let Some(job) = select_heaviest(&board) {
            job.get_next();
        }
    }
}

fn select_heaviest(board: &SkipSet<JobHandle>) -> Option<Arc<Job>> {
    board
        .pop_back()
        .map(|entry| entry.value().clone().into_job())
}

fn split_threads(total: usize) -> (usize, usize) {
    let mut main = (total * 85 + 99) / 100; // round up
    if main == 0 {
        main = 1;
    }
    if main >= total {
        main = total.saturating_sub(1);
    }
    let mut reserve = total.saturating_sub(main);
    if reserve == 0 {
        reserve = 1;
        if main > 1 {
            main -= 1;
        }
    }
    (main, reserve)
}

#[derive(Clone)]
struct JobHandle {
    seq: u64,
    job: Arc<Job>,
}

impl JobHandle {
    fn new(job: Arc<Job>, seq: u64) -> Self {
        JobHandle { seq, job }
    }

    fn into_job(self) -> Arc<Job> {
        self.job
    }

    fn cost(&self) -> usize {
        self.job.cost
    }
}

impl PartialEq for JobHandle {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && self.cost() == other.cost()
    }
}

impl Eq for JobHandle {}

impl PartialOrd for JobHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JobHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.cost().cmp(&other.cost()) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::page_cache::{
        PageCache, PageCacheEntryCompressed, PageCacheEntryUncompressed,
    };
    use crate::helpers::compressor::Compressor;
    use crate::metadata_store::PageDirectory;
    use crate::page_handler::page_io::PageIO;
    use crate::page_handler::{PageFetcher, PageHandler, PageLocator, PageMaterializer};
    use crate::pipeline::{Job, PipelineBatch, PipelineStep};
    use crossbeam::channel;
    use std::sync::{Arc, RwLock};

    fn create_mock_page_handler() -> Arc<PageHandler> {
        let meta_store = Arc::new(RwLock::new(crate::metadata_store::TableMetaStore::new()));
        let directory = Arc::new(PageDirectory::new(meta_store));
        let locator = Arc::new(PageLocator::new(Arc::clone(&directory)));

        let compressed_cache = Arc::new(RwLock::new(PageCache::<PageCacheEntryCompressed>::new()));
        let page_io = Arc::new(PageIO {});
        let fetcher = Arc::new(PageFetcher::new(compressed_cache, page_io));

        let uncompressed_cache =
            Arc::new(RwLock::new(PageCache::<PageCacheEntryUncompressed>::new()));
        let compressor = Arc::new(Compressor::new());
        let materializer = Arc::new(PageMaterializer::new(uncompressed_cache, compressor));

        Arc::new(PageHandler::new(locator, fetcher, materializer))
    }

    fn dummy_job(step_count: usize) -> Job {
        let (entry_tx, _entry_rx) = channel::unbounded::<PipelineBatch>();
        let mut steps = Vec::with_capacity(step_count);
        let page_handler = create_mock_page_handler();
        for idx in 0..step_count {
            let (tx, rx) = channel::unbounded::<PipelineBatch>();
            steps.push(PipelineStep::new(
                "test_table".to_string(),
                format!("c{idx}"),
                idx,
                Vec::new(),
                idx == 0,
                Arc::clone(&page_handler),
                tx,
                rx,
            ));
        }
        let (_out_tx, out_rx) = channel::unbounded::<PipelineBatch>();
        Job::new("t".into(), steps, entry_tx, out_rx)
    }

    #[test]
    fn job_handles_sort_by_cost() {
        let low = Arc::new(dummy_job(1));
        let high = Arc::new(dummy_job(3));
        let handle_low = JobHandle::new(low, 0);
        let handle_high = JobHandle::new(high, 1);
        assert!(handle_high > handle_low);
    }
}
