use crate::pipeline::Job;
use crate::pool::scheduler::ThreadPool;
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::Arc;

/// Executes Jobs across dedicated worker pools, ensuring high-cost jobs
/// can be picked up by reserve workers when main workers signal pressure.
pub struct PipelineExecutor {
    job_tx: Sender<Arc<Job>>,
    #[allow(dead_code)]
    pool: ThreadPool,
}

impl PipelineExecutor {
    /// Build an executor with the given total number of threads.
    pub fn new(total_threads: usize) -> Self {
        let total = total_threads.max(2);
        let (job_tx, job_rx) = channel::unbounded::<Arc<Job>>();
        let pool = ThreadPool::new(total);
        for _ in 0..total {
            let rx = job_rx.clone();
            pool.execute(move || run_worker(rx));
        }

        PipelineExecutor {
            job_tx,
            pool,
        }
    }

    /// Submit a job to be processed by the executor.
    pub fn submit(&self, job: Job) {
        let job = Arc::new(job);
        let step_count = job.steps.len();
        if step_count == 0 {
            return;
        }
        for _ in 0..step_count {
            // Best effort: if receiver side dropped we treat it as executor shutdown.
            let _ = self.job_tx.send(Arc::clone(&job));
        }
    }
}

fn run_worker(job_rx: Receiver<Arc<Job>>) {
    while let Ok(job) = job_rx.recv() {
        job.get_next();
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

    #[test]
    fn pipeline_executor_drains_steps() {
        let page_handler = create_mock_page_handler();
        let (entry_tx, entry_rx) = channel::unbounded::<PipelineBatch>();
        let (out_tx, out_rx) = channel::unbounded::<PipelineBatch>();
        let step = PipelineStep::new(
            "test_table".to_string(),
            "c0".to_string(),
            0,
            Vec::new(),
            true,
            page_handler,
            out_tx,
            entry_rx,
            None,
        );
        let job = Job::new("t".into(), vec![step], entry_tx, out_rx.clone());

        let executor = PipelineExecutor::new(2);
        executor.submit(job);

        let batch = out_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("missing termination batch");
        assert_eq!(batch.num_rows, 0);
    }
}
