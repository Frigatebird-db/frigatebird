use super::batch::ColumnarBatch;
use std::io;

pub struct SpillManager {
    in_memory_runs: Vec<ColumnarBatch>,
}

impl SpillManager {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            in_memory_runs: Vec::new(),
        })
    }

    pub fn spill_batch(&mut self, batch: ColumnarBatch) -> io::Result<()> {
        // TODO: use walrus here for on-disk spill
        self.in_memory_runs.push(batch);
        Ok(())
    }

    pub fn finish(self) -> io::Result<Vec<ColumnarBatch>> {
        Ok(self.in_memory_runs)
    }
}
