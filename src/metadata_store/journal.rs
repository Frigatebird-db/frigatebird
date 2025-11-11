use crate::metadata_store::{ColumnStats, PageDirectory, PendingPage};
use crate::wal::Walrus;
use rkyv::{
    AlignedVec, Archive, Deserialize as RkyvDeserialize, Infallible, Serialize as RkyvSerialize,
    archived_root, to_bytes,
};
use std::io;
use std::sync::Arc;

#[derive(Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct MetaJournalEntry {
    pub column: String,
    pub descriptor_id: String,
    pub disk_path: String,
    pub offset: u64,
    pub alloc_len: u64,
    pub actual_len: u64,
    pub entry_count: u64,
    pub replace_last: bool,
    pub stats: Option<ColumnStats>,
}

#[derive(Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub enum MetaRecord {
    PublishPages {
        table: String,
        entries: Vec<MetaJournalEntry>,
    },
}

pub struct MetaJournal {
    wal: Arc<Walrus>,
    shard_count: usize,
}

impl MetaJournal {
    pub fn new(wal: Arc<Walrus>, shard_count: usize) -> Self {
        MetaJournal {
            wal,
            shard_count: shard_count.max(1),
        }
    }

    pub fn append_commit(&self, table: &str, record: &MetaRecord) -> io::Result<()> {
        let bytes = to_bytes::<_, 512>(record).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("journal serialize failed: {err:?}"),
            )
        })?;
        let topic = self.topic_for_table(table);
        self.wal.append_for_topic(&topic, &bytes)
    }

    pub fn replay_into(&self, directory: &Arc<PageDirectory>) -> io::Result<()> {
        if !directory.is_empty() {
            return Ok(());
        }

        for shard in 0..self.shard_count {
            let topic = self.topic_for_shard(shard);
            loop {
                match self.wal.read_next(&topic, true) {
                    Ok(Some(entry)) => {
                        let mut aligned = AlignedVec::with_capacity(entry.data.len());
                        aligned.extend_from_slice(&entry.data);
                        let archived = unsafe { archived_root::<MetaRecord>(&aligned) };
                        let record = archived.deserialize(&mut Infallible).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "journal deserialize failed")
                        })?;
                        self.apply_record(directory, record);
                    }
                    Ok(None) => break,
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    fn apply_record(&self, directory: &Arc<PageDirectory>, record: MetaRecord) {
        match record {
            MetaRecord::PublishPages { table, entries } => {
                let pending: Vec<PendingPage> = entries
                    .into_iter()
                    .map(|entry| PendingPage {
                        table: table.clone(),
                        column: entry.column,
                        descriptor_id: Some(entry.descriptor_id),
                        disk_path: entry.disk_path,
                        offset: entry.offset,
                        alloc_len: entry.alloc_len,
                        actual_len: entry.actual_len,
                        entry_count: entry.entry_count,
                        replace_last: entry.replace_last,
                        stats: entry.stats,
                    })
                    .collect();
                directory.register_batch(&pending);
            }
        }
    }

    fn topic_for_table(&self, table: &str) -> String {
        let shard = fast_hash(table) % self.shard_count as u64;
        self.topic_for_shard(shard as usize)
    }

    fn topic_for_shard(&self, shard: usize) -> String {
        format!("meta.critical.{shard:02}")
    }
}

fn fast_hash(value: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
