use crate::wal::FsyncSchedule;

#[derive(Clone, Debug)]
pub struct SqlExecutorWalOptions {
    pub(crate) namespace: String,
    pub(crate) cleanup_on_drop: bool,
    pub(crate) reset_namespace: bool,
    pub(crate) storage_dir: Option<String>,
    pub(crate) fsync_schedule: FsyncSchedule,
    pub(crate) wal_enabled: bool,
}

impl SqlExecutorWalOptions {
    pub fn new(namespace: impl Into<String>) -> Self {
        SqlExecutorWalOptions {
            namespace: namespace.into(),
            cleanup_on_drop: true,
            reset_namespace: true,
            storage_dir: None,
            fsync_schedule: FsyncSchedule::SyncEach,
            wal_enabled: true,
        }
    }

    pub fn cleanup_on_drop(mut self, value: bool) -> Self {
        self.cleanup_on_drop = value;
        self
    }

    pub fn reset_namespace(mut self, value: bool) -> Self {
        self.reset_namespace = value;
        self
    }

    pub fn storage_dir(mut self, dir: impl Into<String>) -> Self {
        self.storage_dir = Some(dir.into());
        self
    }

    pub fn fsync_schedule(mut self, schedule: FsyncSchedule) -> Self {
        self.fsync_schedule = schedule;
        self
    }

    pub fn wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
