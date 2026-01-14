use std::path::PathBuf;

pub(crate) fn sql_executor_wal_base_dir() -> PathBuf {
    std::env::var("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("wal_files"))
}

pub(crate) fn ensure_sql_executor_wal_root() {
    let _ = std::fs::create_dir_all(sql_executor_wal_base_dir());
}

pub(crate) fn remove_sql_executor_wal_dir(namespace: &str) {
    let dir = sql_executor_wal_base_dir().join(namespace);
    if dir.exists() {
        let _ = std::fs::remove_dir_all(dir);
    }
}
