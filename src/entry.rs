use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

pub fn current_epoch_millis() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .expect("Time went backwards")
       .as_millis() as u64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Entry {
    prefix_meta: String,
    data: String,
    suffix_meta: String,
}

impl Entry {
    pub fn new(data: &str) -> Self {
        Entry {
            prefix_meta: "".to_string(),
            data: data.to_string(),
            suffix_meta: "".to_string(),
        }
    }
}