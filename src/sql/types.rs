use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
pub enum DataType {
    Null,
    Boolean,
    Int64,
    Float64,
    String,
    Timestamp, // Stored as i64 (Unix microseconds)
    Uuid,      // Stored as u128
    IpAddr,    // Stored as u128 (IPv6) or u32 (IPv4 mapped)
}

impl DataType {
    pub fn from_sql(sql_type: &str) -> Option<Self> {
        match sql_type.to_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Some(DataType::Boolean),
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" => Some(DataType::Int64),
            "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => Some(DataType::Float64),
            "VARCHAR" | "TEXT" | "STRING" | "CHAR" => Some(DataType::String),
            "TIMESTAMP" | "DATETIME" | "DATE" => Some(DataType::Timestamp),
            "UUID" => Some(DataType::Uuid),
            "INET" | "IP" => Some(DataType::IpAddr),
            _ => None,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::Int64 | DataType::Float64)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::String => write!(f, "VARCHAR"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::IpAddr => write!(f, "INET"),
        }
    }
}
