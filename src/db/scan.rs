//! # Scan trait
//!
//! This trait must be implemented for all data structure that implements
//! SCAN-like commands.
use crate::{
    error::Error,
    value::{cursor::Cursor, typ::Typ, Value},
};
use bytes::Bytes;

/// Result of an scan
pub struct Result {
    /// New cursor to be used in the next request
    pub cursor: Cursor,
    /// Result
    pub result: Vec<Value>,
}

impl From<Result> for Value {
    fn from(v: Result) -> Value {
        Value::Array(vec![v.cursor.to_string().into(), Value::Array(v.result)])
    }
}

/// Scan trait
pub trait Scan {
    /// Scans the current data struct and returns a sub-set of the result and a
    /// cursor to continue where it left off.
    fn scan(
        &self,
        cursor: Cursor,
        pattern: Option<Bytes>,
        count: Option<usize>,
        typ: Option<Typ>,
    ) -> std::result::Result<Result, Error>;
}
