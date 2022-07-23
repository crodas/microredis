//! # Redis Value
//!
//! All redis internal data structures and values are abstracted in this mod.
pub mod checksum;
pub mod cursor;
pub mod expiration;
pub mod float;
pub mod locked;
pub mod typ;

use crate::{cmd::now, error::Error, value_try_from, value_vec_try_from};
use bytes::{Bytes, BytesMut};
use redis_zero_protocol_parser::Value as ParsedValue;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::{TryFrom, TryInto},
    str::FromStr,
    time::Duration,
};

/// Redis Value.
///
/// This enum represents all data structures that are supported by Redis
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    /// Hash. This type cannot be serialized
    Hash(locked::Value<HashMap<Bytes, Bytes>>),
    /// List. This type cannot be serialized
    List(locked::Value<VecDeque<checksum::Value>>),
    /// Set. This type cannot be serialized
    Set(locked::Value<HashSet<Bytes>>),
    /// Vector/Array of values
    Array(Vec<Value>),
    /// Bytes/Strings/Binary data
    Blob(BytesMut),
    /// String. This type does not allow new lines
    String(String),
    /// An error
    Err(String, String),
    /// Integer
    Integer(i64),
    /// Boolean
    Boolean(bool),
    /// Float number
    Float(f64),
    /// Big number
    BigInteger(i128),
    /// Null
    Null,
    /// The command has been Queued
    Queued,
    /// Ok
    Ok,
    /// Empty response that is not send to the client
    Ignore,
}

impl Default for Value {
    fn default() -> Self {
        Self::Null
    }
}

/// Value debug struct
#[derive(Debug)]
pub struct VDebug {
    /// Value encoding
    pub encoding: &'static str,
    /// Length of serialized value
    pub serialize_len: usize,
}

impl From<VDebug> for Value {
    fn from(v: VDebug) -> Self {
        Value::Blob(format!(
            "Value at:0x6000004a8840 refcount:1 encoding:{} serializedlength:{} lru:13421257 lru_seconds_idle:367",
            v.encoding, v.serialize_len
            ).as_str().into()
        )
    }
}

impl Value {
    /// Creates a new Redis value from a stream of bytes
    pub fn new(value: &[u8]) -> Self {
        Self::Blob(value.into())
    }

    /// Returns the internal encoding of the redis
    pub fn encoding(&self) -> &'static str {
        match self {
            Self::Hash(_) | Self::Set(_) => "hashtable",
            Self::List(_) => "linkedlist",
            Self::Array(_) => "vector",
            _ => "embstr",
        }
    }

    /// Is the current value an error?
    pub fn is_err(&self) -> bool {
        match self {
            Self::Err(..) => true,
            _ => false,
        }
    }

    /// Return debug information for the type
    pub fn debug(&self) -> VDebug {
        let bytes: Vec<u8> = self.into();
        VDebug {
            encoding: self.encoding(),
            serialize_len: bytes.len(),
        }
    }

    /// Returns the hash of the value
    pub fn digest(&self) -> Vec<u8> {
        let bytes: Vec<u8> = self.into();
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        hasher.finalize().to_vec()
    }
}

impl From<&Value> for Vec<u8> {
    fn from(value: &Value) -> Vec<u8> {
        match value {
            Value::Ignore => b"".to_vec(),
            Value::Null => b"*-1\r\n".to_vec(),
            Value::Array(x) => {
                let mut s: Vec<u8> = format!("*{}\r\n", x.len()).into();
                for i in x.iter() {
                    let b: Vec<u8> = i.into();
                    s.extend(b);
                }
                s
            }
            Value::Integer(x) => format!(":{}\r\n", x).into(),
            Value::BigInteger(x) => format!("({}\r\n", x).into(),
            Value::Float(x) => format!(",{}\r\n", x).into(),
            Value::Blob(x) => {
                let s = format!("${}\r\n", x.len());
                let mut s: BytesMut = s.as_str().as_bytes().into();
                s.extend_from_slice(x);
                s.extend_from_slice(b"\r\n");
                s.to_vec()
            }
            Value::Err(x, y) => format!("-{} {}\r\n", x, y).into(),
            Value::String(x) => format!("+{}\r\n", x).into(),
            Value::Queued => "+QUEUED\r\n".into(),
            Value::Ok => "+OK\r\n".into(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = Error;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        match val {
            Value::BigInteger(x) => (*x).try_into().map_err(|_| Error::NotANumber),
            Value::Integer(x) => Ok(*x),
            Value::Blob(x) => bytes_to_number::<i64>(&x),
            Value::String(x) => x.parse::<i64>().map_err(|_| Error::NotANumber),
            _ => Err(Error::NotANumber),
        }
    }
}

impl TryFrom<&Value> for f64 {
    type Error = Error;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        match val {
            Value::Float(x) => Ok(*x),
            Value::Blob(x) => bytes_to_number::<f64>(x),
            Value::String(x) => x.parse::<f64>().map_err(|_| Error::NotANumber),
            _ => Err(Error::NotANumber),
        }
    }
}

/// Tries to convert bytes data into a number
///
/// If the conversion fails a Error::NotANumber error is returned.
#[inline]
pub fn bytes_to_number<T: FromStr>(bytes: &[u8]) -> Result<T, Error> {
    let x = String::from_utf8_lossy(bytes);
    x.parse::<T>().map_err(|_| Error::NotANumber)
}

/// Tries to convert bytes data into an integer number
#[inline]
pub fn bytes_to_int<T: FromStr>(bytes: &[u8]) -> Result<T, Error> {
    let x = String::from_utf8_lossy(bytes);
    x.parse::<T>()
        .map_err(|_| Error::NotANumberType("an integer".to_owned()))
}

impl<'a> From<&ParsedValue<'a>> for Value {
    fn from(value: &ParsedValue) -> Self {
        match value {
            ParsedValue::String(x) => Self::String((*x).to_string()),
            ParsedValue::Blob(x) => Self::new(*x),
            ParsedValue::Array(x) => Self::Array(x.iter().map(|x| x.into()).collect()),
            ParsedValue::Boolean(x) => Self::Boolean(*x),
            ParsedValue::BigInteger(x) => Self::BigInteger(*x),
            ParsedValue::Integer(x) => Self::Integer(*x),
            ParsedValue::Float(x) => Self::Float(*x),
            ParsedValue::Error(x, y) => Self::Err((*x).to_string(), (*y).to_string()),
            ParsedValue::Null => Self::Null,
        }
    }
}

value_try_from!(f64, Value::Float);
value_try_from!(i32, Value::Integer);
value_try_from!(u32, Value::Integer);
value_try_from!(i64, Value::Integer);
value_try_from!(i128, Value::BigInteger);

impl From<usize> for Value {
    fn from(value: usize) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Vec<u8> {
        (&value).into()
    }
}

impl From<Option<&Bytes>> for Value {
    fn from(v: Option<&Bytes>) -> Self {
        if let Some(v) = v {
            v.into()
        } else {
            Value::Null
        }
    }
}

impl From<&Bytes> for Value {
    fn from(v: &Bytes) -> Self {
        Value::new(v)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Blob(value.as_bytes().into())
    }
}

impl From<HashMap<Bytes, Bytes>> for Value {
    fn from(value: HashMap<Bytes, Bytes>) -> Value {
        Value::Hash(locked::Value::new(value))
    }
}

impl From<VecDeque<checksum::Value>> for Value {
    fn from(value: VecDeque<checksum::Value>) -> Value {
        Value::List(locked::Value::new(value))
    }
}

impl From<HashSet<Bytes>> for Value {
    fn from(value: HashSet<Bytes>) -> Value {
        Value::Set(locked::Value::new(value))
    }
}

value_vec_try_from!(&str);

impl From<String> for Value {
    fn from(value: String) -> Value {
        value.as_str().into()
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Value {
        Value::Array(value)
    }
}

impl TryInto<Vec<Value>> for Value {
    type Error = Error;

    fn try_into(self) -> Result<Vec<Value>, Self::Error> {
        match self {
            Self::Array(x) => Ok(x),
            _ => Err(Error::Internal),
        }
    }
}
