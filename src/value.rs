use crate::{error::Error, value_try_from, value_vec_try_from};
use bytes::{Bytes, BytesMut};
use redis_zero_parser::Value as ParsedValue;
use std::convert::{TryFrom, TryInto};

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Array(Vec<Value>),
    Blob(Bytes),
    String(String),
    Err(String, String),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    BigInteger(i128),
    Null,
    OK,
}

impl From<&Value> for Vec<u8> {
    fn from(value: &Value) -> Vec<u8> {
        match value {
            Value::Null => b"*-1\r\n".to_vec(),
            Value::Array(x) => {
                let mut s: Vec<u8> = format!("*{}\r\n", x.len()).into();
                for i in x.iter() {
                    let b: Vec<u8> = i.into();
                    s.extend(b);
                }
                s.to_vec()
            }
            Value::Integer(x) => format!(":{}\r\n", x).into(),
            Value::BigInteger(x) => format!("({}\r\n", x).into(),
            Value::Blob(x) => {
                let s = format!("${}\r\n", x.len());
                let mut s: BytesMut = s.as_str().as_bytes().into();
                s.extend_from_slice(&x);
                s.extend_from_slice(b"\r\n");
                s.to_vec()
            }
            Value::Err(x, y) => format!("-{} {}\r\n", x, y).into(),
            Value::OK => "+OK\r\n".into(),
            _ => b"*-1\r\n".to_vec(),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = Error;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        match val {
            Value::BigInteger(x) => (*x).try_into().map_err(|_| Error::NotANumber),
            Value::Integer(x) => Ok(*x),
            Value::Blob(x) => {
                let x = unsafe { std::str::from_utf8_unchecked(x) };
                x.parse::<i64>().map_err(|_| Error::NotANumber)
            }
            Value::String(x) => x.parse::<i64>().map_err(|_| Error::NotANumber),
            _ => Err(Error::NotANumber),
        }
    }
}

impl From<Value> for Vec<u8> {
    fn from(value: Value) -> Vec<u8> {
        (&value).into()
    }
}

impl<'a> From<&ParsedValue<'a>> for Value {
    fn from(value: &ParsedValue) -> Self {
        match value {
            ParsedValue::String(x) => Self::String(x.to_string()),
            ParsedValue::Blob(x) => Self::Blob(Bytes::copy_from_slice(*x)),
            ParsedValue::Array(x) => {
                Self::Array(x.iter().map(|x| Value::try_from(x).unwrap()).collect())
            }
            ParsedValue::Boolean(x) => Self::Boolean(*x),
            ParsedValue::BigInteger(x) => Self::BigInteger(*x),
            ParsedValue::Integer(x) => Self::Integer(*x),
            ParsedValue::Float(x) => Self::Float(*x),
            ParsedValue::Error(x, y) => Self::Err(x.to_string(), y.to_string()),
            ParsedValue::Null => Self::Null,
        }
    }
}

value_try_from!(i64, Value::Integer);
value_try_from!(i128, Value::BigInteger);

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Blob(Bytes::copy_from_slice(value.as_bytes()))
    }
}

value_vec_try_from!(&str);
