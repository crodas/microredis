use redis_zero_parser::Value as ParsedValue;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Array(Vec<Value>),
    Blob(Vec<u8>),
    String(String),
    Err(String, String),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    BigInteger(i128),
    Null,
}

impl<'a> TryFrom<&ParsedValue<'a>> for Value {
    type Error = &'static str;

    fn try_from(value: &ParsedValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ParsedValue::String(x) => Self::String(x.to_string()),
            ParsedValue::Blob(x) => Self::Blob(x.to_vec()),
            ParsedValue::Array(x) => {
                Self::Array(x.iter().map(|x| Value::try_from(x).unwrap()).collect())
            }
            ParsedValue::Boolean(x) => Self::Boolean(*x),
            ParsedValue::BigInteger(x) => Self::BigInteger(*x),
            ParsedValue::Integer(x) => Self::Integer(*x),
            ParsedValue::Float(x) => Self::Float(*x),
            ParsedValue::Error(x, y) => Self::Err(x.to_string(), y.to_string()),
            ParsedValue::Null => Self::Null,
        })
    }
}
