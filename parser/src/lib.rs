mod bytes;
#[macro_use]
mod macros;

use bytes::Bytes;
use std::convert::TryInto;

pub struct Parser {
    pos: usize
}

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Array(Vec<Value<'a>>),
    Blob(&'a [u8]),
    String(&'a str),
    Error(&'a str, &'a str),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    BigInteger(i128),
    Null,
}

impl<'a> Clone for Value<'a> {
    fn clone(&self) -> Value<'a> {
        match self {
            _ => Self::Null,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Error {
    Partial,
    InvalidPrefix,
    InvalidLength,
    InvalidBoolean,
    InvalidNumber,
    NewLine,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            pos: 0,
        }
    }

    pub fn parse(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        self.pos = 0;
        self.parse_bytes(bytes)
    }

    fn parse_bytes(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        match next!(self, bytes) {
            b'*' => self.parse_array(bytes),
            b'$' => self.parse_blob(bytes),
            b':' => self.parse_integer(bytes),
            b'(' => self.parse_big_integer(bytes),
            b',' => self.parse_float(bytes),
            b'#' => self.parse_boolean(bytes),
            b'+' => self.parse_str(bytes),
            b'-' => self.parse_error(bytes),
            _ => Err(Error::InvalidPrefix),
        }
    }

    fn parse_error(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let err_type = unsafe { std::str::from_utf8_unchecked(read_until!(self, bytes, b' ')) };
        let str = unsafe { std::str::from_utf8_unchecked(read_line!(self, bytes)) };
        Ok(Value::Error(err_type, str))
    }

    fn parse_str(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let str = unsafe { std::str::from_utf8_unchecked(read_line!(self, bytes)) };
        Ok(Value::String(str))
    }

    fn parse_boolean(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let v = match next!(self, bytes) {
            b't' => true,
            b'f' => false,
            _ => return Err(Error::InvalidBoolean),
        };
        Ok(Value::Boolean(v))
    }

    fn parse_big_integer(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let number = read_line_number!(self, bytes, i128);
        Ok(Value::BigInteger(number))
    }

    fn parse_integer(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let number = read_line_number!(self, bytes, i64);
        Ok(Value::Integer(number))
    }

    fn parse_float(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let number = read_line_number!(self, bytes, f64);
        Ok(Value::Float(number))
    }

    fn parse_blob(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let len = read_line_number!(self, bytes, i32);

        if len <= 0 {
            return Ok(Value::Null);
        }

        let blob = read_len!(self, bytes, len);
        assert_nl!(self, bytes);

        Ok(Value::Blob(blob))
    }

    fn parse_array(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let len = read_line_number!(self, bytes, i32);
        if len <= 0 {
            return Ok(Value::Null);
        }

        let mut v = vec![Value::Null; len as usize];

        for i in 0..len {
            v[i as usize] = self.parse_bytes(bytes)?;
        }

        Ok(Value::Array(v))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_partial() {
        let mut p = Parser::new();
        let d = b"*-1";
        assert_eq!(Err(Error::Partial), p.parse(d));
    }

    #[test]
    fn test_parse_partial_2() {
        let mut p = Parser::new();
        let d = b"*12\r\n";
        assert_eq!(Err(Error::Partial), p.parse(d));
    }

    #[test]
    fn test_incomplete_blob_parsing() {
        let d = b"$60\r\nfoobar\r\n";
        let mut p = Parser::new();

        assert_eq!(Err(Error::Partial), p.parse(d));
    }

    #[test]
    fn test_complete_blob_parsing() {
        let d = b"$6\r\nfoobar\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let data = match r.unwrap() {
            Value::Blob(x) => unsafe { std::str::from_utf8_unchecked(x) },
            _ => "",
        };

        assert_eq!(data, "foobar");
    }

    #[test]
    fn test_complete_array_parser() {
        let d = b"*2\r\n$6\r\nfoobar\r\n$3\r\nfoo\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Array(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(2, x.len());
    }

    #[test]
    fn test_complete_nested_array_parser() {
        let d = b"*2\r\n$6\r\nfoobar\r\n*1\r\n$3\r\nfoo\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Array(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(2, x.len());
    }

    #[test]
    fn test_parse_float() {
        let d = b",0.25887\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Float(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(0.25887, x);
    }

    #[test]
    fn test_parse_integer() {
        let d = b":25887\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Integer(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(25887, x);
    }

    #[test]
    fn test_parse_big_integer() {
        let d = b"(25887\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::BigInteger(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(25887, x);
    }

    #[test]
    fn test_parse_false() {
        let d = b"#f\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Boolean(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert!(!x);
    }

    #[test]
    fn test_parse_true() {
        let d = b"#t\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Boolean(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert!(x);
    }

    #[test]
    fn test_parse_boolean_unexpected() {
        let d = b"#1\r\n";
        let mut p = Parser::new();

        assert_eq!(Err(Error::InvalidBoolean), p.parse(d));
    }

    #[test]
    fn test_parse_str() {
        let d = b"+hello world\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::String(x) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!("hello world", x);
    }

    #[test]
    fn test_parse_error() {
        let d = b"-ERR this is the error description\r\n";
        let mut p = Parser::new();

        let r = p.parse(d);
        assert!(r.is_ok());

        let x = match r.unwrap() {
            Value::Error(a, b) => (a, b),
            _ => panic!("Unxpected type"),
        };

        assert_eq!("ERR", x.0);
        assert_eq!("this is the error description", x.1);
    }
}
