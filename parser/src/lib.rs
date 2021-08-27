mod bytes;
#[macro_use]
mod macros;

use bytes::Bytes;
use std::convert::TryInto;

pub struct Parser<'a> {
    value: Option<Value<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Status {
    Partial,
    Complete,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Error {
    InvalidPrefix,
    InvalidLength,
    InvalidBoolean,
    InvalidNumber,
    NewLine,
}

impl<'a> Default for Parser<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Parser<'a> {
    pub fn new() -> Self {
        Parser { value: None }
    }

    pub fn get_value(&self) -> Option<&'a Value> {
        self.value.as_ref()
    }

    pub fn take_value(&mut self) -> Option<Value> {
        self.value.take()
    }

    pub fn parse(&mut self, buffer: &'a [u8]) -> Result<Status, Error> {
        let mut bytes = Bytes::new(buffer);

        self.parse_bytes(&mut bytes)
    }

    fn parse_bytes(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        match next!(bytes) {
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

    fn parse_error(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let err_type = unsafe { std::str::from_utf8_unchecked(read_until!(bytes, b' ')) };
        let str = unsafe { std::str::from_utf8_unchecked(read_line!(bytes)) };
        self.value = Some(Value::Error(err_type, str));
        Ok(Status::Complete)
    }

    fn parse_str(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let str = unsafe { std::str::from_utf8_unchecked(read_line!(bytes)) };
        self.value = Some(Value::String(str));
        Ok(Status::Complete)
    }

    fn parse_boolean(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let v = match next!(bytes) {
            b't' => true,
            b'f' => false,
            _ => return Err(Error::InvalidBoolean),
        };
        self.value = Some(Value::Boolean(v));
        Ok(Status::Complete)
    }

    fn parse_big_integer(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let number = read_line_number!(bytes, i128);
        self.value = Some(Value::BigInteger(number));
        Ok(Status::Complete)
    }

    fn parse_integer(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let number = read_line_number!(bytes, i64);
        self.value = Some(Value::Integer(number));
        Ok(Status::Complete)
    }

    fn parse_float(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let number = read_line_number!(bytes, f64);
        self.value = Some(Value::Float(number));
        Ok(Status::Complete)
    }

    fn parse_blob(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let len = read_line_number!(bytes, i32);

        if len <= 0 {
            self.value = Some(Value::Null);
            return Ok(Status::Complete);
        }

        let blob = read_len!(bytes, len);
        assert_nl!(bytes);

        self.value = Some(Value::Blob(blob));

        Ok(Status::Complete)
    }

    fn parse_array(&mut self, bytes: &mut Bytes<'a>) -> Result<Status, Error> {
        let len = read_line_number!(bytes, i32);
        if len <= 0 {
            self.value = Some(Value::Null);
            return Ok(Status::Complete);
        }

        let mut v = vec![Value::Null; len as usize];

        for i in 0..len {
            if self.parse_bytes(bytes)? == Status::Partial {
                return Ok(Status::Partial);
            }
            let r = self.value.as_ref().unwrap();
            v[i as usize] = r.clone();
        }

        self.value = Some(Value::Array(v));

        Ok(Status::Complete)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_partial() {
        let mut p = Parser::new();
        let d = b"*-1";
        assert_eq!(Ok(Status::Partial), p.parse(d));
    }

    #[test]
    fn test_parse_partial_2() {
        let mut p = Parser::new();
        let d = b"*12\r\n";
        assert_eq!(Ok(Status::Partial), p.parse(d));
    }

    #[test]
    fn test_incomplete_blob_parsing() {
        let d = b"$60\r\nfoobar\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Partial), p.parse(d));
    }

    #[test]
    fn test_complete_blob_parsing() {
        let d = b"$6\r\nfoobar\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let data = match p.get_value() {
            Some(Value::Blob(x)) => unsafe { std::str::from_utf8_unchecked(x) },
            _ => "",
        };

        assert_eq!(data, "foobar");
    }

    #[test]
    fn test_complete_array_parser() {
        let d = b"*2\r\n$6\r\nfoobar\r\n$3\r\nfoo\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Array(x)) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(2, x.len());
    }

    #[test]
    fn test_complete_nested_array_parser() {
        let d = b"*2\r\n$6\r\nfoobar\r\n*1\r\n$3\r\nfoo\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Array(x)) => x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(2, x.len());
    }

    #[test]
    fn test_parse_float() {
        let d = b",0.25887\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Float(x)) => *x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(0.25887, x);
    }

    #[test]
    fn test_parse_integer() {
        let d = b":25887\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Integer(x)) => *x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(25887, x);
    }

    #[test]
    fn test_parse_big_integer() {
        let d = b"(25887\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::BigInteger(x)) => *x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!(25887, x);
    }

    #[test]
    fn test_parse_false() {
        let d = b"#f\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Boolean(x)) => *x,
            _ => panic!("Unxpected type"),
        };

        assert!(!x);
    }

    #[test]
    fn test_parse_true() {
        let d = b"#t\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Boolean(x)) => *x,
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

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::String(x)) => *x,
            _ => panic!("Unxpected type"),
        };

        assert_eq!("hello world", x);
    }

    #[test]
    fn test_parse_error() {
        let d = b"-ERR this is the error description\r\n";
        let mut p = Parser::new();

        assert_eq!(Ok(Status::Complete), p.parse(d));

        let x = match p.get_value() {
            Some(Value::Error(a, b)) => (*a, *b),
            _ => panic!("Unxpected type"),
        };

        assert_eq!("ERR", x.0);
        assert_eq!("this is the error description", x.1);
    }
}
