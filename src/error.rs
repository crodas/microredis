use crate::value::Value;

pub enum Error {
    CommandNotFound(String),
    InvalidArgsCount(String),
    ProtocolError(String, String),
    NotANumber,
    WrongType,
}

impl From<Error> for Value {
    fn from(value: Error) -> Value {
        let err_type = match value {
            Error::WrongType => "WRONGTYPE",
            _ => "ERR",
        };

        let err_msg = match value {
            Error::CommandNotFound(x) => format!("unknown command `{}`", x),
            Error::InvalidArgsCount(x) => format!("wrong number of arguments for '{}' command", x),
            Error::ProtocolError(x, y) => format!("Protocol error: expected '{}', got '{}'", x, y),
            Error::NotANumber => "value is not an integer or out of range".to_string(),
            Error::WrongType => {
                "Operation against a key holding the wrong kind of value".to_string()
            }
        };

        Value::Err(err_type.to_string(), err_msg)
    }
}
