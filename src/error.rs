use crate::value::Value;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    CommandNotFound(String),
    InvalidArgsCount(String),
    Protocol(String, String),
    WrongArgument(String, String),
    NotFound,
    OutOfRange,
    PubsubOnly(String),
    Syntax,
    NotANumber,
    NotInTx,
    NestedTx,
    WrongType,
}

impl From<Error> for Value {
    fn from(value: Error) -> Value {
        let err_type = match value {
            Error::WrongType => "WRONGTYPE",
            Error::NestedTx => "ERR MULTI",
            Error::NotInTx => "ERR EXEC",
            _ => "ERR",
        };

        let err_msg = match value {
            Error::CommandNotFound(x) => format!("unknown command `{}`", x),
            Error::InvalidArgsCount(x) => format!("wrong number of arguments for '{}' command", x),
            Error::Protocol(x, y) => format!("Protocol error: expected '{}', got '{}'", x, y),
            Error::NotInTx => " without MULTI".to_owned(),
            Error::NotANumber => "value is not an integer or out of range".to_owned(),
            Error::OutOfRange => "index out of range".to_owned(),
            Error::Syntax => "syntax error".to_owned(),
            Error::NotFound => "no such key".to_owned(),
            Error::NestedTx => "calls can not be nested".to_owned(),
            Error::PubsubOnly(x) => format!("Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", x),
            Error::WrongArgument(x, y) => format!(
                "Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.",
                y, x
            ),
            Error::WrongType => {
                "Operation against a key holding the wrong kind of value".to_owned()
            }
        };

        Value::Err(err_type.to_string(), err_msg)
    }
}
