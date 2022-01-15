//! # Redis Error
//!
//! All redis errors are abstracted in this mod.
use crate::value::Value;

/// Redis errors
#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    /// A command is not found
    CommandNotFound(String),
    /// A sub-command is not found
    SubCommandNotFound(String, String),
    /// Invalid number of arguments
    InvalidArgsCount(String),
    /// The glob-pattern is not valid
    InvalidPattern(String),
    /// Internal Error
    Internal,
    /// Protocol error
    Protocol(String, String),
    /// Unexpected argument
    WrongArgument(String, String),
    /// Command not found
    NotFound,
    /// Index out of range
    OutOfRange,
    /// Attempting to move or copy to the same key
    SameEntry,
    /// The connection is in pubsub only mode and the current command is not compabible.
    PubsubOnly(String),
    /// Syntax error
    Syntax,
    /// Byte cannot be converted to a number
    NotANumber,
    /// The connection is not in a transaction
    NotInTx,
    /// The requested database does not exists
    NotSuchDatabase,
    /// The connection is in a transaction and nested transactions are not supported
    NestedTx,
    /// Wrong data type
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
            Error::SubCommandNotFound(x, y) => format!("Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.", x, y),
            Error::InvalidArgsCount(x) => format!("wrong number of arguments for '{}' command", x),
            Error::InvalidPattern(x) => format!("'{}' is not a valid pattern", x),
            Error::Internal => "internal error".to_owned(),
            Error::Protocol(x, y) => format!("Protocol error: expected '{}', got '{}'", x, y),
            Error::NotInTx => " without MULTI".to_owned(),
            Error::SameEntry => "source and destination objects are the same".to_owned(),
            Error::NotANumber => "value is not an integer or out of range".to_owned(),
            Error::OutOfRange => "index out of range".to_owned(),
            Error::Syntax => "syntax error".to_owned(),
            Error::NotFound => "no such key".to_owned(),
            Error::NotSuchDatabase => "DB index is out of range".to_owned(),
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
