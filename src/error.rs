//! # Redis Error
//!
//! All redis errors are abstracted in this mod.
use crate::value::Value;
use thiserror::Error;

/// Redis errors
#[derive(Debug, PartialEq, Error)]
pub enum Error {
    /// IO Error
    #[error("IO error {0}")]
    Io(String),
    /// Config
    #[error("Config error {0}")]
    Config(#[from] redis_config_parser::de::Error),
    /// Empty line
    #[error("No command provided")]
    EmptyLine,
    /// A command is not found
    #[error("Command {0} not found")]
    CommandNotFound(String),
    /// A sub-command is not found
    #[error("Subcommand {0} / {1} not found")]
    SubCommandNotFound(String, String),
    /// Invalid number of arguments
    #[error("Invalid number of argumetns for command {0}")]
    InvalidArgsCount(String),
    /// The glob-pattern is not valid
    #[error("Invalid pattern {0}")]
    InvalidPattern(String),
    /// Internal Error
    #[error("Internal error")]
    Internal,
    /// Protocol error
    #[error("Protocol error {0} expecting {1}")]
    Protocol(String, String),
    /// Unexpected argument
    #[error("Wrong argument {1} for command {0}")]
    WrongArgument(String, String),
    /// Wrong number of arguments
    #[error("wrong number of arguments for '{0}' command")]
    WrongNumberArgument(String),
    /// Key not found
    #[error("Key not found")]
    NotFound,
    /// Index out of range
    #[error("Index out of range")]
    OutOfRange,
    /// Attempting to move or copy to the same key
    #[error("Cannot move same key")]
    SameEntry,
    /// The connection is in pubsub only mode and the current command is not compabible.
    #[error("Invalid command {0} in pubsub mode")]
    PubsubOnly(String),
    /// Syntax error
    #[error("Syntax error")]
    Syntax,
    /// Byte cannot be converted to a number
    #[error("Not a number")]
    NotANumber,
    /// The connection is not in a transaction
    #[error("Not in a transaction")]
    NotInTx,
    /// The requested database does not exists
    #[error("Database does not exists")]
    NotSuchDatabase,
    /// The connection is in a transaction and nested transactions are not supported
    #[error("Nested transaction not allowed")]
    NestedTx,
    /// Wrong data type
    #[error("Wrong type")]
    WrongType,
    /// Cursor error
    #[error("Error while creating or parsing the cursor: {0}")]
    Cursor(#[from] crate::value::cursor::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
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
            Error::Cursor(_) => "internal error".to_owned(),
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
            Error::Io(io) => format!("io error: {}", io),
            Error::Config(c) => format!("failed to parse config: {}", c),
            Error::PubsubOnly(x) => format!("Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", x),
            Error::WrongArgument(x, y) => format!(
                "Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.",
                y, x
            ),
            Error::WrongType => {
                "Operation against a key holding the wrong kind of value".to_owned()
            }
            _ => value.to_string(),
        };

        Value::Err(err_type.to_string(), err_msg)
    }
}
