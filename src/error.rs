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
    #[error("unknown command `{0}`")]
    CommandNotFound(String),
    /// A sub-command is not found
    #[error("Unknown subcommand or wrong number of arguments for '{0}'. Try {1} HELP.")]
    SubCommandNotFound(String, String),
    /// Invalid number of arguments
    #[error("wrong number of arguments for '{0}' command")]
    InvalidArgsCount(String),
    /// Invalid Rank value
    #[error("{0} can't be zero: use 1 to start from the first match, 2 from the second, ...")]
    InvalidRank(String),
    /// The glob-pattern is not valid
    #[error("'{0}' is not a valid pattern")]
    InvalidPattern(String),
    /// Internal Error
    #[error("internal error")]
    Internal,
    /// Protocol error
    #[error("Protocol error: expected '{1}', got '{0}'")]
    Protocol(String, String),
    /// Unexpected argument
    #[error("Unknown subcommand or wrong number of arguments for '{1}'. Try {0} HELP.")]
    WrongArgument(String, String),
    /// We cannot incr by infinity
    #[error("increment would produce NaN or Infinity")]
    IncrByInfOrNan,
    /// Wrong number of arguments
    #[error("wrong number of arguments for '{0}' command")]
    WrongNumberArgument(String),
    /// Key not found
    #[error("no such key")]
    NotFound,
    /// Index out of range
    #[error("index out of range")]
    OutOfRange,
    /// String is bigger than max allowed size
    #[error("string exceeds maximum allowed size (proto-max-bulk-len)")]
    MaxAllowedSize,
    /// Attempting to move or copy to the same key
    #[error("source and destination objects are the same")]
    SameEntry,
    /// The connection is in pubsub only mode and the current command is not compatible.
    #[error("Can't execute '{0}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")]
    PubsubOnly(String),
    /// Syntax error
    #[error("syntax error")]
    Syntax,
    /// Byte cannot be converted to a number
    #[error("value is not a valid number or out of range")]
    NotANumber,
    /// Not a number with specific number type
    #[error("value is not {0} or out of range")]
    NotANumberType(String),
    /// Number overflow
    #[error("increment or decrement would overflow")]
    Overflow,
    /// Unexpected negative number
    #[error("{0} is negative")]
    NegativeNumber(String),
    /// The connection is not in a transaction
    #[error(" without MULTI")]
    NotInTx,
    /// Transaction was aborted
    #[error("Transaction discarded because of previous errors.")]
    TxAborted,
    /// The requested database does not exists
    #[error("DB index is out of range")]
    NotSuchDatabase,
    /// The connection is in a transaction and nested transactions are not supported
    #[error("calls can not be nested")]
    NestedTx,
    /// Watch is not allowed after a Multi has been called
    #[error("WATCH inside MULTI is not allowed")]
    WatchInsideTx,
    /// Wrong data type
    #[error("Operation against a key holding the wrong kind of value")]
    WrongType,
    /// Cursor error
    #[error("Error while creating or parsing the cursor: {0}")]
    Cursor(#[from] crate::value::cursor::Error),
    /// The connection has been unblocked by another connection and it wants to signal it
    /// through an error.
    #[error("client unblocked via CLIENT UNBLOCK")]
    UnblockByError,
    /// Client manual disconnection
    #[error("Manual disconnection")]
    Quit,
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
            Error::TxAborted => "EXECABORT",
            Error::UnblockByError => "UNBLOCKED",
            _ => "ERR",
        };

        Value::Err(err_type.to_string(), value.to_string())
    }
}
