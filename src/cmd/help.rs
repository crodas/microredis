//! # All help text to keep other controllers clean
use crate::{error::Error, value::Value};

fn convert_to_result(text: &[&str]) -> Result<Value, Error> {
    Ok(Value::Array(
        text.iter()
            .map(|text| Value::String(text.to_string()))
            .collect(),
    ))
}

/// Help text for OBJECT command
pub fn object() -> Result<Value, Error> {
    convert_to_result(&[
        "OBJECT <subcommand> arg arg ... arg. Subcommands are:",
        "ENCODING <key> -- Return the kind of internal representation used in order to store the value associated with a key.",
        "FREQ <key> -- Return the access frequency index of the key. The returned integer is proportional to the logarithm of the recent access frequency of the key.",
        "IDLETIME <key> -- Return the idle time of the key, that is the approximated number of seconds elapsed since the last access to the key.",
        "REFCOUNT <key> -- Return the number of references of the value associated with the specified key.",
    ])
}

/// Help text for PUBSUB command
pub fn pubsub() -> Result<Value, Error> {
    convert_to_result(&[
        "PUBSUB <subcommand> arg arg ... arg. Subcommands are:",
        "CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all).",
        "NUMPAT -- Return number of subscriptions to patterns.",
        "NUMSUB [channel-1 .. channel-N] -- Returns the number of subscribers for the specified channels (excluding patterns, default: none).",
    ])
}

/// Help text for COMMAND command
pub fn command() -> Result<Value, Error> {
    convert_to_result(&[
        "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "(no subcommand)",
        "\tReturn details about all Redis commands",
        "COUNT",
        "\tReturn the total number of commands in this Redis server.",
        "GETKEYS <full-command>",
        "\tReturn the keys from a full Redis command.",
        "INFO [<command-name> ...]",
        "Return details about multiple Redis commands.",
        "HELP",
        "\tPrints this help.",
    ])
}
