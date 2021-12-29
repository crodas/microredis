//! # Server command handlers
use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::{convert::TryInto, ops::Neg};
use tokio::time::Duration;

/// Returns Array reply of details about all Redis commands.
pub async fn command(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let dispatcher = conn.all_connections().get_dispatcher();
    if args.len() == 1 {
        return Ok(Value::Array(
            dispatcher
                .get_all_commands()
                .iter()
                .map(|command| command.get_command_info())
                .collect(),
        ));
    }

    match String::from_utf8_lossy(&args[1]).to_lowercase().as_str() {
        "count" => Ok(dispatcher.get_all_commands().len().into()),
        "info" => {
            let mut result = vec![];
            for command in &args[2..] {
                result.push(
                    dispatcher
                        .get_handler_for_command(
                            String::from_utf8_lossy(command).to_string().as_str(),
                        )
                        .map(|command| command.get_command_info())
                        .unwrap_or_else(|_| Value::Null),
                )
            }
            Ok(Value::Array(result))
        }
        "getkeys" => {
            if args.len() == 2 {
                return Err(Error::SubCommandNotFound(
                    String::from_utf8_lossy(&args[1]).into(),
                    String::from_utf8_lossy(&args[0]).into(),
                ));
            }
            let args = &args[2..];
            let command = dispatcher.get_handler(args)?;
            Ok(Value::Array(
                command
                    .get_keys(args)
                    .iter()
                    .map(|key| Value::Blob((*key).clone()))
                    .collect(),
            ))
        }
        "help" => Ok(Value::Array(vec![
            Value::String(
                "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into(),
            ),
            Value::String("(no subcommand)".into()),
            Value::String("\tReturn details about all Redis commands".into()),
            Value::String("COUNT".into()),
            Value::String("\tReturn the total number of commands in this Redis server.".into()),
            Value::String("GETKEYS <full-command>".into()),
            Value::String("\tReturn the keys from a full Redis command.".into()),
            Value::String("INFO [<command-name> ...]".into()),
            Value::String("Return details about multiple Redis commands.".into()),
            Value::String("HELP".into()),
            Value::String("\tPrints this help.".into()),
        ])),
        cmd => Err(Error::SubCommandNotFound(
            cmd.into(),
            String::from_utf8_lossy(&args[0]).into(),
        )),
    }
}

/// The TIME command returns the current server time as a two items lists: a
/// Unix timestamp and the amount of microseconds already elapsed in the current
/// second. Basically the interface is very similar to the one of the
/// gettimeofday system call.
pub async fn time(_conn: &Connection, _args: &[Bytes]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = format!("{}", since_the_epoch.as_secs());
    let millis = format!("{}", since_the_epoch.subsec_millis());

    Ok(vec![seconds.as_str(), millis.as_str()].into())
}
