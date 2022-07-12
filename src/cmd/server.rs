//! # Server command handlers
use crate::{
    check_arg, connection::Connection, error::Error, try_get_arg, value::bytes_to_number,
    value::Value,
};
use bytes::Bytes;
use git_version::git_version;
use std::{
    convert::TryInto,
    ops::Neg,
    time::{SystemTime, UNIX_EPOCH},
};
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
                    .map(|key| Value::new(*key))
                    .collect(),
            ))
        }
        "help" => super::help::command(),
        cmd => Err(Error::SubCommandNotFound(
            cmd.into(),
            String::from_utf8_lossy(&args[0]).into(),
        )),
    }
}

/// The DEBUG command is an internal command. It is meant to be used for
/// developing and testing Redis.
pub async fn debug(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match String::from_utf8_lossy(&args[1]).to_lowercase().as_str() {
        "object" => Ok(conn.db().debug(try_get_arg!(args, 2))?.into()),
        "set-active-expire" => Ok(Value::Ok),
        "digest-value" => Ok(Value::Array(conn.db().digest(&args[2..])?)),
        _ => Err(Error::Syntax),
    }
}

/// The INFO command returns information and statistics about the server in a
/// format that is simple to parse by computers and easy to read by humans.
pub async fn info(_: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    Ok(Value::Blob(
        format!(
            "redis_version: {}\r\nredis_git_sha1:{}\r\n",
            git_version!(),
            git_version!()
        )
        .as_str()
        .into(),
    ))
}

/// Delete all the keys of the currently selected DB. This command never fails.
pub async fn flushdb(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().flushdb()
}

/// Return the number of keys in the currently-selected database.
pub async fn dbsize(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.db().len().map(|s| s.into())
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
