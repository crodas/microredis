//!  # Client-group command handlers

use crate::{connection::Connection, error::Error, option, value::Value};
use bytes::Bytes;
use std::sync::Arc;

/// "client" command handler
///
/// Documentation:
///  * <https://redis.io/commands/client-id>
pub async fn client(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let sub = String::from_utf8_lossy(&args[1]);

    let expected = match sub.to_lowercase().as_str() {
        "setname" => 3,
        _ => 2,
    };

    if args.len() != expected {
        return Err(Error::WrongArgument(
            "client".to_owned(),
            sub.to_uppercase(),
        ));
    }

    match sub.to_lowercase().as_str() {
        "id" => Ok((conn.id() as i64).into()),
        "info" => Ok(conn.as_string().into()),
        "getname" => Ok(option!(conn.name())),
        "list" => {
            let mut v: Vec<Value> = vec![];
            conn.all_connections()
                .iter(&mut |conn: Arc<Connection>| v.push(conn.as_string().into()));
            Ok(v.into())
        }
        "setname" => {
            let name = String::from_utf8_lossy(&args[2]).to_string();
            conn.set_name(name);
            Ok(Value::Ok)
        }
        _ => Err(Error::WrongArgument(
            "client".to_owned(),
            sub.to_uppercase(),
        )),
    }
}

/// "echo" command handler
///
/// Documentation:
///  * <https://redis.io/commands/echo>
pub async fn echo(_conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(Value::Blob(args[1].to_owned()))
}

/// "ping" command handler
///
/// Documentation:
///  * <https://redis.io/commands/ping>
pub async fn ping(_conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match args.len() {
        1 => Ok(Value::String("PONG".to_owned())),
        2 => Ok(Value::Blob(args[1].to_owned())),
        _ => Err(Error::InvalidArgsCount("ping".to_owned())),
    }
}

/// "reset" command handler
///
/// Documentation:
///  * <https://redis.io/commands/reset>
pub async fn reset(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.reset();
    Ok(Value::String("RESET".to_owned()))
}
