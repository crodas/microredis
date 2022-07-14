//!  # Client-group command handlers

use crate::{
    connection::{Connection, UnblockReason},
    error::Error,
    option,
    value::{bytes_to_number, Value},
};
use bytes::Bytes;
use std::sync::Arc;

/// "client" command handler
///
/// Documentation:
///  * <https://redis.io/commands/client-id>
pub async fn client(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let sub = String::from_utf8_lossy(&args[1]);

    let expected = match sub.to_lowercase().as_str() {
        "setname" => Some(3),
        "unblock" => None,
        _ => Some(2),
    };

    if let Some(expected) = expected {
        if args.len() != expected {
            return Err(Error::WrongArgument(
                "client".to_owned(),
                sub.to_uppercase(),
            ));
        }
    }

    match sub.to_lowercase().as_str() {
        "id" => Ok((conn.id() as i64).into()),
        "info" => Ok(conn.to_string().into()),
        "getname" => Ok(option!(conn.name())),
        "list" => {
            let mut list_client = "".to_owned();
            conn.all_connections()
                .iter(&mut |conn: Arc<Connection>| list_client.push_str(&conn.to_string()));
            Ok(list_client.into())
        }
        "unblock" => {
            let reason = match args.get(3) {
                Some(x) => match String::from_utf8_lossy(&x).to_uppercase().as_str() {
                    "TIMEOUT" => UnblockReason::Timeout,
                    "ERROR" => UnblockReason::Error,
                    _ => return Err(Error::Syntax),
                },
                None => UnblockReason::Timeout,
            };
            let other_conn = match conn
                .all_connections()
                .get_by_conn_id(bytes_to_number(&args[2])?)
            {
                Some(conn) => conn,
                None => return Ok(0.into()),
            };

            Ok(if other_conn.unblock(reason) {
                1.into()
            } else {
                0.into()
            })
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
    Ok(Value::new(&args[1]))
}

/// Select the Redis logical database having the specified zero-based numeric
/// index. New connections always use the database 0.
pub async fn select(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.selectdb(bytes_to_number(&args[1])?)
}

/// "ping" command handler
///
/// Documentation:
///  * <https://redis.io/commands/ping>
pub async fn ping(_conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match args.len() {
        1 => Ok(Value::String("PONG".to_owned())),
        2 => Ok(Value::new(&args[1])),
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

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };

    #[tokio::test]
    async fn select() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await
        );
        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["hget", "foo", "f1"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "1"]).await);
        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["hget", "foo", "f1"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "0"]).await);
        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["hget", "foo", "f1"]).await
        );
    }

    #[tokio::test]
    async fn select_err_0() {
        let c = create_connection();
        assert_eq!(
            Err(Error::NotANumber),
            run_command(&c, &["select", "-1"]).await
        );
    }

    #[tokio::test]
    async fn select_err_1() {
        let c = create_connection();
        assert_eq!(
            Err(Error::NotSuchDatabase),
            run_command(&c, &["select", "10000000"]).await
        );
    }
}
