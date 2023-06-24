//!  # Client-group command handlers
use crate::{
    connection::{Connection, ConnectionStatus, UnblockReason},
    error::Error,
    value::{bytes_to_int, bytes_to_number, Value},
};
use bytes::Bytes;
use std::{collections::VecDeque, sync::Arc};

/// "client" command handler
///
/// Documentation:
///  * <https://redis.io/commands/client-id>
pub async fn client(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let sub = args.pop_front().ok_or(Error::Syntax)?;
    let sub = String::from_utf8_lossy(&sub);

    let expected = match sub.to_lowercase().as_str() {
        "setname" => Some(1),
        "unblock" => None,
        _ => Some(0),
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
        "getname" => Ok(conn.name().into()),
        "list" => {
            let mut list_client = "".to_owned();
            conn.all_connections()
                .iter(&mut |conn: Arc<Connection>| list_client.push_str(&conn.to_string()));
            Ok(list_client.into())
        }
        "unblock" => {
            let reason = match args.get(1) {
                Some(x) => match String::from_utf8_lossy(&x).to_uppercase().as_str() {
                    "TIMEOUT" => UnblockReason::Timeout,
                    "ERROR" => UnblockReason::Error,
                    _ => return Err(Error::Syntax),
                },
                None => UnblockReason::Timeout,
            };
            let other_conn = match conn
                .all_connections()
                .get_by_conn_id(bytes_to_int(&args[0])?)
            {
                Some(conn) => conn,
                None => return Ok(0.into()),
            };

            Ok(if other_conn.unblock(reason) {
                other_conn.append_response(if reason == UnblockReason::Error {
                    Error::UnblockByError.into()
                } else {
                    Value::Null
                });
                1.into()
            } else {
                0.into()
            })
        }
        "setname" => {
            let name = String::from_utf8_lossy(&args[0]).to_string();
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
pub async fn echo(_conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(Value::Blob(args.pop_front().ok_or(Error::Syntax)?))
}

/// Select the Redis logical database having the specified zero-based numeric
/// index. New connections always use the database 0.
pub async fn select(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.selectdb(bytes_to_number(&args[0])?)
}

/// "ping" command handler
///
/// Documentation:
///  * <https://redis.io/commands/ping>
pub async fn ping(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    if conn.status() == ConnectionStatus::Pubsub {
        return Ok(Value::Array(vec![
            "pong".into(),
            args.pop_front().map(|p| Value::Blob(p)).unwrap_or_default(),
        ]));
    }
    match args.len() {
        0 => Ok(Value::String("PONG".to_owned())),
        1 => Ok(Value::Blob(args.pop_front().ok_or(Error::Syntax)?)),
        _ => Err(Error::InvalidArgsCount("ping".to_owned())),
    }
}

/// "reset" command handler
///
/// Documentation:
///  * <https://redis.io/commands/reset>
pub async fn reset(conn: &Connection, _: VecDeque<Bytes>) -> Result<Value, Error> {
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

    #[tokio::test]
    async fn client_wrong_args() {
        let c = create_connection();
        assert_eq!(
            Err(Error::WrongArgument("client".to_owned(), "ID".to_owned())),
            run_command(&c, &["client", "id", "xxx"]).await
        );
        assert_eq!(
            Err(Error::WrongArgument("client".to_owned(), "XXX".to_owned())),
            run_command(&c, &["client", "xxx"]).await
        );
    }

    #[tokio::test]
    async fn client_id() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["client", "id"]).await);
        assert_eq!(
            Ok("id=1 addr=127.0.0.1:8080 name=None db=0\r\n".into()),
            run_command(&c, &["client", "info"]).await
        );
    }

    #[tokio::test]
    async fn client_set_name() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["client", "getname"]).await
        );
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["client", "setname", "test"]).await
        );
        assert_eq!(
            Ok("test".into()),
            run_command(&c, &["client", "getname"]).await
        );
        assert_eq!(Ok(1.into()), run_command(&c, &["client", "id"]).await);
    }

    #[tokio::test]
    async fn client_unblock_1() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let c1 = create_connection();
        let (mut c2_recv, c2) = c1.all_connections().new_connection(c1.db(), addr);

        // unblock, will fail because c2 is not blocked
        assert_eq!(
            Ok(0.into()),
            run_command(&c1, &["client", "unblock", "2", "error"]).await,
        );

        // block c2
        c2.block();

        // unblock c2 and return an error
        assert_eq!(
            Ok(1.into()),
            run_command(&c1, &["client", "unblock", "2", "error"]).await,
        );
        assert!(!c2.is_blocked());

        // read from c2 the error
        assert_eq!(
            Some(Value::Err(
                "UNBLOCKED".into(),
                "client unblocked via CLIENT UNBLOCK".into()
            )),
            c2_recv.recv().await
        );
    }

    #[tokio::test]
    async fn client_unblock_2() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let c1 = create_connection();
        let (mut c2_recv, c2) = c1.all_connections().new_connection(c1.db(), addr);
        // block c2
        c2.block();

        // unblock c2 and return an null
        assert_eq!(
            Ok(1.into()),
            run_command(&c1, &["client", "unblock", "2", "timeout"]).await,
        );
        assert!(!c2.is_blocked());

        // read from c2 the error
        assert_eq!(Some(Value::Null), c2_recv.recv().await);
    }

    #[tokio::test]
    async fn client_unblock_3() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let c1 = create_connection();
        let (mut c2_recv, c2) = c1.all_connections().new_connection(c1.db(), addr);
        // block c2
        c2.block();

        // unblock c2 and return an null
        assert_eq!(
            Ok(1.into()),
            run_command(&c1, &["client", "unblock", "2"]).await,
        );
        assert!(!c2.is_blocked());

        // read from c2 the error
        assert_eq!(Some(Value::Null), c2_recv.recv().await);
    }

    #[tokio::test]
    async fn client_unblock_4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let c1 = create_connection();
        let (mut c2_recv, c2) = c1.all_connections().new_connection(c1.db(), addr);
        // block c2
        c2.block();

        // unblock c2 and return an null
        assert_eq!(
            Err(Error::Syntax),
            run_command(&c1, &["client", "unblock", "2", "wrong-arg"]).await,
        );
        assert!(c2.is_blocked());
    }
}
