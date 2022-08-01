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
pub async fn info(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    let connections = conn.all_connections();
    Ok(Value::Blob(
        format!(
            "redis_version: {}\r\nredis_git_sha1:{}\r\n\r\nconnected_clients:{}\r\nblocked_clients:{}\r\n",
            git_version!(),
            git_version!(),
            connections.total_connections(),
            connections.total_blocked_connections(),
        )
        .as_str()
        .into(),
    ))
}

/// Delete all the keys of the currently selected DB. This command never fails.
pub async fn flushdb(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.db().flushdb()
}

/// Delete all the keys of all the existing databases, not just the currently
/// selected one. This command never fails.
pub async fn flushall(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.all_connections()
        .get_databases()
        .into_iter()
        .map(|db| db.flushdb())
        .for_each(drop);

    Ok(Value::Ok)
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

/// Ask the server to close the connection. The connection is closed as soon as
/// all pending replies have been written to the client.
pub async fn quit(_: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    Err(Error::Quit)
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };

    #[tokio::test]
    async fn digest() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "foo0", "f1", "1", "f2", "2", "f3", "3"]).await;
        let _ = run_command(&c, &["set", "foo1", "f1"]).await;
        let _ = run_command(&c, &["rpush", "foo2", "f1"]).await;
        let _ = run_command(&c, &["sadd", "foo3", "f1"]).await;
        assert_eq!(
            Ok(Value::Array(vec![
                "30c7a6a3e846cda0ec6bec93bbcead474c8b735d81d6b13043e8e7bd1287465b".into(),
                "c9c7eecf5cc340e36731787d8844a5b166d9611718fc12f0fa6501f711aad8a5".into(),
                "30c7a6a3e846cda0ec6bec93bbcead474c8b735d81d6b13043e8e7bd1287465b".into(),
                "30c7a6a3e846cda0ec6bec93bbcead474c8b735d81d6b13043e8e7bd1287465b".into(),
            ])),
            run_command(
                &c,
                &["debug", "digest-value", "foo0", "foo1", "foo2", "foo3"]
            )
            .await
        );
    }

    #[tokio::test]
    async fn debug() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "foo0", "f1", "1", "f2", "2", "f3", "3"]).await;
        let _ = run_command(&c, &["set", "foo1", "f1"]).await;
        let _ = run_command(&c, &["rpush", "foo2", "f1"]).await;
        let _ = run_command(&c, &["sadd", "foo3", "f1"]).await;
        match run_command(&c, &["debug", "object", "foo0"]).await {
            Ok(Value::Blob(s)) => {
                let s = String::from_utf8_lossy(&s);
                assert!(s.contains("hashtable"))
            }
            _ => panic!("Unxpected response"),
        };
        match run_command(&c, &["debug", "object", "foo1"]).await {
            Ok(Value::Blob(s)) => {
                let s = String::from_utf8_lossy(&s);
                assert!(s.contains("embstr"));
            }
            _ => panic!("Unxpected response"),
        };
        match run_command(&c, &["debug", "object", "foo2"]).await {
            Ok(Value::Blob(s)) => {
                let s = String::from_utf8_lossy(&s);
                assert!(s.contains("linkedlist"));
            }
            _ => panic!("Unxpected response"),
        };
        match run_command(&c, &["debug", "object", "foo3"]).await {
            Ok(Value::Blob(s)) => {
                let s = String::from_utf8_lossy(&s);
                assert!(s.contains("hashtable"));
            }
            _ => panic!("Unxpected response"),
        };
    }

    #[tokio::test]
    async fn command_info() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Array(vec![Value::Array(vec![
                "CLIENT".into(),
                Value::Integer(-2),
                Value::Array(vec![
                    "admin".into(),
                    "noscript".into(),
                    "random".into(),
                    "loading".into(),
                    "stale".into(),
                ]),
                0.into(),
                0.into(),
                0.into(),
            ])])),
            run_command(&c, &["command", "info", "client"]).await,
        );
        assert_eq!(
            Ok(Value::Array(vec![Value::Array(vec![
                "QUIT".into(),
                1.into(),
                Value::Array(vec![
                    "random".into(),
                    "loading".into(),
                    "stale".into(),
                    "fast".into()
                ]),
                0.into(),
                0.into(),
                0.into(),
            ])])),
            run_command(&c, &["command", "info", "quit"]).await,
        );
    }

    #[tokio::test]
    async fn flush() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "foo0", "f1", "1", "f2", "2", "f3", "3"]).await;
        let _ = run_command(&c, &["set", "foo1", "f1"]).await;
        let _ = run_command(&c, &["rpush", "foo2", "f1"]).await;
        let _ = run_command(&c, &["sadd", "foo3", "f1"]).await;
        assert_eq!(Ok(Value::Integer(4)), run_command(&c, &["dbsize"]).await);
        let _ = run_command(&c, &["flushdb"]).await;
        assert_eq!(Ok(Value::Integer(0)), run_command(&c, &["dbsize"]).await);
    }

    #[tokio::test]
    async fn flushall() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "foo0", "f1", "1", "f2", "2", "f3", "3"]).await;
        let _ = run_command(&c, &["set", "foo1", "f1"]).await;
        let _ = run_command(&c, &["rpush", "foo2", "f1"]).await;
        let _ = run_command(&c, &["sadd", "foo3", "f1"]).await;
        assert_eq!(Ok(Value::Integer(4)), run_command(&c, &["dbsize"]).await);
        let _ = run_command(&c, &["select", "3"]).await;
        let _ = run_command(&c, &["flushall"]).await;
        let _ = run_command(&c, &["select", "0"]).await;
        assert_eq!(Ok(Value::Integer(0)), run_command(&c, &["dbsize"]).await);
    }

    #[tokio::test]
    async fn get_keys_1() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Array(vec!["foo0".into()])),
            run_command(
                &c,
                &["command", "getkeys", "hset", "foo0", "f1", "1", "f2", "2", "f3", "3"]
            )
            .await
        );
    }
    #[tokio::test]
    async fn get_keys_2() {
        let c = create_connection();
        assert_eq!(
            Err(Error::SubCommandNotFound(
                "getkeys".to_owned(),
                "command".to_owned()
            )),
            run_command(&c, &["command", "getkeys"]).await
        );
    }
}
