//! # Transaction command handlers
use crate::{
    connection::{Connection, ConnectionStatus},
    error::Error,
    value::Value,
};
use bytes::Bytes;

/// Flushes all previously queued commands in a transaction and restores the connection state to
/// normal.
///
/// If WATCH was used, DISCARD unwatches all keys watched by the connection
pub async fn discard(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.stop_transaction()
}

/// Marks the start of a transaction block. Subsequent commands will be queued for atomic execution
/// using EXEC.
pub async fn multi(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.start_transaction()
}

/// Executes all previously queued commands in a transaction and restores the connection state to
/// normal.
///
/// When using WATCH, EXEC will execute commands only if the watched keys were not modified,
/// allowing for a check-and-set mechanism.
pub async fn exec(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    if conn.status() != ConnectionStatus::Multi {
        return Err(Error::NotInTx);
    }

    if conn.did_keys_change() {
        let _ = conn.stop_transaction();
        return Ok(Value::Null);
    }

    let db = conn.db();
    let locked_keys = conn.get_tx_keys();

    db.lock_keys(&locked_keys);

    let mut results = vec![];

    if let Some(commands) = conn.get_queue_commands() {
        let dispatcher = conn.all_connections().get_dispatcher();
        for args in commands.iter() {
            let result = dispatcher
                .execute(conn, args)
                .await
                .unwrap_or_else(|x| x.into());
            results.push(result);
        }
    }

    db.unlock_keys(&locked_keys);
    let _ = conn.stop_transaction();

    Ok(results.into())
}

/// Marks the given keys to be watched for conditional execution of a transaction.
pub async fn watch(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.watch_key(
        &(&args[1..])
            .iter()
            .map(|key| (key, conn.db().get_version(key)))
            .collect::<Vec<(&Bytes, u128)>>(),
    );
    Ok(Value::Ok)
}

/// Flushes all the previously watched keys for a transaction.
///
/// If you call EXEC or DISCARD, there's no need to manually call UNWATCH.
pub async fn unwatch(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.discard_watched_keys();
    Ok(Value::Ok)
}

#[cfg(test)]
mod test {
    use crate::dispatcher::Dispatcher;
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn test_exec() {
        let c = create_connection();

        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["set", "foo", "foo"]).await
        );
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Array(vec![
                Value::Null,
                Value::Ok,
                Value::Blob("foo".into()),
            ])),
            run_command(&c, &["exec"]).await
        );
    }

    #[tokio::test]
    async fn test_nested_multi() {
        let c = create_connection();

        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(Err(Error::NestedTx), run_command(&c, &["multi"]).await);
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["set", "foo", "foo"]).await
        );
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Array(vec![
                Value::Null,
                Value::Ok,
                Value::Blob("foo".into()),
            ])),
            run_command(&c, &["exec"]).await
        );
    }

    #[tokio::test]
    async fn test_discard() {
        let c = create_connection();

        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["set", "foo", "foo"]).await
        );
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(Ok(Value::Ok), run_command(&c, &["discard"]).await);
        assert_eq!(Err(Error::NotInTx), run_command(&c, &["exec"]).await);
    }

    #[tokio::test]
    async fn test_exec_watch_changes() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["watch", "foo", "bar"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["set", "foo", "bar"]).await);
        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["set", "foo", "foo"]).await
        );
        assert_eq!(Ok(Value::Queued), run_command(&c, &["get", "foo"]).await);
        assert_eq!(Ok(Value::Null), run_command(&c, &["exec"]).await);
    }

    #[test]
    fn test_extract_keys() {
        assert_eq!(vec!["foo"], get_keys(&["get", "foo"]));
        assert_eq!(vec!["foo"], get_keys(&["set", "foo", "bar"]));
        assert_eq!(vec!["foo", "bar"], get_keys(&["mget", "foo", "bar"]));
        assert_eq!(
            vec!["key", "key1", "key2"],
            get_keys(&["SINTERSTORE", "key", "key1", "key2"])
        );
    }

    #[tokio::test]
    async fn test_exec_brpop_not_waiting() {
        let c = create_connection();

        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["brpop", "foo", "1000"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec![Value::Null,])),
            run_command(&c, &["exec"]).await
        );
    }

    #[tokio::test]
    async fn test_exec_blpop_not_waiting() {
        let c = create_connection();

        assert_eq!(Ok(Value::Ok), run_command(&c, &["multi"]).await);
        assert_eq!(
            Ok(Value::Queued),
            run_command(&c, &["blpop", "foo", "1000"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec![Value::Null,])),
            run_command(&c, &["exec"]).await
        );
    }

    fn get_keys(args: &[&str]) -> Vec<Bytes> {
        let args: Vec<Bytes> = args.iter().map(|s| Bytes::from(s.to_string())).collect();
        let d = Dispatcher::new();
        if let Ok(cmd) = d.get_handler(&args) {
            cmd.get_keys(&args).iter().map(|k| (*k).clone()).collect()
        } else {
            vec![]
        }
    }
}
