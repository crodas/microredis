use crate::{connection::Connection, dispatcher::Dispatcher, error::Error, value::Value};
use bytes::Bytes;
use std::ops::Deref;

pub async fn discard(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.stop_transaction()
}

pub async fn multi(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.start_transaction()
}

pub async fn exec(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    if !conn.in_transaction() {
        return Err(Error::NotInTx);
    }

    if conn.did_keys_change() {
        let _ = conn.stop_transaction();
        return Ok(Value::Null);
    }

    let keys_to_block = conn.get_tx_keys();

    let mut results = vec![];

    if let Some(commands) = conn.get_queue_commands() {
        for args in commands.iter() {
            let result = match Dispatcher::new(args) {
                Ok(handler) => handler
                    .deref()
                    .execute(conn, args)
                    .await
                    .unwrap_or_else(|x| x.into()),
                Err(err) => err.into(),
            };
            results.push(result);
        }
    }

    Ok(results.into())
}

pub async fn watch(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.watch_key(
        &(&args[1..])
            .iter()
            .map(|key| (key, conn.db().get_version(key)))
            .collect::<Vec<(&Bytes, u128)>>(),
    );
    Ok(Value::Ok)
}

pub async fn unwatch(conn: &Connection, _: &[Bytes]) -> Result<Value, Error> {
    conn.discard_watched_keys();
    Ok(Value::Ok)
}

#[cfg(test)]
mod test {
    use crate::dispatcher::Dispatcher;
    use bytes::Bytes;
    use std::ops::Deref;

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

    fn get_keys(args: &[&str]) -> Vec<Bytes> {
        let args: Vec<Bytes> = args.iter().map(|s| Bytes::from(s.to_string())).collect();
        Dispatcher::new(&args)
            .unwrap()
            .deref()
            .get_keys(&args)
            .iter()
            .map(|k| (*k).clone())
            .collect()
    }
}
