use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;

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
