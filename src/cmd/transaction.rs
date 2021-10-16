use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;

pub async fn watch(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let _ = (&args[1..])
        .iter()
        .map(|key| {
            (key.clone(), conn.db().get_version(key))
        })
        .collect::<Vec<(Bytes, u128)>>();
    Ok(Value::OK)
}
