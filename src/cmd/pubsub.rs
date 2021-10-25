use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;

pub async fn publish(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn
        .db()
        .get_pubsub()
        .publish(&args[1], &args[2])
        .await
        .into())
}

pub async fn subscribe(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let pubsub = conn.db().get_pubsub();
    (&args[1..])
        .iter()
        .map(|channel| {
            pubsub.subscribe(channel, conn);
        })
        .for_each(drop);
    Ok(Value::Ok)
}
