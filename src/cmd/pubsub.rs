use crate::{check_arg, connection::Connection, error::Error, value::Value};
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
    let sender = conn.get_pubsub_sender();
    let is_pattern = check_arg!(args, 0, "PSUBSCRIBE");

    for channel in (&args[1..]).iter() {
        let id = if is_pattern {
            pubsub.psubscribe(channel, conn)?
        } else {
            pubsub.subscribe(channel, conn)
        };
        let _ = sender.send(Value::Array(vec![
            (if is_pattern {
                "psubscribe"
            } else {
                "subscribe"
            })
            .into(),
            Value::Blob(channel.clone()),
            id.into(),
        ]));
    }

    conn.start_pubsub()
}
