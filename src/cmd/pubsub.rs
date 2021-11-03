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
    let sender = conn.get_pubsub_sender();

    (&args[1..])
        .iter()
        .map(|channel| {
            let id = pubsub.subscribe(channel, conn);
            let _ = sender.send(Value::Array(vec![
                "subscribe".into(),
                Value::Blob(channel.clone()),
                id.into(),
            ]));
        })
        .for_each(drop);
    Ok(Value::Ok)
}
