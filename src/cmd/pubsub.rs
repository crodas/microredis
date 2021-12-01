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

pub async fn pubsub(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match String::from_utf8_lossy(&args[1]).to_lowercase().as_str() {
        "channels" => Ok(Value::Array(
            conn.db()
                .get_pubsub()
                .channels()
                .iter()
                .map(|v| Value::Blob(v.clone()))
                .collect(),
        )),
        "numpat" => Ok(conn.db().get_pubsub().get_number_of_psubscribed().into()),
        cmd => Ok(Value::Err(
            "ERR".to_owned(),
            format!(
                "Unknown subcommand or wrong number of arguments for '{}'. Try PUBSUB HELP.",
                cmd
            ),
        )),
    }
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
