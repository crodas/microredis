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
        "help" => Ok(Value::Array(vec![
            Value::String("PUBSUB <subcommand> arg arg ... arg. Subcommands are:".to_owned()),
            Value::String("CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all).".to_owned()),
            Value::String("NUMPAT -- Return number of subscriptions to patterns.".to_owned()),
            Value::String("NUMSUB [channel-1 .. channel-N] -- Returns the number of subscribers for the specified channels (excluding patterns, default: none).".to_owned()),
        ])),
        "numpat" => Ok(conn.db().get_pubsub().get_number_of_psubscribers().into()),
        "numsub" => Ok(conn
            .db()
            .get_pubsub()
            .get_number_of_subscribers(&args[2..])
            .iter()
            .map(|(channel, subs)| vec![Value::Blob(channel.clone()), (*subs).into()])
            .flatten()
            .collect::<Vec<Value>>()
            .into()),
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

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{
            create_connection_and_pubsub, create_new_connection_from_connection, run_command,
        },
        value::Value,
    };
    use tokio::sync::mpsc::UnboundedReceiver;

    async fn test_subscription_confirmation_and_first_message(
        msg: &str,
        recv: &mut UnboundedReceiver<Value>,
    ) {
        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("subscribe".into()),
                Value::Blob("foo".into()),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("message".into()),
                Value::Blob("foo".into()),
                msg.into()
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn pubsub_publish() {
        let (mut sub1, c1) = create_connection_and_pubsub();
        let (mut sub2, c2) = create_new_connection_from_connection(&c1);
        let (_, c3) = create_new_connection_from_connection(&c1);

        assert_eq!(Ok(Value::Ok), run_command(&c1, &["subscribe", "foo"]).await);
        assert_eq!(Ok(Value::Ok), run_command(&c2, &["subscribe", "foo"]).await);

        let msg = "foo - message";

        let _ = run_command(&c3, &["publish", "foo", msg]).await;

        test_subscription_confirmation_and_first_message(&msg, &mut sub1).await;
        test_subscription_confirmation_and_first_message(&msg, &mut sub2).await;
    }

    #[tokio::test]
    async fn pubsub_numpat() {
        let (_, c1) = create_connection_and_pubsub();
        let (_, c2) = create_new_connection_from_connection(&c1);

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c1, &["pubsub", "numpat"]).await
        );

        let _ = run_command(&c2, &["psubscribe", "foo", "bar*", "xxx*"]).await;

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c1, &["pubsub", "numpat"]).await
        );
    }
}
