//! # Pubsub command handlers
use crate::{check_arg, connection::Connection, error::Error, value::Value};
use bytes::Bytes;
use glob::Pattern;

/// Posts a message to the given channel.
pub async fn publish(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.pubsub().publish(&args[1], &args[2]).await.into())
}

/// All pubsub commands
pub async fn pubsub(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match String::from_utf8_lossy(&args[1]).to_lowercase().as_str() {
        "channels" => Ok(Value::Array(
            conn.pubsub()
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
        "numpat" => Ok(conn.pubsub().get_number_of_psubscribers().into()),
        "numsub" => Ok(conn
            .pubsub()
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

/// Subscribes the client to the specified channels.
pub async fn subscribe(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let pubsub = conn.pubsub();

    let channels = &args[1..];

    if check_arg!(args, 0, "PSUBSCRIBE") {
        pubsub.psubscribe(channels, conn)?;
    } else {
        pubsub.subscribe(channels, conn);
    }

    conn.start_pubsub()
}

/// Unsubscribes the client from the given patterns, or from all of them if none is given.
pub async fn punsubscribe(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let channels = if args.len() == 1 {
        conn.pubsub_client().psubscriptions()
    } else {
        (&args[1..])
            .iter()
            .map(|channel| {
                let channel = String::from_utf8_lossy(channel);
                Pattern::new(&channel).map_err(|_| Error::InvalidPattern(channel.to_string()))
            })
            .collect::<Result<Vec<Pattern>, Error>>()?
    };

    Ok(conn.pubsub_client().punsubscribe(&channels, conn).into())
}

/// Unsubscribes the client from the given channels, or from all of them if none is given.
pub async fn unsubscribe(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let channels = if args.len() == 1 {
        conn.pubsub_client().subscriptions()
    } else {
        (&args[1..]).to_vec()
    };

    Ok(conn.pubsub_client().unsubscribe(&channels, conn).into())
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{
            create_connection_and_pubsub, create_new_connection_from_connection, run_command,
        },
        value::Value,
    };
    use tokio::sync::mpsc::Receiver;

    async fn test_subscription_confirmation_and_first_message(
        msg: &str,
        channel: &str,
        recv: &mut Receiver<Value>,
    ) {
        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                channel.into(),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("message".into()),
                channel.into(),
                msg.into()
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn test_subscribe_multiple_channels() {
        let (mut recv, c1) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c1, &["subscribe", "foo", "bar"]).await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "foo".into(),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "bar".into(),
                2.into()
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn test_subscribe_multiple_channels_one_by_one() {
        let (mut recv, c1) = create_connection_and_pubsub();

        assert_eq!(Ok(Value::Ok), run_command(&c1, &["subscribe", "foo"]).await);

        assert_eq!(Ok(Value::Ok), run_command(&c1, &["subscribe", "bar"]).await);

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "foo".into(),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "bar".into(),
                2.into()
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn test_unsubscribe_with_args() {
        let (mut recv, c1) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c1, &["subscribe", "foo", "bar"]).await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c1, &["unsubscribe", "foo", "bar"]).await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "foo".into(),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "subscribe".into(),
                "bar".into(),
                2.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "unsubscribe".into(),
                "foo".into(),
                1.into()
            ])),
            recv.recv().await
        );

        assert_eq!(
            Some(Value::Array(vec![
                "unsubscribe".into(),
                "bar".into(),
                1.into()
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

        test_subscription_confirmation_and_first_message(msg, "foo", &mut sub1).await;
        test_subscription_confirmation_and_first_message(msg, "foo", &mut sub2).await;
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
