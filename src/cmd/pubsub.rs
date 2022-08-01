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
                .map(|v| Value::new(&v))
                .collect(),
        )),
        "help" => super::help::pubsub(),
        "numpat" => Ok(conn.pubsub().get_number_of_psubscribers().into()),
        "numsub" => Ok(conn
            .pubsub()
            .get_number_of_subscribers(&args[2..])
            .iter()
            .map(|(channel, subs)| vec![Value::new(&channel), (*subs).into()])
            .flatten()
            .collect::<Vec<Value>>()
            .into()),
        cmd => Err(Error::SubCommandNotFound(
            cmd.into(),
            String::from_utf8_lossy(&args[0]).into(),
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

    let _ = conn.pubsub_client().punsubscribe(&channels, conn);

    Ok(Value::Ignore)
}

/// Unsubscribes the client from the given channels, or from all of them if none is given.
pub async fn unsubscribe(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let channels = if args.len() == 1 {
        conn.pubsub_client().subscriptions()
    } else {
        (&args[1..]).to_vec()
    };

    let _ = conn.pubsub_client().unsubscribe(&channels, conn);
    Ok(Value::Ignore)
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{
            create_connection, create_connection_and_pubsub, create_new_connection_from_connection,
            run_command,
        },
        error::Error,
        value::Value,
    };
    use std::convert::TryInto;
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
            Ok(Value::Ignore),
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

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c1, &["subscribe", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c1, &["subscribe", "bar"]).await
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
    async fn test_unsubscribe_with_no_args() {
        let (mut recv, c1) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c1, &["subscribe", "foo", "bar"]).await
        );

        assert_eq!(Ok(Value::Ignore), run_command(&c1, &["unsubscribe"]).await);

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

        let x: Vec<Vec<Value>> = vec![
            recv.recv().await.unwrap().try_into().unwrap(),
            recv.recv().await.unwrap().try_into().unwrap(),
        ];

        assert_eq!(Value::Blob("unsubscribe".into()), x[0][0]);
        assert_eq!(Value::Integer(1), x[0][2]);
        assert_eq!(Value::Blob("unsubscribe".into()), x[1][0]);
        assert_eq!(Value::Integer(0), x[1][2]);

        assert!(x[0][1] == "foo".into() || x[0][1] == "bar".into());
        assert!(x[1][1] == "foo".into() || x[1][1] == "bar".into());
    }

    #[tokio::test]
    async fn test_unsubscribe_with_args() {
        let (mut recv, c1) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c1, &["subscribe", "foo", "bar"]).await
        );

        assert_eq!(
            Ok(Value::Ignore),
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
                0.into()
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn pubsub_publish() {
        let (mut sub1, c1) = create_connection_and_pubsub();
        let (mut sub2, c2) = create_new_connection_from_connection(&c1);
        let (_, c3) = create_new_connection_from_connection(&c1);

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c1, &["subscribe", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c2, &["subscribe", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec!["foo".into(), 2.into()])),
            run_command(&c2, &["pubsub", "numsub", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec!["foo".into()])),
            run_command(&c2, &["pubsub", "channels"]).await
        );

        let msg = "foo - message";

        let _ = run_command(&c3, &["publish", "foo", msg]).await;

        test_subscription_confirmation_and_first_message(msg, "foo", &mut sub1).await;
        test_subscription_confirmation_and_first_message(msg, "foo", &mut sub2).await;
    }

    #[tokio::test]
    async fn pubsub_not_found() {
        let c1 = create_connection();
        assert_eq!(
            Err(Error::SubCommandNotFound("foo".into(), "pubsub".into())),
            run_command(&c1, &["pubsub", "foo"]).await
        );
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
            Ok(Value::Integer(3)),
            run_command(&c1, &["pubsub", "numpat"]).await
        );

        let _ = run_command(&c2, &["punsubscribe", "barx*"]).await;
        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c2, &["pubsub", "numpat"]).await
        );

        let _ = run_command(&c2, &["punsubscribe", "bar*"]).await;
        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c2, &["pubsub", "numpat"]).await
        );

        let _ = run_command(&c2, &["punsubscribe"]).await;
        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c2, &["pubsub", "numpat"]).await
        );
    }
}
