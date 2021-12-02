use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

type Sender = mpsc::UnboundedSender<Value>;
type Subscription = HashMap<u128, Sender>;

#[derive(Debug)]
pub struct Pubsub {
    subscriptions: RwLock<HashMap<Bytes, Subscription>>,
    psubscriptions: RwLock<HashMap<Pattern, Subscription>>,
    number_of_psubscriptions: RwLock<i64>,
}

impl Pubsub {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            psubscriptions: RwLock::new(HashMap::new()),
            number_of_psubscriptions: RwLock::new(0),
        }
    }

    pub fn channels(&self) -> Vec<Bytes> {
        self.subscriptions.read().keys().cloned().collect()
    }

    pub fn get_number_of_psubscribers(&self) -> i64 {
        *(self.number_of_psubscriptions.read())
    }

    pub fn get_number_of_subscribers(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        let subscribers = self.subscriptions.read();
        let mut ret = vec![];
        for channel in channels.iter() {
            if let Some(subs) = subscribers.get(channel) {
                ret.push((channel.clone(), subs.len()));
            } else {
                ret.push((channel.clone(), 0));
            }
        }

        ret
    }

    pub fn psubscribe(&self, channels: &[Bytes], conn: &Connection) -> Result<(), Error> {
        let mut subscriptions = self.psubscriptions.write();

        for bytes_channel in channels.iter() {
            let channel = String::from_utf8_lossy(bytes_channel);
            let channel =
                Pattern::new(&channel).map_err(|_| Error::InvalidPattern(channel.to_string()))?;

            if let Some(subs) = subscriptions.get_mut(&channel) {
                subs.insert(conn.id(), conn.pubsub_client().sender());
            } else {
                let mut h = HashMap::new();
                h.insert(conn.id(), conn.pubsub_client().sender());
                subscriptions.insert(channel.clone(), h);
            }
            if !conn.pubsub_client().is_psubcribed() {
                let mut psubs = self.number_of_psubscriptions.write();
                conn.pubsub_client().make_psubcribed();
                *psubs += 1;
            }

            let _ = conn.pubsub_client().sender().send(
                vec![
                    "psubscribe".into(),
                    Value::Blob(bytes_channel.clone()),
                    conn.pubsub_client().new_psubscription(&channel).into(),
                ]
                .into(),
            );
        }

        Ok(())
    }

    pub async fn publish(&self, channel: &Bytes, message: &Bytes) -> u32 {
        let mut i = 0;

        if let Some(subs) = self.subscriptions.read().get(channel) {
            for sender in subs.values() {
                let _ = sender.send(Value::Array(vec![
                    "message".into(),
                    Value::Blob(channel.clone()),
                    Value::Blob(message.clone()),
                ]));
                i += 1;
            }
        }

        let str_channel = String::from_utf8_lossy(channel);

        for (pattern, subs) in self.psubscriptions.read().iter() {
            if !pattern.matches(&str_channel) {
                continue;
            }

            for sub in subs.values() {
                let _ = sub.send(Value::Array(vec![
                    "pmessage".into(),
                    pattern.as_str().into(),
                    Value::Blob(channel.clone()),
                    Value::Blob(message.clone()),
                ]));
                i += 1;
            }
        }

        i
    }

    pub fn subscribe(&self, channels: &[Bytes], conn: &Connection) {
        let mut subscriptions = self.subscriptions.write();

        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = subscriptions.get_mut(channel) {
                    subs.insert(conn.id(), conn.pubsub_client().sender());
                } else {
                    let mut h = HashMap::new();
                    h.insert(conn.id(), conn.pubsub_client().sender());
                    subscriptions.insert(channel.clone(), h);
                }

                let _ = conn.pubsub_client().sender().send(
                    vec![
                        "subscribe".into(),
                        Value::Blob(channel.clone()),
                        conn.pubsub_client().new_subscription(channel).into(),
                    ]
                    .into(),
                );
            })
            .for_each(drop);
    }

    pub fn unsubscribe(&self, channels: &[Bytes], conn: &Connection) -> u32 {
        let mut subs = self.subscriptions.write();
        let conn_id = conn.id();
        let mut removed = 0;
        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = subs.get_mut(channel) {
                    if let Some(sender) = subs.remove(&conn_id) {
                        let _ = sender.send(Value::Array(vec![
                            "unsubscribe".into(),
                            Value::Blob(channel.clone()),
                            1.into(),
                        ]));
                        removed += 1;
                    }
                }
            })
            .for_each(drop);

        removed
    }
}
