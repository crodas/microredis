//! # Pubsub server
//!
//! There is one instance of this mod active per server instance.
use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

type Sender = mpsc::Sender<Value>;
type Subscription = HashMap<u128, Sender>;

/// Pubsub global server structure
#[derive(Debug)]
pub struct Pubsub {
    subscriptions: RwLock<HashMap<Bytes, Subscription>>,
    psubscriptions: RwLock<HashMap<Pattern, Subscription>>,
    number_of_psubscriptions: RwLock<i64>,
}

impl Pubsub {
    /// Creates a new Pubsub instance
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            psubscriptions: RwLock::new(HashMap::new()),
            number_of_psubscriptions: RwLock::new(0),
        }
    }

    /// Returns a list of all channels with subscriptions
    pub fn channels(&self) -> Vec<Bytes> {
        self.subscriptions.read().keys().cloned().collect()
    }

    /// Returns numbers of pattern-subscriptions
    pub fn get_number_of_psubscribers(&self) -> i64 {
        *(self.number_of_psubscriptions.read())
    }

    /// Returns numbers of subscribed for given channels
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

    /// Subscribe to patterns
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

            let _ = conn.pubsub_client().sender().try_send(
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

    /// Publishes a new message. This broadcast to channels subscribers and pattern-subscription
    /// that matches the published channel.
    pub async fn publish(&self, channel: &Bytes, message: &Bytes) -> u32 {
        let mut i = 0;

        if let Some(subs) = self.subscriptions.read().get(channel) {
            for sender in subs.values() {
                if sender
                    .try_send(Value::Array(vec![
                        "message".into(),
                        Value::Blob(channel.clone()),
                        Value::Blob(message.clone()),
                    ]))
                    .is_ok()
                {
                    i += 1;
                }
            }
        }

        let str_channel = String::from_utf8_lossy(channel);

        for (pattern, subs) in self.psubscriptions.read().iter() {
            if !pattern.matches(&str_channel) {
                continue;
            }

            for sub in subs.values() {
                let _ = sub.try_send(Value::Array(vec![
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

    /// Unsubscribe from a pattern subscription
    pub fn punsubscribe(&self, channels: &[Pattern], conn: &Connection) -> u32 {
        let mut all_subs = self.psubscriptions.write();
        let conn_id = conn.id();
        let mut removed = 0;
        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = all_subs.get_mut(channel) {
                    if let Some(sender) = subs.remove(&conn_id) {
                        let _ = sender.try_send(Value::Array(vec![
                            "punsubscribe".into(),
                            channel.as_str().into(),
                            1.into(),
                        ]));
                        removed += 1;
                    }
                    if subs.is_empty() {
                        all_subs.remove(channel);
                    }
                }
            })
            .for_each(drop);

        removed
    }

    /// Subscribe connection to channels
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

                let _ = conn.pubsub_client().sender().try_send(
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

    /// Removes connection subscription to channels.
    pub fn unsubscribe(&self, channels: &[Bytes], conn: &Connection) -> u32 {
        let mut all_subs = self.subscriptions.write();
        let conn_id = conn.id();
        let mut removed = 0;
        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = all_subs.get_mut(channel) {
                    if let Some(sender) = subs.remove(&conn_id) {
                        let _ = sender.try_send(Value::Array(vec![
                            "unsubscribe".into(),
                            Value::Blob(channel.clone()),
                            1.into(),
                        ]));
                        removed += 1;
                    }
                    if subs.is_empty() {
                        all_subs.remove(channel);
                    }
                }
            })
            .for_each(drop);

        removed
    }
}
