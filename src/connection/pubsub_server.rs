//! # Pubsub server
//!
//! There is one instance of this mod active per server instance.
use crate::{connection::Connection, error::Error, value::Value};
use bytes::{Bytes, BytesMut};
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

type Sender = mpsc::Sender<Value>;
type Subscription = HashMap<u128, Sender>;

/// Pubsub global server structure
#[derive(Debug)]
pub struct Pubsub {
    subscriptions: RwLock<HashMap<Bytes, Subscription>>,
    psubscriptions: RwLock<HashMap<Pattern, Subscription>>,
}

impl Pubsub {
    /// Creates a new Pubsub instance
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            psubscriptions: RwLock::new(HashMap::new()),
        }
    }

    /// Returns a list of all channels with subscriptions
    pub fn channels(&self) -> Vec<Bytes> {
        self.subscriptions.read().keys().cloned().collect()
    }

    /// Returns numbers of pattern-subscriptions
    pub fn get_number_of_psubscribers(&self) -> usize {
        self.psubscriptions.read().len()
    }

    /// Returns numbers of subscribed for given channels
    pub fn get_number_of_subscribers(&self, channels: &VecDeque<Bytes>) -> Vec<(Bytes, usize)> {
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
    pub fn psubscribe(&self, channels: VecDeque<Bytes>, conn: &Connection) -> Result<(), Error> {
        let mut subscriptions = self.psubscriptions.write();

        for bytes_channel in channels.into_iter() {
            let channel = String::from_utf8_lossy(&bytes_channel);
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
                conn.pubsub_client().make_psubcribed();
            }

            conn.pubsub_client().new_psubscription(&channel);

            conn.append_response(
                vec![
                    "psubscribe".into(),
                    Value::Blob(bytes_channel),
                    conn.pubsub_client().total_subs().into(),
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
                        Value::new(&channel),
                        Value::new(&message),
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
                    Value::new(channel),
                    Value::new(message),
                ]));
                i += 1;
            }
        }

        i
    }

    /// Unsubscribe from a pattern subscription
    pub fn punsubscribe(&self, channels: &[Pattern], conn: &Connection, notify: bool) {
        if channels.is_empty() {
            return conn.append_response(Value::Array(vec![
                "punsubscribe".into(),
                Value::Null,
                0usize.into(),
            ]));
        }
        let mut all_subs = self.psubscriptions.write();
        let conn_id = conn.id();
        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = all_subs.get_mut(channel) {
                    subs.remove(&conn_id);
                    if subs.is_empty() {
                        all_subs.remove(channel);
                    }
                }

                if notify {
                    conn.append_response(Value::Array(vec![
                        "punsubscribe".into(),
                        channel.as_str().into(),
                        conn.pubsub_client().total_subs().into(),
                    ]));
                }
            })
            .for_each(drop);
    }

    /// Subscribe connection to channels
    pub fn subscribe(&self, channels: VecDeque<Bytes>, conn: &Connection) {
        let mut subscriptions = self.subscriptions.write();
        let total_psubs = self.psubscriptions.read().len();

        channels
            .into_iter()
            .map(|channel| {
                if let Some(subs) = subscriptions.get_mut(&channel) {
                    subs.insert(conn.id(), conn.pubsub_client().sender());
                } else {
                    let mut h = HashMap::new();
                    h.insert(conn.id(), conn.pubsub_client().sender());
                    subscriptions.insert(channel.clone(), h);
                }

                conn.pubsub_client().new_subscription(&channel);
                conn.append_response(
                    vec![
                        "subscribe".into(),
                        Value::Blob(channel),
                        conn.pubsub_client().total_subs().into(),
                    ]
                    .into(),
                );
            })
            .for_each(drop);
    }

    /// Removes connection subscription to channels.
    pub fn unsubscribe(&self, channels: &[Bytes], conn: &Connection, notify: bool) {
        if channels.is_empty() {
            return conn.append_response(Value::Array(vec![
                "unsubscribe".into(),
                Value::Null,
                0usize.into(),
            ]));
        }
        let mut all_subs = self.subscriptions.write();
        let total_psubs = self.psubscriptions.read().len();
        let conn_id = conn.id();
        channels
            .iter()
            .map(|channel| {
                if let Some(subs) = all_subs.get_mut(channel) {
                    subs.remove(&conn_id);
                    if subs.is_empty() {
                        all_subs.remove(channel);
                    }
                }
                if notify {
                    conn.append_response(Value::Array(vec![
                        "unsubscribe".into(),
                        Value::new(&channel),
                        (all_subs.len() + total_psubs).into(),
                    ]));
                }
            })
            .for_each(drop);
    }
}
