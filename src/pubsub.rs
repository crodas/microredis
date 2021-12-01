use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Pubsub {
    subscriptions: RwLock<HashMap<Bytes, Vec<mpsc::UnboundedSender<Value>>>>,
    psubscriptions: RwLock<HashMap<Pattern, Vec<mpsc::UnboundedSender<Value>>>>,
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

    pub fn psubscribe(&self, channel: &Bytes, conn: &Connection) -> Result<u32, Error> {
        let mut subscriptions = self.psubscriptions.write();
        let channel = String::from_utf8_lossy(channel);
        let channel =
            Pattern::new(&channel).map_err(|_| Error::InvalidPattern(channel.to_string()))?;

        if let Some(subs) = subscriptions.get_mut(&channel) {
            subs.push(conn.get_pubsub_sender());
        } else {
            subscriptions.insert(channel.clone(), vec![conn.get_pubsub_sender()]);
        }
        if !conn.is_psubcribed() {
            let mut psubs = self.number_of_psubscriptions.write();
            conn.make_psubcribed();
            *psubs += 1;
        }

        Ok(conn.get_subscription_id())
    }

    pub fn subscribe(&self, channel: &Bytes, conn: &Connection) -> u32 {
        let mut subscriptions = self.subscriptions.write();

        if let Some(subs) = subscriptions.get_mut(channel) {
            subs.push(conn.get_pubsub_sender());
        } else {
            subscriptions.insert(channel.clone(), vec![conn.get_pubsub_sender()]);
        }

        conn.get_subscription_id()
    }

    pub async fn publish(&self, channel: &Bytes, message: &Bytes) -> u32 {
        let mut i = 0;

        if let Some(subs) = self.subscriptions.read().get(channel) {
            for sub in subs.iter() {
                let message = Value::Array(vec![
                    "message".into(),
                    Value::Blob(channel.clone()),
                    Value::Blob(message.clone()),
                ]);
                let _ = sub.send(message);
                i += 1;
            }
        }

        let str_channel = String::from_utf8_lossy(channel);

        for (pattern, subs) in self.psubscriptions.read().iter() {
            if !pattern.matches(&str_channel) {
                continue;
            }

            for sub in subs.iter() {
                let message = Value::Array(vec![
                    "pmessage".into(),
                    pattern.as_str().into(),
                    Value::Blob(channel.clone()),
                    Value::Blob(message.clone()),
                ]);
                let _ = sub.send(message);
                i += 1;
            }
        }

        i
    }
}
