use crate::{connection::Connection, value::Value};
use bytes::Bytes;
use std::{collections::HashMap, sync::RwLock};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Pubsub {
    subscriptions: RwLock<HashMap<Bytes, Vec<mpsc::UnboundedSender<Value>>>>,
}

impl Pubsub {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, channel: &Bytes, conn: &Connection) -> u32 {
        let mut subscriptions = self.subscriptions.write().unwrap();

        if let Some(subs) = subscriptions.get_mut(channel) {
            subs.push(conn.get_pubsub_sender());
        } else {
            subscriptions.insert(channel.clone(), vec![conn.get_pubsub_sender()]);
        }

        conn.get_subscription_id()
    }

    pub async fn publish(&self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut i = 0;

        let subscriptions = self.subscriptions.read().unwrap();

        if let Some(subs) = subscriptions.get(channel) {
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

        i
    }
}
