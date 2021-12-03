use super::Connection;
use crate::value::Value;
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct PubsubClient {
    meta: RwLock<MetaData>,
    sender: mpsc::UnboundedSender<Value>,
}

#[derive(Debug)]
struct MetaData {
    subscriptions: HashMap<Bytes, bool>,
    psubscriptions: HashMap<Pattern, bool>,
    is_psubcribed: bool,
    id: usize,
}

impl PubsubClient {
    pub fn new(sender: mpsc::UnboundedSender<Value>) -> Self {
        Self {
            meta: RwLock::new(MetaData {
                subscriptions: HashMap::new(),
                psubscriptions: HashMap::new(),
                is_psubcribed: false,
                id: 0,
            }),
            sender,
        }
    }

    pub fn punsubscribe(&self, channels: &[Pattern], conn: &Connection) -> u32 {
        let mut meta = self.meta.write();
        channels
            .iter()
            .map(|channel| meta.psubscriptions.remove(channel))
            .for_each(drop);
        if meta.psubscriptions.len() + meta.subscriptions.len() == 0 {
            drop(meta);
            conn.reset();
        }
        conn.pubsub().punsubscribe(channels, conn)
    }

    pub fn unsubscribe(&self, channels: &[Bytes], conn: &Connection) -> u32 {
        let mut meta = self.meta.write();
        channels
            .iter()
            .map(|channel| meta.subscriptions.remove(channel))
            .for_each(drop);
        if meta.psubscriptions.len() + meta.subscriptions.len() == 0 {
            drop(meta);
            conn.reset();
        }
        conn.pubsub().unsubscribe(channels, conn)
    }

    pub fn subscriptions(&self) -> Vec<Bytes> {
        self.meta
            .read()
            .subscriptions
            .keys()
            .cloned()
            .collect::<Vec<Bytes>>()
    }

    pub fn psubscriptions(&self) -> Vec<Pattern> {
        self.meta
            .read()
            .psubscriptions
            .keys()
            .cloned()
            .collect::<Vec<Pattern>>()
    }

    pub fn new_subscription(&self, channel: &Bytes) -> usize {
        let mut meta = self.meta.write();
        meta.subscriptions.insert(channel.clone(), true);
        meta.id += 1;
        meta.id
    }

    pub fn new_psubscription(&self, channel: &Pattern) -> usize {
        let mut meta = self.meta.write();
        meta.psubscriptions.insert(channel.clone(), true);
        meta.id += 1;
        meta.id
    }

    pub fn is_psubcribed(&self) -> bool {
        self.meta.read().is_psubcribed
    }

    pub fn make_psubcribed(&self) {
        self.meta.write().is_psubcribed = true;
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<Value> {
        self.sender.clone()
    }
}
