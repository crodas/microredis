//! # Pubsub client
//!
//! Each connection has a pubsub client which is created, even on normal connection mode.
use super::Connection;
use crate::value::Value;
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Pubsubclient
#[derive(Debug)]
pub struct PubsubClient {
    meta: RwLock<MetaData>,
    sender: mpsc::UnboundedSender<Value>,
}

/// Metadata associated with a pubsub client
#[derive(Debug)]
struct MetaData {
    subscriptions: HashMap<Bytes, bool>,
    psubscriptions: HashMap<Pattern, bool>,
    is_psubcribed: bool,
    id: usize,
}

impl PubsubClient {
    /// Creates a new pubsub client instance
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

    /// Unsubscribe from pattern subscriptions
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

    /// Unsubscribe from channels
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

    /// Return list of subscriptions for this connection
    pub fn subscriptions(&self) -> Vec<Bytes> {
        self.meta
            .read()
            .subscriptions
            .keys()
            .cloned()
            .collect::<Vec<Bytes>>()
    }

    /// Return list of pattern subscriptions
    pub fn psubscriptions(&self) -> Vec<Pattern> {
        self.meta
            .read()
            .psubscriptions
            .keys()
            .cloned()
            .collect::<Vec<Pattern>>()
    }

    /// Creates a new subscription and returns the ID for this new subscription.
    pub fn new_subscription(&self, channel: &Bytes) -> usize {
        let mut meta = self.meta.write();
        meta.subscriptions.insert(channel.clone(), true);
        meta.id += 1;
        meta.id
    }

    /// Creates a new pattern subscription and returns the ID for this new subscription.
    pub fn new_psubscription(&self, channel: &Pattern) -> usize {
        let mut meta = self.meta.write();
        meta.psubscriptions.insert(channel.clone(), true);
        meta.id += 1;
        meta.id
    }

    /// Does this connection has a pattern subscription?
    pub fn is_psubcribed(&self) -> bool {
        self.meta.read().is_psubcribed
    }

    /// Keeps a record about this connection using pattern suscription
    pub fn make_psubcribed(&self) {
        self.meta.write().is_psubcribed = true;
    }

    /// Returns a copy of the pubsub sender. This sender object can be used to send messages (from
    /// other connections) to this connection.
    pub fn sender(&self) -> mpsc::UnboundedSender<Value> {
        self.sender.clone()
    }
}
