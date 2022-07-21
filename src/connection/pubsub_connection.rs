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
    sender: mpsc::Sender<Value>,
}

/// Metadata associated with a pubsub client
#[derive(Debug)]
struct MetaData {
    subscriptions: HashMap<Bytes, bool>,
    psubscriptions: HashMap<Pattern, bool>,
    is_psubcribed: bool,
}

impl PubsubClient {
    /// Creates a new pubsub client instance
    pub fn new(sender: mpsc::Sender<Value>) -> Self {
        Self {
            meta: RwLock::new(MetaData {
                subscriptions: HashMap::new(),
                psubscriptions: HashMap::new(),
                is_psubcribed: false,
            }),
            sender,
        }
    }

    /// Unsubscribe from pattern subscriptions
    pub fn punsubscribe(&self, channels: &[Pattern], conn: &Connection) {
        let mut meta = self.meta.write();
        channels
            .iter()
            .map(|channel| meta.psubscriptions.remove(channel))
            .for_each(drop);
        drop(meta);
        conn.pubsub().punsubscribe(channels, conn, true);

        if self.total_subs() == 0 {
            conn.reset();
        }
    }

    /// Unsubscribe from channels
    pub fn unsubscribe(&self, channels: &[Bytes], conn: &Connection) {
        let mut meta = self.meta.write();
        channels
            .iter()
            .map(|channel| meta.subscriptions.remove(channel))
            .for_each(drop);
        drop(meta);
        conn.pubsub().unsubscribe(channels, conn, true);

        if self.total_subs() == 0 {
            conn.reset();
        }
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

    /// Return total number of subscriptions + psubscription
    pub fn total_subs(&self) -> usize {
        let meta = self.meta.read();
        meta.subscriptions.len() + meta.psubscriptions.len()
    }

    /// Creates a new subscription
    pub fn new_subscription(&self, channel: &Bytes) {
        let mut meta = self.meta.write();
        meta.subscriptions.insert(channel.clone(), true);
    }

    /// Creates a new pattern subscription
    pub fn new_psubscription(&self, channel: &Pattern) {
        let mut meta = self.meta.write();
        meta.psubscriptions.insert(channel.clone(), true);
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
    pub fn sender(&self) -> mpsc::Sender<Value> {
        self.sender.clone()
    }

    /// Sends a message
    #[inline]
    pub fn send(&self, message: Value) {
        let _ = self.sender.try_send(message);
    }
}
