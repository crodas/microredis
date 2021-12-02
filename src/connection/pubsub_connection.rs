use crate::value::Value;
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct PubsubClient {
    meta: RwLock<MetaData>,
    sender: mpsc::UnboundedSender<Value>,
}

#[derive(Debug)]
struct MetaData {
    subscriptions: Vec<Bytes>,
    psubscriptions: Vec<Pattern>,
    is_psubcribed: bool,
    id: usize,
}

impl PubsubClient {
    pub fn new(sender: mpsc::UnboundedSender<Value>) -> Self {
        Self {
            meta: RwLock::new(MetaData {
                subscriptions: vec![],
                psubscriptions: vec![],
                is_psubcribed: false,
                id: 0,
            }),
            sender,
        }
    }

    pub fn subscriptions(&self) -> Vec<Bytes> {
        self.meta.read().subscriptions.clone()
    }

    pub fn new_subscription(&self, channel: &Bytes) -> usize {
        let mut meta = self.meta.write();
        meta.subscriptions.push(channel.clone());
        meta.id += 1;
        meta.id
    }

    pub fn new_psubscription(&self, channel: &Pattern) -> usize {
        let mut meta = self.meta.write();
        meta.psubscriptions.push(channel.clone());
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
