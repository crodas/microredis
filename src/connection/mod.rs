use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use self::pubsub_server::Pubsub;

pub mod connections;
pub mod pubsub_connection;
pub mod pubsub_server;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    Multi,
    ExecutingTx,
    Pubsub,
    Normal,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Normal
    }
}

#[derive(Debug)]
pub struct ConnectionInfo {
    pub name: Option<String>,
    pub watch_keys: Vec<(Bytes, u128)>,
    pub tx_keys: HashSet<Bytes>,
    pub status: ConnectionStatus,
    pub commands: Option<Vec<Vec<Bytes>>>,
}

#[derive(Debug)]
pub struct Connection {
    id: u128,
    db: Db,
    current_db: u32,
    all_connections: Arc<connections::Connections>,
    addr: SocketAddr,
    info: RwLock<ConnectionInfo>,
    pubsub_client: pubsub_connection::PubsubClient,
}

impl ConnectionInfo {
    fn new() -> Self {
        Self {
            name: None,
            watch_keys: vec![],
            tx_keys: HashSet::new(),
            commands: None,
            status: ConnectionStatus::Normal,
        }
    }
}

impl Connection {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn pubsub(&self) -> Arc<Pubsub> {
        self.all_connections.pubsub()
    }

    pub fn pubsub_client(&self) -> &pubsub_connection::PubsubClient {
        &self.pubsub_client
    }

    pub fn id(&self) -> u128 {
        self.id
    }

    pub fn stop_transaction(&self) -> Result<Value, Error> {
        let info = &mut self.info.write();
        if info.status == ConnectionStatus::Multi {
            info.commands = None;
            info.watch_keys.clear();
            info.tx_keys.clear();
            info.status = ConnectionStatus::ExecutingTx;

            Ok(Value::Ok)
        } else {
            Err(Error::NotInTx)
        }
    }

    pub fn start_transaction(&self) -> Result<Value, Error> {
        let mut info = self.info.write();
        if info.status == ConnectionStatus::Normal {
            info.status = ConnectionStatus::Multi;
            Ok(Value::Ok)
        } else {
            Err(Error::NestedTx)
        }
    }

    pub fn start_pubsub(&self) -> Result<Value, Error> {
        let mut info = self.info.write();
        match info.status {
            ConnectionStatus::Normal | ConnectionStatus::Pubsub => {
                info.status = ConnectionStatus::Pubsub;
                Ok(Value::Ok)
            }
            _ => Err(Error::NestedTx),
        }
    }

    pub fn status(&self) -> ConnectionStatus {
        self.info.read().status
    }

    pub fn watch_key(&self, keys: &[(&Bytes, u128)]) {
        let watch_keys = &mut self.info.write().watch_keys;
        keys.iter()
            .map(|(bytes, version)| {
                watch_keys.push(((*bytes).clone(), *version));
            })
            .for_each(drop);
    }

    pub fn did_keys_change(&self) -> bool {
        let watch_keys = &self.info.read().watch_keys;

        for key in watch_keys.iter() {
            if self.db.get_version(&key.0) != key.1 {
                return true;
            }
        }

        false
    }

    pub fn discard_watched_keys(&self) {
        let watch_keys = &mut self.info.write().watch_keys;
        watch_keys.clear();
    }

    pub fn get_tx_keys(&self) -> Vec<Bytes> {
        self.info
            .read()
            .tx_keys
            .iter()
            .cloned()
            .collect::<Vec<Bytes>>()
    }

    pub fn queue_command(&self, args: &[Bytes]) {
        let info = &mut self.info.write();
        let commands = info.commands.get_or_insert(vec![]);
        commands.push(args.iter().map(|m| (*m).clone()).collect());
    }

    pub fn get_queue_commands(&self) -> Option<Vec<Vec<Bytes>>> {
        let info = &mut self.info.write();
        info.watch_keys = vec![];
        info.status = ConnectionStatus::ExecutingTx;
        info.commands.take()
    }

    pub fn tx_keys(&self, keys: Vec<&Bytes>) {
        #[allow(clippy::mutable_key_type)]
        let tx_keys = &mut self.info.write().tx_keys;
        keys.iter()
            .map(|k| {
                tx_keys.insert((*k).clone());
            })
            .for_each(drop);
    }

    pub fn destroy(self: Arc<Connection>) {
        let pubsub = self.pubsub();
        pubsub.unsubscribe(&self.pubsub_client.subscriptions(), &self);
        pubsub.punsubscribe(&self.pubsub_client.psubscriptions(), &self);
        self.all_connections.clone().remove(self);
    }

    pub fn all_connections(&self) -> Arc<connections::Connections> {
        self.all_connections.clone()
    }

    pub fn name(&self) -> Option<String> {
        self.info.read().name.clone()
    }

    pub fn set_name(&self, name: String) {
        let mut r = self.info.write();
        r.name = Some(name);
    }

    #[allow(dead_code)]
    pub fn current_db(&self) -> u32 {
        self.current_db
    }

    pub fn info(&self) -> String {
        format!(
            "id={} addr={} name={:?} db={}\r\n",
            self.id,
            self.addr,
            self.info.read().name,
            self.current_db
        )
    }
}
