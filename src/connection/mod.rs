//! # Connection module
use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use self::pubsub_server::Pubsub;

pub mod connections;
pub mod pubsub_connection;
pub mod pubsub_server;

/// Possible status of connections
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    /// The connection is in a MULTI stage and commands are being queued
    Multi,
    /// The connection is executing a transaction
    ExecutingTx,
    /// The connection is in pub-sub only mode
    Pubsub,
    /// The connection is a normal conection
    Normal,
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Normal
    }
}

/// Connection information
#[derive(Debug)]
pub struct ConnectionInfo {
    name: Option<String>,
    watch_keys: Vec<(Bytes, u128)>,
    tx_keys: HashSet<Bytes>,
    status: ConnectionStatus,
    commands: Option<Vec<Vec<Bytes>>>,
}

/// Connection
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
    /// Creates a new connection
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
    /// Returns a connection database.
    ///
    /// The database object is unique to this connection but most of its internal structure is
    /// shared (like the entries).
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Returns the global pubsub server
    pub fn pubsub(&self) -> Arc<Pubsub> {
        self.all_connections.pubsub()
    }

    /// Returns a reference to the pubsub client
    pub fn pubsub_client(&self) -> &pubsub_connection::PubsubClient {
        &self.pubsub_client
    }

    /// Switch the connection to a pub-sub only mode
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

    /// Connection ID
    pub fn id(&self) -> u128 {
        self.id
    }

    /// Drops a multi/transaction and reset the connection
    ///
    /// If the connection was not in a MULTI stage an error is thrown.
    pub fn stop_transaction(&self) -> Result<Value, Error> {
        let mut info = self.info.write();
        if info.status == ConnectionStatus::Multi || info.status == ConnectionStatus::ExecutingTx {
            info.commands = None;
            info.watch_keys.clear();
            info.tx_keys.clear();
            info.status = ConnectionStatus::Normal;

            Ok(Value::Ok)
        } else {
            Err(Error::NotInTx)
        }
    }

    /// Starts a transaction/multi
    ///
    /// Nested transactions are not possible.
    pub fn start_transaction(&self) -> Result<Value, Error> {
        let mut info = self.info.write();
        if info.status == ConnectionStatus::Normal {
            info.status = ConnectionStatus::Multi;
            Ok(Value::Ok)
        } else {
            Err(Error::NestedTx)
        }
    }

    /// Resets the current connection.
    pub fn reset(&self) {
        let mut info = self.info.write();
        info.status = ConnectionStatus::Normal;
        info.name = None;
        info.watch_keys = vec![];
        info.commands = None;
        info.tx_keys = HashSet::new();

        let pubsub = self.pubsub();
        pubsub.unsubscribe(&self.pubsub_client.subscriptions(), self);
        pubsub.punsubscribe(&self.pubsub_client.psubscriptions(), self);
    }

    /// Returns the status of the connection
    pub fn status(&self) -> ConnectionStatus {
        self.info.read().status
    }

    /// Watches keys. In a transaction watched keys are a mechanism to discard a transaction if
    /// some value changed since the moment the command was queued until the execution time.
    pub fn watch_key(&self, keys: &[(&Bytes, u128)]) {
        let watch_keys = &mut self.info.write().watch_keys;
        keys.iter()
            .map(|(bytes, version)| {
                watch_keys.push(((*bytes).clone(), *version));
            })
            .for_each(drop);
    }

    /// Returns true if any of the watched keys changed their value
    pub fn did_keys_change(&self) -> bool {
        let watch_keys = &self.info.read().watch_keys;

        for key in watch_keys.iter() {
            if self.db.get_version(&key.0) != key.1 {
                return true;
            }
        }

        false
    }

    /// Resets the watched keys list
    pub fn discard_watched_keys(&self) {
        self.info.write().watch_keys.clear()
    }

    /// Returns a list of key that are involved in a transaction. These keys will be locked as
    /// exclusive, even if they don't exists, during the execution of a transction.
    ///
    /// The original implementation of Redis does not need this promise because only one
    /// transaction is executed at a time, in microredis transactions reserve their keys and do not
    /// prevent other connections to continue modifying the database.
    pub fn get_tx_keys(&self) -> Vec<Bytes> {
        self.info
            .read()
            .tx_keys
            .iter()
            .cloned()
            .collect::<Vec<Bytes>>()
    }

    /// Queues a command for later execution
    pub fn queue_command(&self, args: &[Bytes]) {
        let mut info = self.info.write();
        let commands = info.commands.get_or_insert(vec![]);
        commands.push(args.iter().map(|m| (*m).clone()).collect());
    }

    /// Returns a list of queued commands.
    pub fn get_queue_commands(&self) -> Option<Vec<Vec<Bytes>>> {
        let mut info = self.info.write();
        info.watch_keys = vec![];
        info.status = ConnectionStatus::ExecutingTx;
        info.commands.take()
    }

    /// Returns a lsit of transaction keys
    pub fn tx_keys(&self, keys: Vec<&Bytes>) {
        #[allow(clippy::mutable_key_type)]
        let tx_keys = &mut self.info.write().tx_keys;
        keys.iter()
            .map(|k| {
                tx_keys.insert((*k).clone());
            })
            .for_each(drop);
    }

    /// Disconnects from the server, disconnect from all pubsub channels and remove itself from the
    /// all_connection lists.
    pub fn destroy(self: Arc<Connection>) {
        let pubsub = self.pubsub();
        pubsub.unsubscribe(&self.pubsub_client.subscriptions(), &self);
        pubsub.punsubscribe(&self.pubsub_client.psubscriptions(), &self);
        self.all_connections.clone().remove(self);
    }

    /// Returns the all_connections (Connections) instance
    pub fn all_connections(&self) -> Arc<connections::Connections> {
        self.all_connections.clone()
    }

    /// Returns the connection name
    pub fn name(&self) -> Option<String> {
        self.info.read().name.clone()
    }

    /// Sets a connection name
    pub fn set_name(&self, name: String) {
        let mut r = self.info.write();
        r.name = Some(name);
    }

    /// Returns a string representation of this connection
    pub fn as_string(&self) -> String {
        format!(
            "id={} addr={} name={:?} db={}\r\n",
            self.id,
            self.addr,
            self.info.read().name,
            self.current_db
        )
    }
}
