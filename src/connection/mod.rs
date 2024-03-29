//! # Connection module
use self::pubsub_server::Pubsub;
use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::broadcast::{self, Receiver, Sender};

pub mod connections;
pub mod pubsub_connection;
pub mod pubsub_server;

/// Possible status of connections
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ConnectionStatus {
    /// The connection is in a MULTI stage and commands are being queued
    Multi,
    /// Failed Tx
    FailedTx,
    /// The connection is executing a transaction
    ExecutingTx,
    /// The connection is in pub-sub only mode
    Pubsub,
    /// The connection is a normal conection
    #[default]
    Normal,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// Reason while a client was unblocked
pub enum UnblockReason {
    /// Timeout
    Timeout,
    /// Throw an error
    Error,
    /// Operation finished successfully
    Finished,
}

/// Connection information
#[derive(Debug)]
pub struct ConnectionInfo {
    current_db: usize,
    db: Arc<Db>,
    name: Option<String>,
    watch_keys: Vec<(Bytes, usize)>,
    tx_keys: HashSet<Bytes>,
    status: ConnectionStatus,
    commands: Option<Vec<VecDeque<Bytes>>>,
    is_blocked: bool,
    blocked_notification: Option<Sender<()>>,
    block_id: usize,
    unblock_reason: Option<UnblockReason>,
}

/// Connection
#[derive(Debug)]
pub struct Connection {
    id: u128,
    all_connections: Arc<connections::Connections>,
    addr: String,
    info: RwLock<ConnectionInfo>,
    pubsub_client: pubsub_connection::PubsubClient,
}

impl ConnectionInfo {
    /// Creates a new connection
    fn new(db: Arc<Db>) -> Self {
        Self {
            name: None,
            watch_keys: vec![],
            db,
            current_db: 0,
            tx_keys: HashSet::new(),
            commands: None,
            status: ConnectionStatus::default(),
            blocked_notification: None,
            is_blocked: false,
            block_id: 0,
            unblock_reason: None,
        }
    }
}

impl Connection {
    /// Returns a connection database.
    ///
    /// The database object is unique to this connection but most of its internal structure is
    /// shared (like the entries).
    pub fn db(&self) -> Arc<Db> {
        self.info.read().db.clone()
    }

    /// Creates a clone connection
    pub fn get_connection(&self) -> Arc<Connection> {
        self.all_connections
            .get_by_conn_id(self.id)
            .expect("Connection must be registered")
    }

    /// Returns the global pubsub server
    pub fn pubsub(&self) -> Arc<Pubsub> {
        self.all_connections.pubsub()
    }

    /// Queue response, this is the only way that a handler has to send multiple
    /// responses leveraging internally the pubsub to itself.
    pub fn append_response(&self, message: Value) {
        self.pubsub_client.send(message)
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
                Ok(Value::Ignore)
            }
            _ => Err(Error::NestedTx),
        }
    }

    /// Block the connection
    pub fn block(&self) {
        let notification = broadcast::channel(1);
        let mut info = self.info.write();
        info.is_blocked = true;
        info.blocked_notification = Some(notification.0);
        info.block_id += 1;
        info.unblock_reason = None;
    }

    /// Returns the current block task ID number. This is an internal ID to
    /// identify each blocking command as unique
    #[inline]
    pub fn get_block_id(&self) -> Option<usize> {
        let info = self.info.read();
        if info.is_blocked {
            Some(info.block_id)
        } else {
            None
        }
    }

    /// Returns a receiver that will be called if the client is externally unblocked
    #[inline]
    pub fn get_unblocked_subscription(&self) -> Option<Receiver<()>> {
        self.info
            .read()
            .blocked_notification
            .as_ref()
            .map(|notification| notification.subscribe())
    }

    /// Unblock connection
    pub fn unblock(&self, reason: UnblockReason) -> bool {
        let mut info = self.info.write();
        if info.is_blocked {
            let notification = info.blocked_notification.as_ref().cloned();
            info.is_blocked = false;
            info.unblock_reason = Some(reason);
            info.blocked_notification = None;
            drop(info); // drop write lock

            if let Some(s) = notification {
                // Notify connection about this change
                let _ = s.send(());
            }

            true
        } else {
            false
        }
    }

    /// Is the current connection blocked?
    #[inline]
    pub fn is_blocked(&self) -> bool {
        self.info.read().is_blocked
    }

    /// Connection ID
    #[inline]
    pub fn id(&self) -> u128 {
        self.id
    }

    /// Drops a multi/transaction and reset the connection
    ///
    /// If the connection was not in a MULTI stage an error is thrown.
    pub fn stop_transaction(&self) -> Result<Value, Error> {
        let mut info = self.info.write();
        match info.status {
            ConnectionStatus::Multi
            | ConnectionStatus::FailedTx
            | ConnectionStatus::ExecutingTx => {
                info.commands = None;
                info.watch_keys.clear();
                info.tx_keys.clear();
                info.status = ConnectionStatus::default();

                Ok(Value::Ok)
            }
            _ => Err(Error::NotInTx),
        }
    }

    /// Flag the transaction as failed
    pub fn fail_transaction(&self) {
        let mut info = self.info.write();
        info.status = ConnectionStatus::FailedTx;
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
        info.status = ConnectionStatus::default();
        info.name = None;
        info.watch_keys = vec![];
        info.commands = None;
        info.tx_keys = HashSet::new();
        drop(info);

        let pubsub = self.pubsub();
        let pubsub_client = self.pubsub_client();
        if !pubsub_client.subscriptions().is_empty() {
            pubsub.unsubscribe(&self.pubsub_client.subscriptions(), self, false);
        }
        if !pubsub_client.psubscriptions().is_empty() {
            pubsub.punsubscribe(&self.pubsub_client.psubscriptions(), self, false);
        }
    }

    /// Returns the status of the connection
    #[inline]
    pub fn status(&self) -> ConnectionStatus {
        self.info.read().status
    }

    /// Is connection executing transaction?
    #[inline]
    pub fn is_executing_tx(&self) -> bool {
        self.info.read().status == ConnectionStatus::ExecutingTx
    }

    /// Watches keys. In a transaction watched keys are a mechanism to discard a transaction if
    /// some value changed since the moment the command was queued until the execution time.
    pub fn watch_key(&self, keys: Vec<(Bytes, usize)>) {
        let watch_keys = &mut self.info.write().watch_keys;
        keys.into_iter()
            .map(|value| {
                watch_keys.push(value);
            })
            .for_each(drop);
    }

    /// Returns true if any of the watched keys changed their value
    pub fn did_keys_change(&self) -> bool {
        let watch_keys = &self.info.read().watch_keys;

        for key in watch_keys.iter() {
            if self.info.read().db.get(&key.0).version() != key.1 {
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
    pub fn queue_command(&self, args: VecDeque<Bytes>) {
        let mut info = self.info.write();
        let commands = info.commands.get_or_insert(vec![]);
        commands.push(args);
    }

    /// Returns a list of queued commands.
    pub fn get_queue_commands(&self) -> Option<Vec<VecDeque<Bytes>>> {
        let mut info = self.info.write();
        info.watch_keys = vec![];
        info.status = ConnectionStatus::ExecutingTx;
        info.commands.take()
    }

    /// Returns a list of transaction keys
    pub fn tx_keys<T>(&self, keys: T)
    where
        T: IntoIterator<Item = Bytes>,
    {
        #[allow(clippy::mutable_key_type)]
        let tx_keys = &mut self.info.write().tx_keys;
        keys.into_iter()
            .map(|k| {
                tx_keys.insert(k);
            })
            .for_each(drop);
    }

    /// Disconnects from the server, disconnect from all pubsub channels and remove itself from the
    /// all_connection lists.
    pub fn destroy(self: Arc<Connection>) {
        let pubsub = self.pubsub();
        self.clone().unblock(UnblockReason::Timeout);
        pubsub.unsubscribe(&self.pubsub_client.subscriptions(), &self, false);
        pubsub.punsubscribe(&self.pubsub_client.psubscriptions(), &self, false);
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

    /// Changes the current db for the current connection
    pub fn selectdb(&self, db: usize) -> Result<Value, Error> {
        let mut info = self.info.write();
        info.db = self
            .all_connections
            .get_databases()
            .get(db)?
            .set_conn_id(self.id);
        info.current_db = db;
        Ok(Value::Ok)
    }
}

impl ToString for Connection {
    /// Returns a string representation of this connection
    fn to_string(&self) -> String {
        let info = self.info.read();
        format!(
            "id={} addr={} name={:?} db={}\r\n",
            self.id, self.addr, info.name, info.current_db
        )
    }
}
