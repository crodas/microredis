//! # Connections object
//!
//! This mod keeps track of all active conections. There is one instance of this mod per running
//! server.
use super::{pubsub_connection::PubsubClient, pubsub_server::Pubsub, Connection, ConnectionInfo};
use crate::{db::pool::Databases, db::Db, dispatcher::Dispatcher, value::Value};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::mpsc;

/// Connections struct
#[derive(Debug)]
pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    dbs: Arc<Databases>,
    pubsub: Arc<Pubsub>,
    dispatcher: Arc<Dispatcher>,
    counter: RwLock<u128>,
}

impl Connections {
    /// Returns a new instance of connections.
    pub fn new(dbs: Arc<Databases>) -> Self {
        Self {
            counter: RwLock::new(0),
            dbs,
            pubsub: Arc::new(Pubsub::new()),
            dispatcher: Arc::new(Dispatcher::new()),
            connections: RwLock::new(BTreeMap::new()),
        }
    }

    /// Returns all databases
    pub fn get_databases(&self) -> Arc<Databases> {
        self.dbs.clone()
    }

    /// Returns the dispatcher instance
    pub fn get_dispatcher(&self) -> Arc<Dispatcher> {
        self.dispatcher.clone()
    }

    /// Returns the pubsub server instance
    pub fn pubsub(&self) -> Arc<Pubsub> {
        self.pubsub.clone()
    }

    /// Removes a connection from the connections
    pub fn remove(self: Arc<Connections>, conn: Arc<Connection>) {
        let id = conn.id();
        self.connections.write().remove(&id);
    }

    /// Creates a new connection
    pub fn new_connection<T: ToString>(
        self: &Arc<Connections>,
        db: Arc<Db>,
        addr: T,
    ) -> (mpsc::Receiver<Value>, Arc<Connection>) {
        let mut id = self.counter.write();
        *id += 1;

        let (pubsub_sender, pubsub_receiver) = mpsc::channel(1_000);

        let conn = Arc::new(Connection {
            id: *id,
            addr: addr.to_string(),
            all_connections: self.clone(),
            info: RwLock::new(ConnectionInfo::new(db.new_db_instance(*id))),
            pubsub_client: PubsubClient::new(pubsub_sender),
        });

        self.connections.write().insert(*id, conn.clone());
        (pubsub_receiver, conn)
    }

    /// Get a connection by their connection id
    pub fn get_by_conn_id(&self, conn_id: u128) -> Option<Arc<Connection>> {
        self.connections.read().get(&conn_id).cloned()
    }

    /// Iterates over all connections
    pub fn iter(&self, f: &mut dyn FnMut(Arc<Connection>)) {
        for (_, value) in self.connections.read().iter() {
            f(value.clone())
        }
    }
}
