use super::{pubsub_connection::PubsubClient, pubsub_server::Pubsub, Connection, ConnectionInfo};
use crate::{db::Db, value::Value};
use parking_lot::RwLock;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    db: Arc<Db>,
    pubsub: Arc<Pubsub>,
    counter: RwLock<u128>,
}

impl Connections {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            counter: RwLock::new(0),
            db,
            pubsub: Arc::new(Pubsub::new()),
            connections: RwLock::new(BTreeMap::new()),
        }
    }

    #[allow(dead_code)]
    pub fn db(&self) -> Arc<Db> {
        self.db.clone()
    }

    pub fn pubsub(&self) -> Arc<Pubsub> {
        self.pubsub.clone()
    }

    pub fn remove(self: Arc<Connections>, conn: Arc<Connection>) {
        let id = conn.id();
        self.connections.write().remove(&id);
    }

    pub fn new_connection(
        self: &Arc<Connections>,
        db: Arc<Db>,
        addr: SocketAddr,
    ) -> (mpsc::UnboundedReceiver<Value>, Arc<Connection>) {
        let mut id = self.counter.write();
        *id += 1;

        let (pubsub_sender, pubsub_receiver) = mpsc::unbounded_channel();

        let conn = Arc::new(Connection {
            id: *id,
            db: db.new_db_instance(*id),
            addr,
            all_connections: self.clone(),
            current_db: 0,
            info: RwLock::new(ConnectionInfo::new()),
            pubsub_client: PubsubClient::new(pubsub_sender),
        });

        self.connections.write().insert(*id, conn.clone());
        (pubsub_receiver, conn)
    }

    pub fn iter(&self, f: &mut dyn FnMut(Arc<Connection>)) {
        for (_, value) in self.connections.read().iter() {
            f(value.clone())
        }
    }
}
