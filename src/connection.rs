use crate::db::Db;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    counter: RwLock<u128>,
}

impl Connections {
    pub fn new() -> Self {
        Self {
            counter: RwLock::new(0),
            connections: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn remove(self: Arc<Connections>, conn: Arc<Connection>) {
        let id = conn.id();
        self.connections.write().unwrap().remove(&id);
    }

    pub fn new_connection(
        self: &Arc<Connections>,
        db: Arc<Db>,
        addr: SocketAddr,
    ) -> Arc<Connection> {
        let mut id = self.counter.write().unwrap();

        let conn = Arc::new(Connection {
            id: *id,
            db,
            addr,
            connections: self.clone(),
            current_db: 0,
            name: RwLock::new(None),
        });
        self.connections.write().unwrap().insert(*id, conn.clone());
        *id += 1;
        conn
    }

    pub fn iter(&self, f: &mut dyn FnMut(Arc<Connection>)) {
        for (_, value) in self.connections.read().unwrap().iter() {
            f(value.clone())
        }
    }
}

pub struct Connection {
    id: u128,
    db: Arc<Db>,
    current_db: u32,
    connections: Arc<Connections>,
    addr: SocketAddr,
    name: RwLock<Option<String>>,
}

impl Connection {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn id(&self) -> u128 {
        self.id
    }

    pub fn destroy(self: Arc<Connection>) {
        self.connections.clone().remove(self);
    }

    pub fn all_connections(&self) -> Arc<Connections> {
        self.connections.clone()
    }

    pub fn name(&self) -> Option<String> {
        self.name.read().unwrap().clone()
    }

    pub fn set_name(&self, name: String) {
        let mut r = self.name.write().unwrap();
        *r = Some(name);
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
            self.name.read().unwrap(),
            self.current_db
        )
    }
}
