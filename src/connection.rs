use crate::db::Db;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub struct Connections {
    connections: BTreeMap<u128, Arc<Mutex<Connection>>>,
    counter: u128,
}

impl Connections {
    pub fn new() -> Self {
        Self {
            counter: 0,
            connections: BTreeMap::new(),
        }
    }

    pub fn new_connection(&mut self, db: Arc<Db>, addr: SocketAddr) -> Arc<Mutex<Connection>> {
        let id = self.counter;
        let conn = Arc::new(Mutex::new(Connection {
            id,
            db,
            addr,
            current_db: 0,
            name: None,
        }));
        self.counter += 1;
        self.connections.insert(id, conn.clone());
        conn
    }
}

pub struct Connection {
    id: u128,
    db: Arc<Db>,
    current_db: u32,
    addr: SocketAddr,
    name: Option<String>,
}

impl Connection {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn id(&self) -> u128 {
        self.id
    }

    pub fn name(&self) -> &Option<String> {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    #[allow(dead_code)]
    pub fn current_db(&self) -> u32 {
        self.current_db
    }

    pub fn info(&self) -> String {
        format!(
            "id={} addr={} name={:?} db={}\r\n",
            self.id, self.addr, self.name, self.current_db
        )
    }
}
