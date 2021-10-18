use crate::db::Db;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    counter: RwLock<u128>,
}

pub struct ConnectionInfo {
    pub name: Option<String>,
    pub watch_keys: Vec<(Bytes, u128)>,
    pub tx_keys: HashSet<Bytes>,
}

pub struct Connection {
    id: u128,
    db: Arc<Db>,
    current_db: u32,
    connections: Arc<Connections>,
    addr: SocketAddr,
    info: RwLock<ConnectionInfo>,
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
            info: RwLock::new(ConnectionInfo::new()),
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

impl ConnectionInfo {
    fn new() -> Self {
        Self {
            name: None,
            watch_keys: vec![],
            tx_keys: HashSet::new(),
        }
    }
}

impl Connection {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn id(&self) -> u128 {
        self.id
    }

    pub fn in_transaction(&self) -> bool {
        false
    }

    pub fn watch_key(&self, keys: &[(&Bytes, u128)]) {
        let watch_keys = &mut self.info.write().unwrap().watch_keys;
        keys.iter()
            .map(|(bytes, version)| {
                watch_keys.push(((*bytes).clone(), *version));
            })
            .for_each(drop);
    }

    pub fn tx_keys(&self, keys: Vec<&Bytes>) {
        #[allow(clippy::mutable_key_type)]
        let tx_keys = &mut self.info.write().unwrap().tx_keys;
        keys.iter()
            .map(|k| {
                tx_keys.insert((*k).clone());
            })
            .for_each(drop);
    }

    pub fn get_tx_keys(&self) -> Vec<Bytes> {
        self.info
            .read()
            .unwrap()
            .tx_keys
            .iter()
            .map(|key| key.clone())
            .collect::<Vec<Bytes>>()
    }

    pub fn discard_watched_keys(&self) {
        let watch_keys = &mut self.info.write().unwrap().watch_keys;
        watch_keys.clear();
    }

    pub fn destroy(self: Arc<Connection>) {
        self.connections.clone().remove(self);
    }

    pub fn all_connections(&self) -> Arc<Connections> {
        self.connections.clone()
    }

    pub fn name(&self) -> Option<String> {
        self.info.read().unwrap().name.clone()
    }

    pub fn set_name(&self, name: String) {
        let mut r = self.info.write().unwrap();
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
            self.info.read().unwrap().name,
            self.current_db
        )
    }
}
