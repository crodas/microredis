use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    counter: RwLock<u128>,
}

#[derive(Debug)]
pub struct ConnectionInfo {
    pub name: Option<String>,
    pub watch_keys: Vec<(Bytes, u128)>,
    pub tx_keys: HashSet<Bytes>,
    pub in_transaction: bool,
    pub in_executing_transaction: bool,
    pub commands: Option<Vec<Vec<Bytes>>>,
}

#[derive(Debug)]
pub struct Connection {
    id: u128,
    db: Db,
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
        *id += 1;

        let conn = Arc::new(Connection {
            id: *id,
            db: db.new_db_instance(*id),
            addr,
            connections: self.clone(),
            current_db: 0,
            info: RwLock::new(ConnectionInfo::new()),
        });

        self.connections.write().unwrap().insert(*id, conn.clone());
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
            commands: None,
            in_transaction: false,
            in_executing_transaction: false,
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

    pub fn stop_transaction(&self) -> Result<Value, Error> {
        let info = &mut self.info.write().unwrap();
        if info.in_transaction {
            info.commands = None;
            info.watch_keys.clear();
            info.tx_keys.clear();
            info.in_transaction = false;

            Ok(Value::Ok)
        } else {
            Err(Error::NotInTx)
        }
    }

    pub fn start_transaction(&self) -> Result<Value, Error> {
        let mut info = self.info.write().unwrap();
        if !info.in_transaction {
            info.in_transaction = true;
            Ok(Value::Ok)
        } else {
            Err(Error::NestedTx)
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.info.read().unwrap().in_transaction
    }

    pub fn is_executing_transaction(&self) -> bool {
        self.info.read().unwrap().in_executing_transaction
    }

    pub fn start_executing_transaction(&self) {
        let info = &mut self.info.write().unwrap();
        info.in_executing_transaction = true;
    }

    pub fn watch_key(&self, keys: &[(&Bytes, u128)]) {
        let watch_keys = &mut self.info.write().unwrap().watch_keys;
        keys.iter()
            .map(|(bytes, version)| {
                watch_keys.push(((*bytes).clone(), *version));
            })
            .for_each(drop);
    }

    pub fn did_keys_change(&self) -> bool {
        let watch_keys = &self.info.read().unwrap().watch_keys;

        for key in watch_keys.iter() {
            if self.db.get_version(&key.0) != key.1 {
                return true;
            }
        }

        false
    }

    pub fn discard_watched_keys(&self) {
        let watch_keys = &mut self.info.write().unwrap().watch_keys;
        watch_keys.clear();
    }

    pub fn get_tx_keys(&self) -> Vec<Bytes> {
        self.info
            .read()
            .unwrap()
            .tx_keys
            .iter()
            .cloned()
            .collect::<Vec<Bytes>>()
    }

    pub fn queue_command(&self, args: &[Bytes]) {
        let info = &mut self.info.write().unwrap();
        let commands = info.commands.get_or_insert(vec![]);
        commands.push(args.iter().map(|m| (*m).clone()).collect());
    }

    pub fn get_queue_commands(&self) -> Option<Vec<Vec<Bytes>>> {
        let info = &mut self.info.write().unwrap();
        info.watch_keys = vec![];
        info.in_transaction = false;
        info.commands.take()
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
