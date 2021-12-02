use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;
use glob::Pattern;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Connections {
    connections: RwLock<BTreeMap<u128, Arc<Connection>>>,
    db: Arc<Db>,
    counter: RwLock<u128>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
    pub subscriptions: Vec<Bytes>,
    pub psubscriptions: Vec<Pattern>,
    pub watch_keys: Vec<(Bytes, u128)>,
    pub tx_keys: HashSet<Bytes>,
    pub status: ConnectionStatus,
    pub commands: Option<Vec<Vec<Bytes>>>,
    pub is_psubcribed: bool,
}

#[derive(Debug)]
pub struct Connection {
    id: u128,
    db: Db,
    current_db: u32,
    connections: Arc<Connections>,
    addr: SocketAddr,
    info: RwLock<ConnectionInfo>,
    pubsub_sender: mpsc::UnboundedSender<Value>,
}

impl Connections {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            counter: RwLock::new(0),
            db,
            connections: RwLock::new(BTreeMap::new()),
        }
    }

    #[allow(dead_code)]
    pub fn db(&self) -> Arc<Db> {
        self.db.clone()
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
            connections: self.clone(),
            current_db: 0,
            info: RwLock::new(ConnectionInfo::new()),
            pubsub_sender,
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

impl ConnectionInfo {
    fn new() -> Self {
        Self {
            name: None,
            subscriptions: vec![],
            psubscriptions: vec![],
            watch_keys: vec![],
            tx_keys: HashSet::new(),
            commands: None,
            status: ConnectionStatus::Normal,
            is_psubcribed: false,
        }
    }
}

impl Connection {
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn get_pubsub_sender(&self) -> mpsc::UnboundedSender<Value> {
        self.pubsub_sender.clone()
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
        self.info.read().status.clone()
    }

    pub fn is_psubcribed(&self) -> bool {
        self.info.read().is_psubcribed
    }

    pub fn make_psubcribed(&self) {
        self.info.write().is_psubcribed = true;
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
        self.connections.clone().remove(self);
    }

    pub fn all_connections(&self) -> Arc<Connections> {
        self.connections.clone()
    }

    pub fn name(&self) -> Option<String> {
        self.info.read().name.clone()
    }

    pub fn set_name(&self, name: String) {
        let mut r = self.info.write();
        r.name = Some(name);
    }

    pub fn get_subscription_id(&self, channel: &Bytes) -> usize {
        let mut info = self.info.write();

        info.subscriptions.push(channel.clone());

        info.subscriptions.len() + info.psubscriptions.len()
    }

    pub fn get_psubscription_id(&self, channel: &Pattern) -> usize {
        let mut info = self.info.write();

        info.psubscriptions.push(channel.clone());
        info.subscriptions.len() + info.psubscriptions.len()
    }

    pub fn get_pubsub_subscriptions(&self) -> Vec<Bytes> {
        self.info.read().subscriptions.clone()
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
