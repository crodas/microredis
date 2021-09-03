use crate::{error::Error, value::Value};
use bytes::Bytes;
use seahash::hash;
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Db {
    entries: Vec<RwLock<HashMap<Bytes, Value>>>,
    expirations: RwLock<BTreeMap<(Instant, u64), String>>,
    slots: usize,
}

impl Db {
    pub fn new(slots: usize) -> Self {
        let mut entries = vec![];

        for _i in 0..slots {
            entries.push(RwLock::new(HashMap::new()));
        }

        Self {
            entries,
            expirations: RwLock::new(BTreeMap::new()),
            slots,
        }
    }

    fn get_slot(&self, key: &Bytes) -> usize {
        (hash(key) as usize) % self.entries.len()
    }

    pub fn get(&self, key: &Value) -> Result<Value, Error> {
        match key {
            Value::Blob(key) => {
                let entries = self.entries[self.get_slot(key)].read().unwrap();
                Ok(entries.get(key).cloned().unwrap_or(Value::Null))
            }
            _ => Err(Error::WrongType),
        }
    }

    pub fn set(&self, key: &Value, value: &Value) -> Result<Value, Error> {
        match key {
            Value::Blob(key) => {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                entries.insert(key.clone(), value.clone());
                Ok(Value::OK)
            }
            _ => Err(Error::WrongType),
        }
    }
}
