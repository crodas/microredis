use crate::{error::Error, value::Value};
use bytes::Bytes;
use log::trace;
use seahash::hash;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
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

    #[inline]
    fn get_slot(&self, key: &Bytes) -> usize {
        let id = (hash(key) as usize) % self.entries.len();
        trace!("selected slot {} for key {:?}", id, key);
        id
    }

    pub fn incr(&self, key: &Bytes, incr_by: i64) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        match entries.get(key) {
            Some(x) => {
                let mut val: i64 = x.try_into()?;

                val += incr_by;

                entries.insert(key.clone(), format!("{}", val).as_str().into());

                Ok(val.into())
            }
            None => {
                entries.insert(key.clone(), "1".into());
                Ok((1_i64).into())
            }
        }
    }

    pub fn get(&self, key: &Bytes) -> Result<Value, Error> {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        Ok(entries.get(key).cloned().unwrap_or(Value::Null))
    }

    pub fn getset(&self, key: &Bytes, value: &Value) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        let prev = entries.get(key).cloned().unwrap_or(Value::Null);
        entries.insert(key.clone(), value.clone());
        Ok(prev)
    }

    pub fn set(&self, key: &Bytes, value: &Value) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries.insert(key.clone(), value.clone());
        Ok(Value::OK)
    }
}
