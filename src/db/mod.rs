use crate::{error::Error, value::Value};
use bytes::Bytes;
use log::trace;
use seahash::hash;
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryInto,
    sync::RwLock,
};
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    pub fn change_value(&mut self, value: Value) {
        self.value = value;
    }

    pub fn get_mut(&mut self) -> &mut Value {
        &mut self.value
    }

    pub fn get(&self) -> &Value {
        &self.value
    }

    pub fn is_valid(&self) -> bool {
        self.expires_at.map_or(true, |x| x > Instant::now())
    }
}

#[derive(Debug)]
pub struct Db {
    entries: Vec<RwLock<HashMap<Bytes, Entry>>>,
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
        match entries.get_mut(key) {
            Some(x) => {
                let value = x.get();
                let mut number: i64 = value.try_into()?;

                number += incr_by;

                x.change_value(format!("{}", number).as_str().into());

                Ok(number.into())
            }
            None => {
                entries.insert(key.clone(), Entry::new(incr_by.into()));
                Ok((incr_by as i64).into())
            }
        }
    }

    pub fn persist(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0_i64.into(), |mut x| {
                let ret = x.expires_at.map_or(0_i64, |_| 1_i64);
                x.expires_at = None;
                ret.into()
            })
    }

    pub fn expire(&self, key: &Bytes, time: Duration) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0_i64.into(), |mut x| {
                x.expires_at = Some(Instant::now() + time);
                1_i64.into()
            })
    }

    pub fn del(&self, keys: &[Bytes]) -> Value {
        let mut deleted = 0_i64;
        keys.iter().map(|key| {
            let mut entries = self.entries[self.get_slot(key)].write().unwrap();
            if entries.remove(key).is_some() {
                deleted += 1;
            }
        }).for_each(drop);

        deleted.into()
    }

    pub fn get(&self, key: &Bytes) -> Value {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.get().clone())
    }

    pub fn getset(&self, key: &Bytes, value: &Value) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries
            .insert(key.clone(), Entry::new(value.clone()))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.get().clone())
    }

    pub fn set(&self, key: &Bytes, value: &Value) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries.insert(key.clone(), Entry::new(value.clone()));
        Value::OK
    }

    pub fn ttl(&self, key: &Bytes) -> Option<Option<Instant>> {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map(|x| x.expires_at)
    }
}
