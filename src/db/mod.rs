mod entry;
mod expiration;

use crate::{error::Error, value::Value};
use bytes::Bytes;
use entry::Entry;
use expiration::ExpirationDb;
use log::trace;
use seahash::hash;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    ops::AddAssign,
    sync::{Mutex, RwLock},
};
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct Db {
    /// A vector of hashmaps.
    ///
    /// Instead of having a single HashMap, and having all threads fighting for
    /// blocking the single HashMap, we have a vector of N HashMap
    /// (configurable), which in theory allow to have faster reads and writes.
    ///
    /// Because all operations are always key specific, the key is used to hash
    /// and select to which HashMap the data might be stored.
    entries: Vec<RwLock<HashMap<Bytes, Entry>>>,

    /// Data structure to store all expiring keys
    expirations: Mutex<ExpirationDb>,

    /// Number of HashMaps that are available.
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
            expirations: Mutex::new(ExpirationDb::new()),
            slots,
        }
    }

    #[inline]
    fn get_slot(&self, key: &Bytes) -> usize {
        let id = (hash(key) as usize) % self.entries.len();
        trace!("selected slot {} for key {:?}", id, key);
        id
    }

    pub fn incr<
        T: ToString + AddAssign + for<'a> TryFrom<&'a Value, Error = Error> + Into<Value> + Copy,
    >(
        &self,
        key: &Bytes,
        incr_by: T,
    ) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        match entries.get_mut(key) {
            Some(x) => {
                let value = x.get();
                let mut number: T = value.try_into()?;

                number += incr_by;

                x.change_value(number.to_string().as_str().into());

                Ok(number.into())
            }
            None => {
                entries.insert(
                    key.clone(),
                    Entry::new(incr_by.to_string().as_str().into(), None),
                );
                Ok((incr_by as T).into())
            }
        }
    }

    pub fn persist(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0_i64.into(), |x| {
                if x.has_ttl() {
                    x.persist();
                    1_i64.into()
                } else {
                    0_i64.into()
                }
            })
    }

    pub fn set_ttl(&self, key: &Bytes, expires_in: Duration) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        let expires_at = Instant::now() + expires_in;

        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0_i64.into(), |x| {
                self.expirations.lock().unwrap().add(key, expires_at);
                x.set_ttl(expires_at);
                1_i64.into()
            })
    }

    pub fn del(&self, keys: &[Bytes]) -> Value {
        let mut deleted = 0_i64;
        let mut expirations = self.expirations.lock().unwrap();
        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                if entries.remove(key).is_some() {
                    expirations.remove(&key);
                    deleted += 1;
                }
            })
            .for_each(drop);

        deleted.into()
    }

    pub fn exists(&self, keys: &[Bytes]) -> Value {
        let mut matches = 0_i64;
        keys.iter()
            .map(|key| {
                let entries = self.entries[self.get_slot(key)].read().unwrap();
                if entries.get(key).is_some() {
                    matches += 1;
                }
            })
            .for_each(drop);

        matches.into()
    }

    pub fn get(&self, key: &Bytes) -> Value {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.get().clone())
    }

    pub fn get_multi(&self, keys: &[Bytes]) -> Value {
        keys.iter()
            .map(|key| {
                let entries = self.entries[self.get_slot(key)].read().unwrap();
                entries.get(key).map_or(Value::Null, |x| x.get().clone())
            })
            .collect::<Vec<Value>>()
            .into()
    }

    pub fn getset(&self, key: &Bytes, value: &Value) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        self.expirations.lock().unwrap().remove(&key);
        entries
            .insert(key.clone(), Entry::new(value.clone(), None))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.get().clone())
    }

    pub fn getdel(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries.remove(key).map_or(Value::Null, |x| {
            self.expirations.lock().unwrap().remove(&key);
            x.get().clone()
        })
    }

    pub fn set(&self, key: &Bytes, value: &Value, expires_in: Option<Duration>) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        let expires_at = expires_in.map(|duration| Instant::now() + duration);

        if let Some(expires_at) = expires_at {
            self.expirations.lock().unwrap().add(&key, expires_at);
        }
        entries.insert(key.clone(), Entry::new(value.clone(), expires_at));
        Value::OK
    }

    pub fn ttl(&self, key: &Bytes) -> Option<Option<Instant>> {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map(|x| x.get_ttl())
    }

    pub fn purge(&self) {
        let mut expirations = self.expirations.lock().unwrap();

        trace!("Watching {} keys for expirations", expirations.len());

        let keys = expirations.get_expired_keys(None);
        drop(expirations);

        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                if entries.remove(key).is_some() {
                    trace!("Removed key {:?} due timeout", key);
                }
            })
            .for_each(drop);
    }
}
