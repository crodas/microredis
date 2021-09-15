pub mod entry;

use crate::{error::Error, value::Value};
use bytes::Bytes;
use entry::Entry;
use log::trace;
use seahash::hash;
use std::{
    collections::{BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    ops::AddAssign,
    sync::RwLock,
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
    /// B-Tree Map of expiring keys
    ///
    /// This B-Tree has the name of expiring entries, and they are sorted by the
    /// Instant where the entries are expiring.
    ///
    /// Because it is possible that two entries expire at the same Instant, a
    /// counter is introduced to avoid collisions on this B-Tree.
    expirations: RwLock<BTreeMap<(Instant, u64), String>>,
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
                entries.insert(key.clone(), Entry::new(incr_by.to_string().as_str().into()));
                Ok((incr_by as T).into())
            }
        }
    }

    pub fn remove_expiration(&self, key: &Bytes) -> Value {
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

    pub fn add_expiration(&self, key: &Bytes, time: Duration) -> Value {
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
        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                if entries.remove(key).is_some() {
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
        entries
            .insert(key.clone(), Entry::new(value.clone()))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.get().clone())
    }

    pub fn getdel(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries.remove(key).map_or(Value::Null, |x| x.get().clone())
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
