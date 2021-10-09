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
            .map_or(0.into(), |x| {
                if x.has_ttl() {
                    x.persist();
                    1.into()
                } else {
                    0.into()
                }
            })
    }

    pub fn set_ttl(&self, key: &Bytes, expires_in: Duration) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        let expires_at = Instant::now() + expires_in;

        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0.into(), |x| {
                self.expirations.lock().unwrap().add(key, expires_at);
                x.set_ttl(expires_at);
                1.into()
            })
    }

    pub fn del(&self, keys: &[Bytes]) -> Value {
        let mut deleted = 0;
        let mut expirations = self.expirations.lock().unwrap();
        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                if let Some(entry) = entries.remove(key) {
                    expirations.remove(key);
                    if entry.is_valid() {
                        deleted += 1;
                    }
                }
            })
            .for_each(drop);

        deleted.into()
    }

    pub fn exists(&self, keys: &[Bytes]) -> Value {
        let mut matches = 0;
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

    pub fn get_map_or<F1, F2>(&self, key: &Bytes, found: F1, not_found: F2) -> Result<Value, Error>
    where
        F1: FnOnce(&Value) -> Result<Value, Error>,
        F2: FnOnce() -> Result<Value, Error>,
    {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        let entry = entries.get(key).filter(|x| x.is_valid()).map(|e| e.get());

        if let Some(entry) = entry {
            found(entry)
        } else {
            // drop lock
            drop(entries);

            not_found()
        }
    }

    pub fn get(&self, key: &Bytes) -> Value {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    pub fn get_multi(&self, keys: &[Bytes]) -> Value {
        keys.iter()
            .map(|key| {
                let entries = self.entries[self.get_slot(key)].read().unwrap();
                entries
                    .get(key)
                    .filter(|x| x.is_valid() && x.is_clonable())
                    .map_or(Value::Null, |x| x.clone_value())
            })
            .collect::<Vec<Value>>()
            .into()
    }

    pub fn getset(&self, key: &Bytes, value: &Value) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        self.expirations.lock().unwrap().remove(key);
        entries
            .insert(key.clone(), Entry::new(value.clone(), None))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    pub fn getdel(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        entries.remove(key).map_or(Value::Null, |x| {
            self.expirations.lock().unwrap().remove(key);
            x.clone_value()
        })
    }

    pub fn set(&self, key: &Bytes, value: Value, expires_in: Option<Duration>) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write().unwrap();
        let expires_at = expires_in.map(|duration| Instant::now() + duration);

        if let Some(expires_at) = expires_at {
            self.expirations.lock().unwrap().add(key, expires_at);
        }
        entries.insert(key.clone(), Entry::new(value, expires_at));
        Value::OK
    }

    pub fn ttl(&self, key: &Bytes) -> Option<Option<Instant>> {
        let entries = self.entries[self.get_slot(key)].read().unwrap();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map(|x| x.get_ttl())
    }

    pub fn purge(&self) -> u64 {
        let mut expirations = self.expirations.lock().unwrap();
        let mut removed = 0;

        trace!("Watching {} keys for expirations", expirations.len());

        let keys = expirations.get_expired_keys(None);
        drop(expirations);

        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write().unwrap();
                if entries.remove(key).is_some() {
                    trace!("Removed key {:?} due timeout", key);
                    removed += 1;
                }
            })
            .for_each(drop);

        removed
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bytes;

    #[test]
    fn incr_wrong_type() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("some string")), None);

        let r = db.incr(&bytes!("num"), 1);

        assert!(r.is_err());
        assert_eq!(Error::NotANumber, r.expect_err("should fail"));
        assert_eq!(Value::Blob(bytes!("some string")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_float() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1.1")), None);

        assert_eq!(Value::Float(2.2), db.incr(&bytes!("num"), 1.1).unwrap());
        assert_eq!(Value::Blob(bytes!("2.2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_float() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Value::Float(2.1), db.incr(&bytes!("num"), 1.1).unwrap());
        assert_eq!(Value::Blob(bytes!("2.1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Value::Integer(2), db.incr(&bytes!("num"), 1).unwrap());
        assert_eq!(Value::Blob(bytes!("2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_set() {
        let db = Db::new(100);
        assert_eq!(Value::Integer(1), db.incr(&bytes!("num"), 1).unwrap());
        assert_eq!(Value::Blob(bytes!("1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_float_set() {
        let db = Db::new(100);
        assert_eq!(Value::Float(1.1), db.incr(&bytes!("num"), 1.1).unwrap());
        assert_eq!(Value::Blob(bytes!("1.1")), db.get(&bytes!("num")));
    }

    #[test]
    fn del() {
        let db = Db::new(100);
        db.set(&bytes!(b"expired"), Value::OK, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"valid"), Value::OK, None);
        db.set(
            &bytes!(b"expiring"),
            Value::OK,
            Some(Duration::from_secs(5)),
        );

        assert_eq!(
            Value::Integer(2),
            db.del(&[
                bytes!(b"expired"),
                bytes!(b"valid"),
                bytes!(b"expiring"),
                bytes!(b"not_existing_key")
            ])
        );
    }

    #[test]
    fn ttl() {
        let db = Db::new(100);
        db.set(&bytes!(b"expired"), Value::OK, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"valid"), Value::OK, None);
        db.set(
            &bytes!(b"expiring"),
            Value::OK,
            Some(Duration::from_secs(5)),
        );

        assert_eq!(None, db.ttl(&bytes!(b"expired")));
        assert_eq!(None, db.ttl(&bytes!(b"not_existing_key")));
        assert_eq!(Some(None), db.ttl(&bytes!(b"valid")));
        assert!(match db.ttl(&bytes!(b"expiring")) {
            Some(Some(_)) => true,
            _ => false,
        });
    }

    #[test]
    fn purge_keys() {
        let db = Db::new(100);
        db.set(&bytes!(b"one"), Value::OK, Some(Duration::from_secs(0)));
        // Expired keys should not be returned, even if they are not yet
        // removed by the purge process.
        assert_eq!(Value::Null, db.get(&bytes!(b"one")));

        // Purge twice
        assert_eq!(1, db.purge());
        assert_eq!(0, db.purge());

        assert_eq!(Value::Null, db.get(&bytes!(b"one")));
    }

    #[test]
    fn replace_purge_keys() {
        let db = Db::new(100);
        db.set(&bytes!(b"one"), Value::OK, Some(Duration::from_secs(0)));
        // Expired keys should not be returned, even if they are not yet
        // removed by the purge process.
        assert_eq!(Value::Null, db.get(&bytes!(b"one")));

        db.set(&bytes!(b"one"), Value::OK, Some(Duration::from_secs(5)));
        assert_eq!(Value::OK, db.get(&bytes!(b"one")));

        // Purge should return 0 as the expired key has been removed already
        assert_eq!(0, db.purge());
    }
}
