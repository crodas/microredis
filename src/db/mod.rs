//! # In-Memory database
//!
//! This database module is the core of the miniredis project. All other modules around this
//! database module.
mod entry;
mod expiration;

use crate::{error::Error, value::Value};
use bytes::Bytes;
use entry::{new_version, Entry};
use expiration::ExpirationDb;
use log::trace;
use parking_lot::{Mutex, RwLock};
use seahash::hash;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    ops::AddAssign,
    sync::Arc,
    thread,
};
use tokio::time::{Duration, Instant};

/// Databas structure
///
/// Each connection has their own clone of the database and the conn_id is stored in each instance.
/// The entries property is shared for all connections.
///
/// To avoid lock contention this database is *not* a single HashMap, instead it is a vector of
/// HashMaps. Each key is presharded and a bucket is selected. By doing this pre-step instead of
/// locking the entire database, only a small portion is locked (shared or exclusively) at a time,
/// making this database implementation thread-friendly. The number of slots available cannot be
/// changed at runtime.
///
/// The database is also aware of other connections locking other keys exclusively (for
/// transactions).
///
/// Each entry is wrapped with an entry::Entry struct, which is aware of expirations and data
/// versioning (in practice the nanosecond of last modification).
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
    entries: Arc<Vec<RwLock<HashMap<Bytes, Entry>>>>,

    /// Data structure to store all expiring keys
    expirations: Arc<Mutex<ExpirationDb>>,

    /// Number of HashMaps that are available.
    slots: usize,

    /// A Database is attached to a conn_id. The entries and expiration data
    /// structures are shared between all connections.
    ///
    /// This particular database instace is attached to a conn_id, used to block
    /// all keys in case of a transaction.
    conn_id: u128,

    /// HashMap of all blocked keys by other connections. If a key appears in
    /// here and it is not being hold by the current connection, current
    /// connection must wait.
    tx_key_locks: Arc<RwLock<HashMap<Bytes, u128>>>,
}

impl Db {
    /// Creates a new database instance
    pub fn new(slots: usize) -> Self {
        let mut entries = vec![];

        for _i in 0..slots {
            entries.push(RwLock::new(HashMap::new()));
        }

        Self {
            entries: Arc::new(entries),
            expirations: Arc::new(Mutex::new(ExpirationDb::new())),
            conn_id: 0,
            tx_key_locks: Arc::new(RwLock::new(HashMap::new())),
            slots,
        }
    }

    /// Creates a new Database instance bound to a connection.
    ///
    /// This is particular useful when locking keys exclusively.
    ///
    /// All the internal data are shjared through an Arc.
    pub fn new_db_instance(self: Arc<Db>, conn_id: u128) -> Db {
        Self {
            entries: self.entries.clone(),
            tx_key_locks: self.tx_key_locks.clone(),
            expirations: self.expirations.clone(),
            conn_id,
            slots: self.slots,
        }
    }

    #[inline]
    /// Returns a slot where a key may be hosted.
    ///
    /// In order to avoid too much locks, instead of having a single hash a
    /// database instance is a set of hashes. Each key is pre-shared with a
    /// quick hashing algorithm to select a 'slot' or HashMap where it may be
    /// hosted.
    fn get_slot(&self, key: &Bytes) -> usize {
        let id = (hash(key) as usize) % self.entries.len();
        trace!("selected slot {} for key {:?}", id, key);

        let waiting = Duration::from_nanos(100);

        while let Some(blocker) = self.tx_key_locks.read().get(key) {
            // Loop while the key we are trying to access is being blocked by a
            // connection in a transaction
            if *blocker == self.conn_id {
                // the key is being blocked by ourself, it is safe to break the
                // waiting loop
                break;
            }

            thread::sleep(waiting);
        }

        id
    }

    /// Locks keys exclusively
    ///
    /// The locked keys are only accesible (read or write) by the connection
    /// that locked them, any other connection must wait until the locking
    /// connection releases them.
    ///
    /// This is used to simulate redis transactions. Transaction in Redis are
    /// atomic but pausing a multi threaded Redis just to keep the same promises
    /// was a bit extreme, that's the reason why a transaction will lock
    /// exclusively all keys involved.
    pub fn lock_keys(&self, keys: &[Bytes]) {
        let waiting = Duration::from_nanos(100);
        loop {
            let mut lock = self.tx_key_locks.write();
            let mut i = 0;

            for key in keys.iter() {
                if let Some(blocker) = lock.get(key) {
                    if *blocker == self.conn_id {
                        // It is blocked by us already.
                        continue;
                    }
                    // It is blocked by another tx, we need to break
                    // and retry to gain the lock over this key
                    break;
                }
                lock.insert(key.clone(), self.conn_id);
                i += 1;
            }

            if i == keys.len() {
                // All the involved keys are successfully being blocked
                // exclusively.
                break;
            }

            // We need to sleep a bit and retry.
            drop(lock);
            thread::sleep(waiting);
        }
    }

    /// Releases the lock on keys
    pub fn unlock_keys(&self, keys: &[Bytes]) {
        let mut lock = self.tx_key_locks.write();
        for key in keys.iter() {
            lock.remove(key);
        }
    }

    /// Increments a key's value by a given number
    ///
    /// If the stored value cannot be converted into a number an error will be
    /// thrown.
    pub fn incr<
        T: ToString + AddAssign + for<'a> TryFrom<&'a Value, Error = Error> + Into<Value> + Copy,
    >(
        &self,
        key: &Bytes,
        incr_by: T,
    ) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write();
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

    /// Removes any expiration associated with a given key
    pub fn persist(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0.into(), |x| {
                if x.has_ttl() {
                    self.expirations.lock().remove(key);
                    x.persist();
                    1.into()
                } else {
                    0.into()
                }
            })
    }

    /// Set time to live for a given key
    pub fn set_ttl(&self, key: &Bytes, expires_in: Duration) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        let expires_at = Instant::now() + expires_in;

        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0.into(), |x| {
                self.expirations.lock().add(key, expires_at);
                x.set_ttl(expires_at);
                1.into()
            })
    }

    /// Removes keys from the database
    pub fn del(&self, keys: &[Bytes]) -> Value {
        let mut expirations = self.expirations.lock();

        keys.iter()
            .filter_map(|key| {
                expirations.remove(key);
                self.entries[self.get_slot(key)].write().remove(key)
            })
            .filter(|key| key.is_valid())
            .count()
            .into()
    }

    /// Check if keys exists in the database
    pub fn exists(&self, keys: &[Bytes]) -> Value {
        let mut matches = 0;
        keys.iter()
            .map(|key| {
                let entries = self.entries[self.get_slot(key)].read();
                if entries.get(key).is_some() {
                    matches += 1;
                }
            })
            .for_each(drop);

        matches.into()
    }

    /// get_map_or
    ///
    /// Instead of returning an entry of the database, to avoid clonning, this function will
    /// execute a callback function with the entry as a parameter. If no record is found another
    /// callback function is going to be executed, dropping the lock before doing so.
    ///
    /// If an entry is found, the lock is not dropped before doing the callback. Avoid inserting
    /// new entries. In this case the value is passed by reference, so it is possible to modify the
    /// entry itself.
    ///
    /// This function is useful to read non-scalar values from the database. Non-scalar values are
    /// forbidden to clone, attempting cloning will endup in an error (Error::WrongType)
    pub fn get_map_or<F1, F2>(&self, key: &Bytes, found: F1, not_found: F2) -> Result<Value, Error>
    where
        F1: FnOnce(&Value) -> Result<Value, Error>,
        F2: FnOnce() -> Result<Value, Error>,
    {
        let entries = self.entries[self.get_slot(key)].read();
        let entry = entries.get(key).filter(|x| x.is_valid()).map(|e| e.get());

        if let Some(entry) = entry {
            found(entry)
        } else {
            // drop lock
            drop(entries);

            not_found()
        }
    }

    /// Updates the entry version of a given key
    pub fn bump_version(&self, key: &Bytes) -> bool {
        let mut entries = self.entries[self.get_slot(key)].write();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map(|entry| {
                entry.bump_version();
            })
            .is_some()
    }

    /// Returns the version of a given key
    pub fn get_version(&self, key: &Bytes) -> u128 {
        let entries = self.entries[self.get_slot(key)].read();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map(|entry| entry.version())
            .unwrap_or_else(new_version)
    }

    /// Get a copy of an entry
    pub fn get(&self, key: &Bytes) -> Value {
        let entries = self.entries[self.get_slot(key)].read();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    /// Get a copy of an entry and modifies the expiration of the key
    pub fn getex(&self, key: &Bytes, expires_in: Option<Duration>, make_persistent: bool) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        entries
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map(|value| {
                if make_persistent {
                    self.expirations.lock().remove(key);
                    value.persist();
                } else if let Some(expires_in) = expires_in {
                    let expires_at = Instant::now() + expires_in;
                    self.expirations.lock().add(key, expires_at);
                    value.set_ttl(expires_at);
                }
                value
            })
            .map_or(Value::Null, |x| x.clone_value())
    }

    /// Get multiple copies of entries
    pub fn get_multi(&self, keys: &[Bytes]) -> Value {
        keys.iter()
            .map(|key| {
                let entries = self.entries[self.get_slot(key)].read();
                entries
                    .get(key)
                    .filter(|x| x.is_valid() && x.is_clonable())
                    .map_or(Value::Null, |x| x.clone_value())
            })
            .collect::<Vec<Value>>()
            .into()
    }

    /// Get a key or set a new value for the given key.
    pub fn getset(&self, key: &Bytes, value: &Value) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        self.expirations.lock().remove(key);
        entries
            .insert(key.clone(), Entry::new(value.clone(), None))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    /// Takes an entry from the database.
    pub fn getdel(&self, key: &Bytes) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        entries.remove(key).map_or(Value::Null, |x| {
            self.expirations.lock().remove(key);
            x.clone_value()
        })
    }
    ///
    /// Set a key, value with an optional expiration time
    pub fn append(&self, key: &Bytes, value_to_append: &Bytes) -> Result<Value, Error> {
        let mut entries = self.entries[self.get_slot(key)].write();
        let mut entry = entries.get_mut(key).filter(|x| x.is_valid());

        if let Some(entry) = entries.get_mut(key).filter(|x| x.is_valid()) {
            match entry.get() {
                Value::Blob(value) => {
                    let binary: Bytes = [value.as_ref(), value_to_append.as_ref()].concat().into();
                    let len = binary.len();
                    entry.change_value(Value::Blob(binary));
                    Ok(len.into())
                }
                _ => Err(Error::WrongType),
            }
        } else {
            entries.insert(
                key.clone(),
                Entry::new(Value::Blob(value_to_append.clone()), None),
            );
            Ok(value_to_append.len().into())
        }
    }

    /// Set multiple key/value pairs. Are involved keys are locked exclusively
    /// like a transaction.
    ///
    /// If override_all is set to false, all entries must be new entries or the
    /// entire operation fails, in this case 1 or is returned. Otherwise `Ok` is
    /// returned.
    pub fn multi_set(&self, key_values: &[Bytes], override_all: bool) -> Value {
        let keys = key_values
            .iter()
            .step_by(2)
            .cloned()
            .collect::<Vec<Bytes>>();

        self.lock_keys(&keys);

        if !override_all {
            for key in keys.iter() {
                let entries = self.entries[self.get_slot(key)].read();
                if entries.get(key).is_some() {
                    self.unlock_keys(&keys);
                    return 0.into();
                }
            }
        }

        for (i, _) in key_values.iter().enumerate().step_by(2) {
            let mut entries = self.entries[self.get_slot(&key_values[i])].write();
            entries.insert(
                key_values[i].clone(),
                Entry::new(Value::Blob(key_values[i + 1].clone()), None),
            );
        }

        self.unlock_keys(&keys);

        if override_all {
            Value::Ok
        } else {
            1.into()
        }
    }

    /// Set a key, value with an optional expiration time
    pub fn set(&self, key: &Bytes, value: Value, expires_in: Option<Duration>) -> Value {
        let mut entries = self.entries[self.get_slot(key)].write();
        let expires_at = expires_in.map(|duration| Instant::now() + duration);

        if let Some(expires_at) = expires_at {
            self.expirations.lock().add(key, expires_at);
        }
        entries.insert(key.clone(), Entry::new(value, expires_at));
        Value::Ok
    }

    /// Returns the TTL of a given key
    pub fn ttl(&self, key: &Bytes) -> Option<Option<Instant>> {
        let entries = self.entries[self.get_slot(key)].read();
        entries
            .get(key)
            .filter(|x| x.is_valid())
            .map(|x| x.get_ttl())
    }

    /// Check whether a given key is in the list of keys to be purged or not.
    /// This function is mainly used for unit testing
    pub fn is_key_in_expiration_list(&self, key: &Bytes) -> bool {
        self.expirations.lock().has(key)
    }

    /// Remove expired entries from the database.
    ///
    /// This function should be called from a background thread every few seconds. Calling it more
    /// often is a waste of resources.
    ///
    /// Expired keys are automatically hidden by the database, this process is just claiming back
    /// the memory from those expired keys.
    pub fn purge(&self) -> u64 {
        let mut expirations = self.expirations.lock();
        let mut removed = 0;

        trace!("Watching {} keys for expirations", expirations.len());

        let keys = expirations.get_expired_keys(None);
        drop(expirations);

        keys.iter()
            .map(|key| {
                let mut entries = self.entries[self.get_slot(key)].write();
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

        assert_eq!(Ok(Value::Float(2.2)), db.incr(&bytes!("num"), 1.1));
        assert_eq!(Value::Blob(bytes!("2.2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_float() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Ok(Value::Float(2.1)), db.incr(&bytes!("num"), 1.1));
        assert_eq!(Value::Blob(bytes!("2.1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Ok(Value::Integer(2)), db.incr(&bytes!("num"), 1));
        assert_eq!(Value::Blob(bytes!("2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_set() {
        let db = Db::new(100);
        assert_eq!(Ok(Value::Integer(1)), db.incr(&bytes!("num"), 1));
        assert_eq!(Value::Blob(bytes!("1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_float_set() {
        let db = Db::new(100);
        assert_eq!(Ok(Value::Float(1.1)), db.incr(&bytes!("num"), 1.1));
        assert_eq!(Value::Blob(bytes!("1.1")), db.get(&bytes!("num")));
    }

    #[test]
    fn del() {
        let db = Db::new(100);
        db.set(&bytes!(b"expired"), Value::Ok, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"valid"), Value::Ok, None);
        db.set(
            &bytes!(b"expiring"),
            Value::Ok,
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
        db.set(&bytes!(b"expired"), Value::Ok, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"valid"), Value::Ok, None);
        db.set(
            &bytes!(b"expiring"),
            Value::Ok,
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
    fn persist_bug() {
        let db = Db::new(100);
        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(1)));
        assert_eq!(Value::Ok, db.get(&bytes!(b"one")));
        assert!(db.is_key_in_expiration_list(&bytes!(b"one")));
        db.persist(&bytes!(b"one"));
        assert!(!db.is_key_in_expiration_list(&bytes!(b"one")));
    }

    #[test]
    fn purge_keys() {
        let db = Db::new(100);
        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(0)));
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
        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(0)));
        // Expired keys should not be returned, even if they are not yet
        // removed by the purge process.
        assert_eq!(Value::Null, db.get(&bytes!(b"one")));

        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(5)));
        assert_eq!(Value::Ok, db.get(&bytes!(b"one")));

        // Purge should return 0 as the expired key has been removed already
        assert_eq!(0, db.purge());
    }
}
