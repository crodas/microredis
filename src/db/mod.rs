//! # in-memory database
//!
//! This database module is the core of the miniredis project. All other modules around this
//! database module.
use self::utils::{far_future, ExpirationOpts, Override};
use crate::{
    error::Error,
    value::{
        bytes_to_number,
        cursor::Cursor,
        typ::{Typ, ValueTyp},
        VDebug, Value,
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use core::num;
use entry::{new_version, Entry};
use expiration::ExpirationDb;
use glob::Pattern;
use log::trace;
use num_traits::CheckedAdd;
use parking_lot::{Mutex, RwLock};
use seahash::hash;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    ops::{AddAssign, Deref},
    str::FromStr,
    sync::Arc,
    thread,
};
use tokio::time::{Duration, Instant};

mod entry;
mod expiration;
pub mod pool;
pub mod scan;
pub(crate) mod utils;

/// Database structure
///
/// Each connection has their own clone of the database and the conn_id is stored in each instance.
/// The slots property is shared for all connections.
///
/// To avoid lock contention this database is *not* a single HashMap, instead it is a vector of
/// HashMaps. Each key is pre-sharded and a bucket is selected. By doing this pre-step instead of
/// locking the entire database, only a small portion is locked (shared or exclusively) at a time,
/// making this database implementation thread-friendly. The number of number_of_slots available cannot be
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
    slots: Arc<Vec<RwLock<HashMap<Bytes, Entry>>>>,

    /// Data structure to store all expiring keys
    expirations: Arc<Mutex<ExpirationDb>>,

    /// Number of HashMaps that are available.
    number_of_slots: usize,

    /// Databases unique ID. This is an internal identifier to avoid deadlocks
    /// when copying and moving data between databases.
    pub db_id: u128,

    /// Current connection  ID
    ///
    /// A Database is attached to a conn_id. The slots and expiration data
    /// structures are shared between all connections, regardless of conn_id.
    ///
    /// This particular database instace is attached to a conn_id, which is used
    /// to lock keys exclusively for transactions and other atomic operations.
    conn_id: u128,

    /// HashMap of all blocked keys by other connections. If a key appears in
    /// here and it is not being hold by the current connection, current
    /// connection must wait.
    tx_key_locks: Arc<RwLock<HashMap<Bytes, u128>>>,
}

impl Db {
    /// Creates a new database instance
    pub fn new(number_of_slots: usize) -> Self {
        let slots = (0..number_of_slots)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();

        Self {
            slots: Arc::new(slots),
            expirations: Arc::new(Mutex::new(ExpirationDb::new())),
            conn_id: 0,
            db_id: new_version(),
            tx_key_locks: Arc::new(RwLock::new(HashMap::new())),
            number_of_slots,
        }
    }

    /// Creates a new Database instance bound to a connection.
    ///
    /// This is particular useful when locking keys exclusively.
    ///
    /// All the internal data are shjared through an Arc.
    pub fn new_db_instance(self: Arc<Db>, conn_id: u128) -> Arc<Db> {
        Arc::new(Self {
            slots: self.slots.clone(),
            tx_key_locks: self.tx_key_locks.clone(),
            expirations: self.expirations.clone(),
            conn_id,
            db_id: self.db_id,
            number_of_slots: self.number_of_slots,
        })
    }

    #[inline]
    /// Returns a slot where a key may be hosted.
    ///
    /// In order to avoid too much locks, instead of having a single hash a
    /// database instance is a set of hashes. Each key is pre-shared with a
    /// quick hashing algorithm to select a 'slot' or HashMap where it may be
    /// hosted.
    fn get_slot(&self, key: &Bytes) -> usize {
        let id = (hash(key) as usize) % self.number_of_slots;
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

    /// Return debug info for a key
    pub fn debug(&self, key: &Bytes) -> Result<VDebug, Error> {
        let slot = self.slots[self.get_slot(key)].read();
        Ok(slot
            .get(key)
            .filter(|x| x.is_valid())
            .ok_or(Error::NotFound)?
            .value
            .debug())
    }

    /// Return the digest for each key. This used for testing only
    pub fn digest(&self, keys: &[Bytes]) -> Result<Vec<Value>, Error> {
        Ok(keys
            .iter()
            .map(|key| {
                let slot = self.slots[self.get_slot(key)].read();
                Value::Blob(
                    slot.get(key)
                        .filter(|v| v.is_valid())
                        .map(|v| hex::encode(&v.value.digest()))
                        .unwrap_or("00000".into())
                        .as_str()
                        .into(),
                )
            })
            .collect::<Vec<Value>>())
    }

    /// Flushes the entire database
    pub fn flushdb(&self) -> Result<Value, Error> {
        self.expirations.lock().flush();
        self.slots
            .iter()
            .map(|s| {
                let mut s = s.write();
                s.clear();
            })
            .for_each(drop);
        Ok(Value::Ok)
    }

    /// Returns the number of elements in the database
    pub fn len(&self) -> Result<usize, Error> {
        self.purge();
        Ok(self.slots.iter().map(|s| s.read().len()).sum())
    }

    /// Round numbers to store efficiently, specially float numbers. For instance `1.00` will be converted to `1`.
    fn round_numbers<T>(number: T) -> BytesMut
    where
        T: ToString,
    {
        let number_to_str = number.to_string();

        if number_to_str.find('.').is_none() {
            return number_to_str.as_bytes().into();
        }

        let number_to_str = number_to_str
            .trim_end_matches(|c| c == '0' || c == '.')
            .to_string();

        if number_to_str.is_empty() {
            "0"
        } else {
            number_to_str.as_str()
        }
        .into()
    }

    // Converts a given number to a correct Value, it should be used with Self::round_numbers()
    fn number_to_value(number: &[u8]) -> Result<Value, Error> {
        if number.iter().find(|x| **x == b'.').is_some() {
            Ok(Value::new(number))
        } else {
            Ok(Value::Integer(bytes_to_number(number)?))
        }
    }

    /// Increment a sub-key in a hash
    ///
    /// If the stored value cannot be converted into a number an error will be thrown
    pub fn hincrby<T>(
        &self,
        key: &Bytes,
        sub_key: &Bytes,
        incr_by: &Bytes,
        typ: &str,
    ) -> Result<Value, Error>
    where
        T: ToString
            + FromStr
            + CheckedAdd
            + for<'a> TryFrom<&'a Value, Error = Error>
            + Into<Value>
            + Copy,
    {
        let mut slot = self.slots[self.get_slot(key)].write();
        let mut incr_by: T =
            bytes_to_number(incr_by).map_err(|_| Error::NotANumberType(typ.to_owned()))?;
        match slot.get_mut(key).filter(|x| x.is_valid()).map(|x| x.get()) {
            Some(Value::Hash(h)) => {
                let mut h = h.write();
                if let Some(n) = h.get(sub_key) {
                    incr_by = incr_by
                        .checked_add(
                            &bytes_to_number(n)
                                .map_err(|_| Error::NotANumberType(typ.to_owned()))?,
                        )
                        .ok_or(Error::Overflow)?;
                }
                let incr_by_bytes = Self::round_numbers(incr_by).freeze();
                h.insert(sub_key.clone(), incr_by_bytes.clone());

                Self::number_to_value(&incr_by_bytes)
            }
            None => {
                #[allow(clippy::mutable_key_type)]
                let mut h = HashMap::new();
                let incr_by_bytes = Self::round_numbers(incr_by).freeze();
                h.insert(sub_key.clone(), incr_by_bytes.clone());
                let _ = slot.insert(key.clone(), Entry::new(h.into(), None));
                Self::number_to_value(&incr_by_bytes)
            }
            _ => Err(Error::WrongType),
        }
    }

    /// Increments a key's value by a given number
    ///
    /// If the stored value cannot be converted into a number an error will be
    /// thrown.
    pub fn incr<T>(&self, key: &Bytes, incr_by: T) -> Result<T, Error>
    where
        T: ToString + CheckedAdd + for<'a> TryFrom<&'a Value, Error = Error> + Into<Value> + Copy,
    {
        let mut slot = self.slots[self.get_slot(key)].write();
        match slot.get_mut(key).filter(|x| x.is_valid()) {
            Some(x) => {
                if !x.is_clonable() {
                    return Err(Error::WrongType);
                }
                let value = x.get();
                let mut number: T = value.try_into()?;

                number = incr_by.checked_add(&number).ok_or(Error::Overflow)?;

                x.change_value(Value::Blob(Self::round_numbers(number)));

                Ok(number)
            }
            None => {
                slot.insert(
                    key.clone(),
                    Entry::new(Value::Blob(Self::round_numbers(incr_by)), None),
                );
                Ok(incr_by)
            }
        }
    }

    /// Removes any expiration associated with a given key
    pub fn persist(&self, key: &Bytes) -> Value {
        let mut slot = self.slots[self.get_slot(key)].write();
        slot.get_mut(key)
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
    pub fn set_ttl(
        &self,
        key: &Bytes,
        expires_in: Duration,
        opts: ExpirationOpts,
    ) -> Result<Value, Error> {
        if (opts.NX && opts.XX) || (opts.NX && opts.GT) || (opts.NX && opts.LT) {
            return Err(Error::OptsNotCompatible("NX and XX, GT or LT".to_owned()));
        }

        if opts.GT && opts.LT {
            return Err(Error::OptsNotCompatible("GT and LT".to_owned()));
        }

        let mut slot = self.slots[self.get_slot(key)].write();
        let expires_at = Instant::now()
            .checked_add(expires_in)
            .unwrap_or_else(far_future);

        Ok(slot
            .get_mut(key)
            .filter(|x| x.is_valid())
            .map_or(0.into(), |x| {
                let current_expire = x.get_ttl();
                if opts.NX && current_expire.is_some() {
                    return 0.into();
                }
                if opts.XX && current_expire.is_none() {
                    return 0.into();
                }
                if opts.GT {
                    if let Some(current_expire) = current_expire {
                        if expires_at <= current_expire {
                            return 0.into();
                        }
                    } else {
                        return 0.into();
                    }
                }

                if opts.LT {
                    if let Some(current_expire) = current_expire {
                        if expires_at >= current_expire {
                            return 0.into();
                        }
                    }
                }

                self.expirations.lock().add(key, expires_at);
                x.set_ttl(expires_at);
                1.into()
            }))
    }

    /// Overwrites part of the string stored at key, starting at the specified
    /// offset, for the entire length of value. If the offset is larger than the
    /// current length of the string at key, the string is padded with zero-bytes to
    /// make offset fit. Non-existing keys are considered as empty strings, so this
    /// command will make sure it holds a string large enough to be able to set
    /// value at offset.
    pub fn set_range(&self, key: &Bytes, offset: i128, data: &[u8]) -> Result<Value, Error> {
        let mut slot = self.slots[self.get_slot(key)].write();
        let value = slot.get_mut(key).map(|value| {
            if !value.is_valid() {
                self.expirations.lock().remove(key);
                value.persist();
            }
            value.get_mut()
        });

        if offset < 0 {
            return Err(Error::OutOfRange);
        }

        if offset >= 512 * 1024 * 1024 - 4 {
            return Err(Error::MaxAllowedSize);
        }

        let length = offset as usize + data.len();
        match value {
            Some(Value::Blob(bytes)) => {
                if bytes.capacity() < length {
                    bytes.resize(length, 0);
                }
                let writer = &mut bytes[offset as usize..length];
                writer.copy_from_slice(data);
                Ok(bytes.len().into())
            }
            None => {
                if data.len() == 0 {
                    return Ok(0.into());
                }
                let mut bytes = BytesMut::new();
                bytes.resize(length, 0);
                let writer = &mut bytes[offset as usize..];
                writer.copy_from_slice(data);
                slot.insert(key.clone(), Entry::new(Value::new(&bytes), None));
                Ok(bytes.len().into())
            }
            _ => Err(Error::WrongType),
        }
    }

    /// Copies a key
    pub fn copy(
        &self,
        source: &Bytes,
        target: &Bytes,
        replace: Override,
        target_db: Option<Arc<Db>>,
    ) -> Result<bool, Error> {
        let slot = self.slots[self.get_slot(source)].read();
        let value = if let Some(value) = slot.get(source).filter(|x| x.is_valid()) {
            value.clone()
        } else {
            return Ok(false);
        };
        drop(slot);

        if let Some(db) = target_db {
            if db.db_id == self.db_id && source == target {
                return Err(Error::SameEntry);
            }
            if replace == Override::No && db.exists(&[target.clone()]) > 0 {
                return Ok(false);
            }
            let _ = db.set_advanced(
                target,
                value.value.clone(),
                value.get_ttl().map(|v| v - Instant::now()),
                replace,
                false,
                false,
            );
            Ok(true)
        } else {
            if source == target {
                return Err(Error::SameEntry);
            }

            if replace == Override::No && self.exists(&[target.clone()]) > 0 {
                return Ok(false);
            }
            let mut slot = self.slots[self.get_slot(target)].write();
            slot.insert(target.clone(), value);

            Ok(true)
        }
    }

    /// Moves a given key between databases
    pub fn move_key(&self, source: &Bytes, target_db: Arc<Db>) -> Result<bool, Error> {
        if self.db_id == target_db.db_id {
            return Err(Error::SameEntry);
        }
        let mut slot = self.slots[self.get_slot(source)].write();
        let (expires_in, value) = if let Some(value) = slot.get(source).filter(|v| v.is_valid()) {
            (
                value.get_ttl().map(|t| t - Instant::now()),
                value.value.clone(),
            )
        } else {
            return Ok(false);
        };

        if Value::Integer(1)
            == target_db.set_advanced(&source, value, expires_in, Override::No, false, false)
        {
            slot.remove(source);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Renames a key
    pub fn rename(
        &self,
        source: &Bytes,
        target: &Bytes,
        override_value: Override,
    ) -> Result<bool, Error> {
        let slot1 = self.get_slot(source);
        let slot2 = self.get_slot(target);

        if slot1 == slot2 {
            let mut slot = self.slots[slot1].write();

            if override_value == Override::No && slot.get(target).is_some() {
                return Ok(false);
            }

            if let Some(value) = slot.remove(source) {
                slot.insert(target.clone(), value);
                Ok(true)
            } else {
                Err(Error::NotFound)
            }
        } else {
            let mut slot1 = self.slots[slot1].write();
            let mut slot2 = self.slots[slot2].write();
            if override_value == Override::No && slot2.get(target).is_some() {
                return Ok(false);
            }
            if let Some(value) = slot1.remove(source) {
                slot2.insert(target.clone(), value);
                Ok(true)
            } else {
                Err(Error::NotFound)
            }
        }
    }

    /// Removes keys from the database
    pub fn del(&self, keys: &[Bytes]) -> Value {
        let mut expirations = self.expirations.lock();

        keys.iter()
            .filter_map(|key| {
                expirations.remove(key);
                self.slots[self.get_slot(key)].write().remove(key)
            })
            .filter(|key| key.is_valid())
            .count()
            .into()
    }

    /// Returns all keys that matches a given pattern. This is a very expensive command.
    pub fn get_all_keys(&self, pattern: &Bytes) -> Result<Vec<Value>, Error> {
        let pattern = String::from_utf8_lossy(pattern);
        let pattern =
            Pattern::new(&pattern).map_err(|_| Error::InvalidPattern(pattern.to_string()))?;
        Ok(self
            .slots
            .iter()
            .map(|slot| {
                slot.read()
                    .keys()
                    .filter(|key| {
                        let str_key = String::from_utf8_lossy(key);
                        pattern.matches(&str_key)
                    })
                    .map(|key| Value::new(key))
                    .collect::<Vec<Value>>()
            })
            .flatten()
            .collect())
    }

    /// Check if keys exists in the database
    pub fn exists(&self, keys: &[Bytes]) -> usize {
        let mut matches = 0;
        keys.iter()
            .map(|key| {
                let slot = self.slots[self.get_slot(key)].read();
                if slot.get(key).is_some() {
                    matches += 1;
                }
            })
            .for_each(drop);

        matches
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
        let slot = self.slots[self.get_slot(key)].read();
        let entry = slot.get(key).filter(|x| x.is_valid()).map(|e| e.get());

        if let Some(entry) = entry {
            found(entry)
        } else {
            // drop lock
            drop(slot);

            not_found()
        }
    }

    /// Updates the entry version of a given key
    pub fn bump_version(&self, key: &Bytes) -> bool {
        let mut slot = self.slots[self.get_slot(key)].write();
        slot.get_mut(key)
            .filter(|x| x.is_valid())
            .map(|entry| {
                entry.bump_version();
            })
            .is_some()
    }

    /// Returns the version of a given key
    pub fn get_version(&self, key: &Bytes) -> u128 {
        let slot = self.slots[self.get_slot(key)].read();
        slot.get(key)
            .filter(|x| x.is_valid())
            .map(|entry| entry.version())
            .unwrap_or_default()
    }

    /// Returns the name of the value type
    pub fn get_data_type(&self, key: &Bytes) -> String {
        let slot = self.slots[self.get_slot(key)].read();
        slot.get(key)
            .filter(|x| x.is_valid())
            .map_or("none".to_owned(), |x| {
                Typ::get_type(x.get()).to_string().to_lowercase()
            })
    }

    /// Get a copy of an entry
    pub fn get(&self, key: &Bytes) -> Value {
        let slot = self.slots[self.get_slot(key)].read();
        slot.get(key)
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    /// Get a copy of an entry and modifies the expiration of the key
    pub fn getex(&self, key: &Bytes, expires_in: Option<Duration>, make_persistent: bool) -> Value {
        let mut slot = self.slots[self.get_slot(key)].write();
        slot.get_mut(key)
            .filter(|x| x.is_valid())
            .map(|value| {
                if make_persistent {
                    self.expirations.lock().remove(key);
                    value.persist();
                } else if let Some(expires_in) = expires_in {
                    let expires_at = Instant::now()
                        .checked_add(expires_in)
                        .unwrap_or_else(far_future);
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
                let slot = self.slots[self.get_slot(key)].read();
                slot.get(key)
                    .filter(|x| x.is_valid() && x.is_clonable())
                    .map_or(Value::Null, |x| x.clone_value())
            })
            .collect::<Vec<Value>>()
            .into()
    }

    /// Get a key or set a new value for the given key.
    pub fn getset(&self, key: &Bytes, value: Value) -> Value {
        let mut slot = self.slots[self.get_slot(key)].write();
        self.expirations.lock().remove(key);
        slot.insert(key.clone(), Entry::new(value, None))
            .filter(|x| x.is_valid())
            .map_or(Value::Null, |x| x.clone_value())
    }

    /// Takes an entry from the database.
    pub fn getdel(&self, key: &Bytes) -> Value {
        let mut slot = self.slots[self.get_slot(key)].write();
        slot.remove(key).map_or(Value::Null, |x| {
            self.expirations.lock().remove(key);
            x.clone_value()
        })
    }

    /// Set a key, value with an optional expiration time
    pub fn append(&self, key: &Bytes, value_to_append: &Bytes) -> Result<Value, Error> {
        let mut slot = self.slots[self.get_slot(key)].write();
        let mut entry = slot.get_mut(key).filter(|x| x.is_valid());

        if let Some(entry) = slot.get_mut(key).filter(|x| x.is_valid()) {
            match entry.get_mut() {
                Value::Blob(value) => {
                    value.put(value_to_append.as_ref());
                    Ok(value.len().into())
                }
                _ => Err(Error::WrongType),
            }
        } else {
            slot.insert(key.clone(), Entry::new(Value::new(value_to_append), None));
            Ok(value_to_append.len().into())
        }
    }

    /// Set multiple key/value pairs. Are involved keys are locked exclusively
    /// like a transaction.
    ///
    /// If override_all is set to false, all entries must be new entries or the
    /// entire operation fails, in this case 1 or is returned. Otherwise `Ok` is
    /// returned.
    pub fn multi_set(&self, key_values: &[Bytes], override_all: bool) -> Result<Value, Error> {
        if key_values.len() % 2 == 1 {
            return Err(Error::Syntax);
        }

        let keys = key_values
            .iter()
            .step_by(2)
            .cloned()
            .collect::<Vec<Bytes>>();

        self.lock_keys(&keys);

        if !override_all {
            for key in keys.iter() {
                let slot = self.slots[self.get_slot(key)].read();
                if slot.get(key).is_some() {
                    self.unlock_keys(&keys);
                    return Ok(0.into());
                }
            }
        }

        for (i, _) in key_values.iter().enumerate().step_by(2) {
            let mut slot = self.slots[self.get_slot(&key_values[i])].write();
            slot.insert(
                key_values[i].clone(),
                Entry::new(Value::new(&key_values[i + 1]), None),
            );
        }

        self.unlock_keys(&keys);

        if override_all {
            Ok(Value::Ok)
        } else {
            Ok(1.into())
        }
    }

    /// Set a key, value with an optional expiration time
    pub fn set(&self, key: &Bytes, value: Value, expires_in: Option<Duration>) -> Value {
        self.set_advanced(key, value, expires_in, Default::default(), false, false)
    }

    /// Set a value in the database with various settings
    pub fn set_advanced(
        &self,
        key: &Bytes,
        value: Value,
        expires_in: Option<Duration>,
        override_value: Override,
        keep_ttl: bool,
        return_previous: bool,
    ) -> Value {
        let mut slot = self.slots[self.get_slot(key)].write();
        let expires_at = expires_in.map(|duration| {
            Instant::now()
                .checked_add(duration)
                .unwrap_or_else(far_future)
        });
        let previous = slot.get(key).filter(|x| x.is_valid());

        let expires_at = if keep_ttl {
            if let Some(previous) = previous {
                previous.get_ttl()
            } else {
                expires_at
            }
        } else {
            expires_at
        };

        let to_return = if return_previous {
            let previous = previous.map_or(Value::Null, |v| v.clone_value());
            if previous.is_err() {
                // Error while trying to clone the previous value to return, we
                // must halt and return immediately.
                return previous;
            }
            Some(previous)
        } else {
            None
        };

        match override_value {
            Override::No => {
                if previous.is_some() {
                    return if let Some(to_return) = to_return {
                        to_return
                    } else {
                        0.into()
                    };
                }
            }
            Override::Only => {
                if previous.is_none() {
                    return if let Some(to_return) = to_return {
                        to_return
                    } else {
                        0.into()
                    };
                }
            }
            _ => {}
        };

        if let Some(expires_at) = expires_at {
            self.expirations.lock().add(key, expires_at);
        } else {
            /// Make sure to remove the new key (or replaced) from the
            /// expiration table (from any possible past value).
            self.expirations.lock().remove(key);
        }

        slot.insert(key.clone(), Entry::new(value, expires_at));

        if let Some(to_return) = to_return {
            to_return
        } else if override_value == Override::Yes {
            Value::Ok
        } else {
            1.into()
        }
    }

    /// Returns the TTL of a given key
    pub fn ttl(&self, key: &Bytes) -> Option<Option<Instant>> {
        let slot = self.slots[self.get_slot(key)].read();
        slot.get(key).filter(|x| x.is_valid()).map(|x| x.get_ttl())
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
                let mut slot = self.slots[self.get_slot(key)].write();
                if slot.remove(key).is_some() {
                    trace!("Removed key {:?} due timeout", key);
                    removed += 1;
                }
            })
            .for_each(drop);

        removed
    }
}

impl scan::Scan for Db {
    fn scan(
        &self,
        cursor: Cursor,
        pattern: Option<&Bytes>,
        count: Option<usize>,
        typ: Option<Typ>,
    ) -> Result<scan::Result, Error> {
        let mut keys = vec![];
        let mut slot_id = cursor.bucket as usize;
        let mut last_pos = cursor.last_position as usize;
        let pattern = pattern
            .map(|pattern| {
                let pattern = String::from_utf8_lossy(pattern);
                Pattern::new(&pattern).map_err(|_| Error::InvalidPattern(pattern.to_string()))
            })
            .transpose()?;

        loop {
            let slot = if let Some(value) = self.slots.get(slot_id) {
                value.read()
            } else {
                // We iterated through all the entries, time to signal that to
                // the client but returning a "0" cursor.
                slot_id = 0;
                last_pos = 0;
                break;
            };

            for (key, value) in slot.iter().skip(last_pos) {
                if !value.is_valid() {
                    // Entry still exists in memory but it is not longer valid
                    // and will soon be gargabe collected.
                    last_pos += 1;
                    continue;
                }
                if let Some(pattern) = &pattern {
                    let str_key = String::from_utf8_lossy(key);
                    if !pattern.matches(&str_key) {
                        last_pos += 1;
                        continue;
                    }
                }
                if let Some(typ) = &typ {
                    if !typ.is_value_type(value.get()) {
                        last_pos += 1;
                        continue;
                    }
                }
                keys.push(Value::new(key));
                last_pos += 1;
                if keys.len() == count.unwrap_or(10) {
                    break;
                }
            }

            if keys.len() == count.unwrap_or(10) {
                break;
            }

            last_pos = 0;
            slot_id += 1;
        }

        Ok(scan::Result {
            cursor: Cursor::new(slot_id as u16, last_pos as u64)?,
            result: keys,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{bytes, db::scan::Scan, value::float::Float};
    use std::str::FromStr;

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

        assert_eq!(Ok(2.2.into()), db.incr::<Float>(&bytes!("num"), 1.1.into()));
        assert_eq!(Value::Blob(bytes!("2.2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_float() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Ok(2.1.into()), db.incr::<Float>(&bytes!("num"), 1.1.into()));
        assert_eq!(Value::Blob(bytes!("2.1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int() {
        let db = Db::new(100);
        db.set(&bytes!(b"num"), Value::Blob(bytes!("1")), None);

        assert_eq!(Ok(2), db.incr(&bytes!("num"), 1));
        assert_eq!(Value::Blob(bytes!("2")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_int_set() {
        let db = Db::new(100);
        assert_eq!(Ok(1), db.incr(&bytes!("num"), 1));
        assert_eq!(Value::Blob(bytes!("1")), db.get(&bytes!("num")));
    }

    #[test]
    fn incr_blob_float_set() {
        let db = Db::new(100);
        assert_eq!(Ok(1.1.into()), db.incr::<Float>(&bytes!("num"), 1.1.into()));
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

    #[test]
    fn scan_skip_expired() {
        let db = Db::new(100);
        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"two"), Value::Ok, Some(Duration::from_secs(0)));
        for i in 0u64..20u64 {
            let key: Bytes = i.to_string().into();
            db.set(&key, Value::Ok, None);
        }
        let result = db
            .scan(Cursor::from_str("0").unwrap(), None, None, None)
            .unwrap();
        // first 10 records
        assert_eq!(10, result.result.len());
        // make sure the cursor is valid
        assert_ne!("0", result.cursor.to_string());
        let result = db.scan(result.cursor, None, None, None).unwrap();
        // 10 more records
        assert_eq!(10, result.result.len());
        // make sure the cursor is valid
        assert_ne!("0", result.cursor.to_string());

        let result = db.scan(result.cursor, None, None, None).unwrap();
        // No more results!
        assert_eq!(0, result.result.len());
        assert_eq!("0", result.cursor.to_string());
    }

    #[test]
    fn scan_limit() {
        let db = Db::new(10);
        db.set(&bytes!(b"one"), Value::Ok, Some(Duration::from_secs(0)));
        db.set(&bytes!(b"two"), Value::Ok, Some(Duration::from_secs(0)));
        for i in 0u64..2000u64 {
            let key: Bytes = i.to_string().into();
            db.set(&key, Value::Ok, None);
        }
        let result = db
            .scan(Cursor::from_str("0").unwrap(), None, Some(2), None)
            .unwrap();
        assert_eq!(2, result.result.len());
        assert_ne!("0", result.cursor.to_string());
    }

    #[test]
    fn scan_filter() {
        let db = Db::new(100);
        db.set(&bytes!(b"fone"), Value::Ok, None);
        db.set(&bytes!(b"ftwo"), Value::Ok, None);
        for i in 0u64..20u64 {
            let key: Bytes = i.to_string().into();
            db.set(&key, Value::Ok, None);
        }
        let result = db
            .scan(
                Cursor::from_str("0").unwrap(),
                Some(&bytes!(b"f*")),
                None,
                None,
            )
            .unwrap();
        assert_eq!(2, result.result.len());
        assert_eq!("0", result.cursor.to_string());
    }
}
