use crate::{error::Error, value::Value};
use bytes::BytesMut;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Instant;

#[derive(Debug)]
pub struct Entry {
    value: RwLock<Value>,
    version: AtomicUsize,
    expires_at: Mutex<Option<Instant>>,
}

static LAST_VERSION: AtomicUsize = AtomicUsize::new(0);

/// Returns a new version
pub fn unique_id() -> usize {
    LAST_VERSION.fetch_add(1, Ordering::Relaxed)
}

/// Database Entry
///
/// A database entry is a Value associated with an optional ttl.
///
/// The database will never return an entry if has expired already, by having
/// this promise we can run the purge process every few seconds instead of doing
/// so more frequently.
impl Entry {
    pub fn new(value: Value, expires_at: Option<Instant>) -> Self {
        Self {
            value: RwLock::new(value),
            expires_at: Mutex::new(expires_at),
            version: AtomicUsize::new(LAST_VERSION.fetch_add(1, Ordering::Relaxed)),
        }
    }

    #[inline(always)]
    pub fn take_value(self) -> Value {
        self.value.into_inner()
    }

    #[inline(always)]
    pub fn digest(&self) -> Vec<u8> {
        self.value.read().digest()
    }

    #[inline(always)]
    pub fn bump_version(&self) {
        self.version.store(
            LAST_VERSION.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        )
    }

    pub fn persist(&self) {
        *self.expires_at.lock() = None;
    }

    pub fn clone(&self) -> Self {
        Self::new(self.value.read().clone(), *self.expires_at.lock())
    }

    pub fn get_ttl(&self) -> Option<Instant> {
        *self.expires_at.lock()
    }

    pub fn has_ttl(&self) -> bool {
        self.expires_at.lock().is_some()
    }

    pub fn set_ttl(&self, expires_at: Instant) {
        *self.expires_at.lock() = Some(expires_at);
        self.bump_version()
    }

    pub fn version(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }

    pub fn get(&self) -> RwLockReadGuard<'_, Value> {
        self.value.read()
    }

    pub fn get_mut(&self) -> RwLockWriteGuard<'_, Value> {
        self.value.write()
    }

    pub fn ensure_blob_is_mutable(&self) -> Result<(), Error> {
        self.bump_version();
        let mut val = self.get_mut();
        match *val {
            Value::Blob(ref mut data) => {
                let rw_data = BytesMut::from(&data[..]);
                *val = Value::BlobRw(rw_data);
                Ok(())
            }
            Value::BlobRw(_) => Ok(()),
            _ => Err(Error::WrongType),
        }
    }

    /// If the Entry should be taken as valid, if this function returns FALSE
    /// the callee should behave as if the key was not found. By having this
    /// behaviour we can schedule the purge thread to run every few seconds or
    /// even minutes instead of once every second.
    pub fn is_valid(&self) -> bool {
        self.expires_at.lock().map_or(true, |x| x > Instant::now())
    }

    /// Whether or not the value is scalar
    pub fn is_scalar(&self) -> bool {
        matches!(
            *self.value.read(),
            Value::Boolean(_)
                | Value::Blob(_)
                | Value::BlobRw(_)
                | Value::BigInteger(_)
                | Value::Integer(_)
                | Value::Float(_)
                | Value::String(_)
                | Value::Null
                | Value::Ok
        )
    }

    /// Clone a value. If the value is not clonable an error is Value::Error is
    /// returned instead
    pub fn clone_value(&self) -> Value {
        if self.is_scalar() {
            self.value.read().clone()
        } else {
            Error::WrongType.into()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::Duration;

    #[test]
    fn is_valid_without_expiration() {
        let e = Entry::new(Value::Null, None);
        assert!(e.is_valid());
    }

    #[test]
    fn is_valid() {
        let e = (
            Entry::new(Value::Null, Some(Instant::now() - Duration::from_secs(5))),
            Entry::new(Value::Null, Some(Instant::now())),
            Entry::new(Value::Null, Some(Instant::now() + Duration::from_secs(5))),
        );
        assert!(!e.0.is_valid());
        assert!(!e.1.is_valid());
        assert!(e.2.is_valid());
    }

    #[test]
    fn persist() {
        let e = Entry::new(Value::Null, Some(Instant::now()));
        assert!(!e.is_valid());
        e.persist();
        assert!(e.is_valid());
    }

    #[test]
    fn update_ttl() {
        let e = Entry::new(Value::Null, Some(Instant::now()));
        assert!(!e.is_valid());
        e.persist();
        assert!(e.is_valid());
        e.set_ttl(Instant::now());
        assert!(!e.is_valid());
    }
}
