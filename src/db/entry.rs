use crate::{error::Error, value::Value};
use std::time::SystemTime;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Entry {
    pub value: Value,
    pub version: u128,
    expires_at: Option<Instant>,
}

pub fn new_version() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("get millis error")
        .as_nanos()
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
            value,
            expires_at,
            version: new_version(),
        }
    }

    pub fn bump_version(&mut self) {
        self.version = new_version();
    }

    pub fn persist(&mut self) {
        self.expires_at = None;
    }

    pub fn get_ttl(&self) -> Option<Instant> {
        self.expires_at
    }

    pub fn has_ttl(&self) -> bool {
        self.expires_at.is_some()
    }

    pub fn set_ttl(&mut self, expires_at: Instant) {
        self.expires_at = Some(expires_at);
        self.version = new_version();
    }

    pub fn version(&self) -> u128 {
        self.version
    }

    /// Changes the value that is wrapped in this entry, the TTL (expired_at) is
    /// not affected.
    pub fn change_value(&mut self, value: Value) {
        self.value = value;
        self.version = new_version();
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut Value {
        self.version = new_version();
        &mut self.value
    }

    pub fn get(&self) -> &Value {
        &self.value
    }

    /// If the Entry should be taken as valid, if this function returns FALSE
    /// the callee should behave as if the key was not found. By having this
    /// behaviour we can schedule the purge thread to run every few seconds or
    /// even minutes instead of once every second.
    pub fn is_valid(&self) -> bool {
        self.expires_at.map_or(true, |x| x > Instant::now())
    }

    /// Whether or not the value is clonable. Special types like hashes should
    /// not be clonable because those types cannot be returned to the user with
    /// the `get` command.
    pub fn is_clonable(&self) -> bool {
        matches!(
            &self.value,
            Value::Boolean(_)
                | Value::Blob(_)
                | Value::BigInteger(_)
                | Value::Integer(_)
                | Value::Float(_)
                | Value::String(_)
                | Value::Null
                | Value::OK
        )
    }

    pub fn clone_value(&self) -> Value {
        if self.is_clonable() {
            self.value.clone()
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
        let mut e = Entry::new(Value::Null, Some(Instant::now()));
        assert!(!e.is_valid());
        e.persist();
        assert!(e.is_valid());
    }

    #[test]
    fn update_ttl() {
        let mut e = Entry::new(Value::Null, Some(Instant::now()));
        assert!(!e.is_valid());
        e.persist();
        assert!(e.is_valid());
        e.set_ttl(Instant::now());
        assert!(!e.is_valid());
    }
}
