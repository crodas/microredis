use crate::value::Value;
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct Entry {
    pub value: Value,
    expires_at: Option<Instant>,
}

/// Database Entry
///
/// A database entry is a Value associated with an optional ttl.
///
/// The database will never return an entry if has expired already, by having
/// this promise we can run the purge process every few seconds instead of doing
/// so more frequently.
impl Entry {
    pub fn new(value: Value, expires_in: Option<Duration>) -> Self {
        Self {
            value,
            expires_at: expires_in.map(|duration| Instant::now() + duration),
        }
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

    pub fn set_ttl(&mut self, expires_in: Duration) {
        self.expires_at = Some(Instant::now() + expires_in);
    }

    /// Changes the value that is wrapped in this entry, the TTL (expired_at) is
    /// not affected.
    pub fn change_value(&mut self, value: Value) {
        self.value = value;
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut Value {
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
}
