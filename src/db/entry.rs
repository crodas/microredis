use crate::value::Value;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

/// Database Entry
///
/// A database entry is a Value associated with an optional expiration time.
///
/// The database will never return an entry if has expired already, by having
/// this promise we can run the purge process every few seconds instead of doing
/// so more frequently.
impl Entry {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    /// Changes the value that is wrapped in this entry, the TTL (expired_at) is
    /// not affected.
    pub fn change_value(&mut self, value: Value) {
        self.value = value;
    }

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
