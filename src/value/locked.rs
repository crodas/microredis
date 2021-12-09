//! # Locked Value
//!
//! Wraps any a structure and makes it read-write lockable. This is a very simple abstraction on
//! top of a RwLock.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Locked Value
///
/// This is a very simple data structure to wrap a value behind a read/write lock.
///
/// The wrap objects are comparable and clonable.
#[derive(Debug)]
pub struct Value<T: Clone + PartialEq>(pub RwLock<T>);

impl<T: Clone + PartialEq> Clone for Value<T> {
    fn clone(&self) -> Self {
        Self(RwLock::new(self.0.read().clone()))
    }
}

impl<T: PartialEq + Clone> PartialEq for Value<T> {
    fn eq(&self, other: &Value<T>) -> bool {
        self.0.read().eq(&other.0.read())
    }
}

impl<T: PartialEq + Clone> Value<T> {
    /// Creates a new instance
    pub fn new(obj: T) -> Self {
        Self(RwLock::new(obj))
    }

    /// Acquire a write lock and return the wrapped Value
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    /// Acquire a read lock and return the wrapped Value
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn locked_eq1() {
        let a = Value::new(1);
        let b = Value::new(1);
        assert!(a == b);
    }

    #[test]
    fn locked_eq2() {
        let a = Value::new(1);
        let b = Value::new(2);
        assert!(a != b);
    }

    #[test]
    fn locked_clone() {
        let a = Value::new((1, 2, 3));
        assert!(a == a.clone());
    }
}
