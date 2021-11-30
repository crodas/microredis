use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

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
    pub fn new(obj: T) -> Self {
        Self(RwLock::new(obj))
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

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
