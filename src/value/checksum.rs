use crate::value;
use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use std::hash::{Hash, Hasher};

fn calculate_checksum(bytes: &Bytes) -> Option<u32> {
        if bytes.len() < 1024 {
            None
        } else {
            let mut hasher = Crc32Hasher::new();
            hasher.update(bytes);
            Some(hasher.finalize())
        }
}

pub struct Ref<'a> {
    bytes: &'a Bytes,
    checksum: Option<u32>,
}

impl<'a> Ref<'a> {
    pub fn new(bytes: &'a Bytes) -> Self {
        let checksum = calculate_checksum(bytes);
        Self { bytes, checksum }
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    bytes: Bytes,
    checksum: Option<u32>,
}

impl Value {
    pub fn new(bytes: Bytes) -> Self {
        let checksum = calculate_checksum(&bytes);
        Self { bytes, checksum }
    }

    pub fn clone_value(&self) -> value::Value {
        value::Value::Blob(self.bytes.clone())
    }

    pub fn has_checksum(&self) -> bool {
        self.checksum.is_some()
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        if self.checksum == other.checksum && self.bytes.len() == other.bytes.len() {
            // The data have the same checksum now perform a more extensive
            // comparision
            return self.bytes.eq(&other.bytes);
        }

        false
    }
}

impl Eq for Value {}

impl<'a> PartialEq<Ref<'a>> for Value {
    fn eq(&self, other: &Ref) -> bool {
        if self.checksum == other.checksum && self.bytes.len() == other.bytes.len() {
            // The data have the same checksum now perform a more extensive
            // comparision
            return self.bytes.eq(&other.bytes);
        }

        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bytes;

    #[test]
    fn does_not_have_checksum() {
        let data = Value::new(bytes!(b"one"));
        assert!(!data.has_checksum())
    }

    #[test]
    fn has_checksum() {
        let data = Value::new(bytes!(
            b"
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
        "
        ));
        assert!(data.has_checksum())
    }

    #[test]
    fn compare() {
        let data1 = Value::new(bytes!(
            b"
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
            one one one one one one one one one one one one one one one one one one
        "
        ));
        assert!(data1.clone() == data1.clone());

        let data2 = Value::new(bytes!(b"one"));
        assert!(data2 == data2.clone());
        assert!(data1 != data2);
    }
}
