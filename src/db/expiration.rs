use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use tokio::time::Instant;

/// ExpirationId
///
/// The internal data structure is a B-Tree and the key is the expiration time,
/// all data are naturally sorted by expiration time. Because it is possible
/// that different keys expire at the same instant, an internal counter is added
/// to the ID to make each ID unique (and sorted by Expiration Time +
/// Incremental
/// counter).
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct ExpirationId(pub (Instant, u64));

#[derive(Debug)]
pub struct ExpirationDb {
    /// B-Tree Map of expiring keys
    expiring_keys: BTreeMap<ExpirationId, Bytes>,
    /// Hash which contains the keys and their ExpirationId.
    keys: HashMap<Bytes, ExpirationId>,
    next_id: u64,
}

impl ExpirationDb {
    pub fn new() -> Self {
        Self {
            expiring_keys: BTreeMap::new(),
            keys: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn add(&mut self, key: &Bytes, expires_at: Instant) {
        let entry_id = ExpirationId((expires_at, self.next_id));

        if let Some(prev) = self.keys.remove(key) {
            // Another key with expiration is already known, it has
            // to be removed before adding a new one
            self.expiring_keys.remove(&prev);
        }

        self.expiring_keys.insert(entry_id, key.clone());
        self.keys.insert(key.clone(), entry_id);

        self.next_id += 1;
    }

    pub fn remove(&mut self, key: &Bytes) -> bool {
        if let Some(prev) = self.keys.remove(key) {
            // Another key with expiration is already known, it has
            // to be removed before adding a new one
            self.expiring_keys.remove(&prev);
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.expiring_keys.len()
    }

    /// Returns a list of expired keys, these keys are removed from the internal
    /// data structure which is keeping track of expiring keys.
    pub fn get_expired_keys(&mut self, now: Option<Instant>) -> Vec<Bytes> {
        let now = now.unwrap_or_else(Instant::now);

        let mut expiring_keys = vec![];

        for (key, value) in self.expiring_keys.iter_mut() {
            if key.0 .0 > now {
                break;
            }

            expiring_keys.push((*key, value.clone()));
            self.keys.remove(value);
        }

        expiring_keys
            .iter()
            .map(|(k, v)| {
                self.expiring_keys.remove(k);
                v.to_owned()
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bytes;
    use tokio::time::{Duration, Instant};

    #[test]
    fn two_entires_same_expiration() {
        let mut db = ExpirationDb::new();
        let key1 = bytes!(b"key");
        let key2 = bytes!(b"bar");
        let key3 = bytes!(b"xxx");
        let expiration = Instant::now() + Duration::from_secs(5);

        db.add(&key1, expiration);
        db.add(&key2, expiration);
        db.add(&key3, expiration);

        assert_eq!(3, db.len());
    }

    #[test]
    fn remove_prev_expiration() {
        let mut db = ExpirationDb::new();
        let key1 = bytes!(b"key");
        let key2 = bytes!(b"bar");
        let expiration = Instant::now() + Duration::from_secs(5);

        db.add(&key1, expiration);
        db.add(&key2, expiration);
        db.add(&key1, expiration);

        assert_eq!(2, db.len());
    }

    #[test]
    fn get_expiration() {
        let mut db = ExpirationDb::new();
        let keys = vec![
            (bytes!(b"hix"), Instant::now() + Duration::from_secs(15)),
            (bytes!(b"key"), Instant::now() + Duration::from_secs(2)),
            (bytes!(b"bar"), Instant::now() + Duration::from_secs(3)),
            (bytes!(b"hi"), Instant::now() + Duration::from_secs(3)),
        ];

        keys.iter()
            .map(|v| {
                db.add(&v.0, v.1);
            })
            .for_each(drop);

        assert_eq!(db.len(), keys.len());

        assert_eq!(0, db.get_expired_keys(Some(Instant::now())).len());
        assert_eq!(db.len(), keys.len());

        assert_eq!(
            vec![keys[1].0.clone()],
            db.get_expired_keys(Some(Instant::now() + Duration::from_secs(2)))
        );
        assert_eq!(3, db.len());

        assert_eq!(
            vec![keys[2].0.clone(), keys[3].0.clone()],
            db.get_expired_keys(Some(Instant::now() + Duration::from_secs(4)))
        );
        assert_eq!(1, db.len());
    }

    #[test]
    pub fn remove() {
        let mut db = ExpirationDb::new();
        let keys = vec![
            (bytes!(b"hix"), Instant::now() + Duration::from_secs(15)),
            (bytes!(b"key"), Instant::now() + Duration::from_secs(2)),
            (bytes!(b"bar"), Instant::now() + Duration::from_secs(3)),
            (bytes!(b"hi"), Instant::now() + Duration::from_secs(3)),
        ];

        keys.iter()
            .map(|v| {
                db.add(&v.0, v.1);
            })
            .for_each(drop);

        assert_eq!(keys.len(), db.len());

        assert!(db.remove(&keys[0].0));
        assert!(!db.remove(&keys[0].0));

        assert_eq!(keys.len() - 1, db.len());
    }
}
