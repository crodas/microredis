use crate::value::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use tokio::time::Instant;

pub struct Db {
    entries: Arc<RwLock<HashMap<String, Value>>>,
    expiration: Arc<RwLock<BTreeMap<(Instant, u64), String>>>,
}
