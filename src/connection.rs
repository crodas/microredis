use crate::db::Db;
use std::sync::Arc;

pub struct Connection {
    db: Arc<Db>,
    current_db: i32,
}

impl Connection {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db, current_db: 0 }
    }

    pub fn db(&self) -> &Db {
        &self.db
    }
}
