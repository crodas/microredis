//! # pool of databases
//!
//! Redis and microredis support multiple databases.
//!
//! Each database is completely independent from each other. There are a few
//! commands that allows multiple databases to interact with each other (to move
//! or copy entires atomically).
//!
//! This struct will hold an Arc for each database to share databases between
//! connections.
use super::Db;
use crate::error::Error;
use std::sync::Arc;

/// Databases
#[derive(Debug)]
pub struct Databases {
    databases: Vec<Arc<Db>>,
}

impl Databases {
    /// Creates new pool of databases.
    ///
    /// The default database is returned along side the pool
    pub fn new(databases: usize, number_of_slots: usize) -> (Arc<Db>, Arc<Self>) {
        let databases = (0..databases)
            .map(|_| Arc::new(Db::new(number_of_slots)))
            .collect::<Vec<Arc<Db>>>();

        (databases[0].clone(), Arc::new(Self { databases }))
    }

    /// Returns a single database or None
    pub fn get(&self, db: usize) -> Result<Arc<Db>, Error> {
        self.databases
            .get(db)
            .cloned()
            .ok_or(Error::NotSuchDatabase)
    }
}

/// Database iterator
pub struct DatabasesIterator<'a> {
    databases: &'a Databases,
    index: usize,
}

impl<'a> IntoIterator for &'a Databases {
    type Item = Arc<Db>;
    type IntoIter = DatabasesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DatabasesIterator {
            databases: self,
            index: 0,
        }
    }
}

impl<'a> Iterator for DatabasesIterator<'a> {
    type Item = Arc<Db>;
    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.databases.get(self.index - 1).ok()
    }
}
