//! # Microredis: A multi-threaded redis implementation of Redis
//!
//! In-memory database compatible with Redis.
#![deny(missing_docs)]
#![deny(warnings)]

pub mod cmd;
pub mod config;
pub mod connection;
pub mod db;
pub mod dispatcher;
pub mod error;
pub mod macros;
pub mod server;
pub mod value;
