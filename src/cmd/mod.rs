//! # All commands handlers
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant};

pub mod client;
pub mod hash;
pub mod help;
pub mod key;
pub mod list;
pub mod metrics;
pub mod pubsub;
pub mod server;
pub mod set;
pub mod string;
pub mod transaction;

/// Returns the current time
pub fn now() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

#[cfg(test)]
mod test {
    use crate::{
        connection::{connections::Connections, Connection},
        db::pool::Databases,
        dispatcher::Dispatcher,
        error::Error,
        value::Value,
    };
    use bytes::Bytes;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };
    use tokio::sync::mpsc::Receiver;

    pub fn create_connection() -> Arc<Connection> {
        let (default_db, all_dbs) = Databases::new(16, 1000);
        let all_connections = Arc::new(Connections::new(all_dbs));

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(default_db, client).1
    }

    pub fn create_connection_and_pubsub() -> (Receiver<Value>, Arc<Connection>) {
        let (default_db, all_dbs) = Databases::new(16, 1000);
        let all_connections = Arc::new(Connections::new(all_dbs));

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(default_db, client)
    }

    pub fn create_new_connection_from_connection(
        conn: &Connection,
    ) -> (Receiver<Value>, Arc<Connection>) {
        let all_connections = conn.all_connections();

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(
            all_connections.get_databases().get(0).expect("DB(0)"),
            client,
        )
    }

    pub async fn run_command(conn: &Connection, cmd: &[&str]) -> Result<Value, Error> {
        let args: Vec<Bytes> = cmd.iter().map(|s| Bytes::from(s.to_string())).collect();

        let dispatcher = Dispatcher::new();
        dispatcher.execute(conn, &args).await
    }
}
