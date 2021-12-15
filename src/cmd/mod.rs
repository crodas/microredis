//! # All commands handlers
pub mod client;
pub mod hash;
pub mod key;
pub mod list;
pub mod metrics;
pub mod pubsub;
pub mod set;
pub mod string;
pub mod transaction;

#[cfg(test)]
mod test {
    use crate::{
        connection::{connections::Connections, Connection},
        db::Db,
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
        let db = Arc::new(Db::new(1000));
        let all_connections = Arc::new(Connections::new(db.clone()));

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(db.clone(), client).1
    }

    pub fn create_connection_and_pubsub() -> (Receiver<Value>, Arc<Connection>) {
        let db = Arc::new(Db::new(1000));
        let all_connections = Arc::new(Connections::new(db.clone()));

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(db.clone(), client)
    }

    pub fn create_new_connection_from_connection(
        conn: &Connection,
    ) -> (Receiver<Value>, Arc<Connection>) {
        let all_connections = conn.all_connections();

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(all_connections.db(), client)
    }

    pub async fn run_command(conn: &Connection, cmd: &[&str]) -> Result<Value, Error> {
        let args: Vec<Bytes> = cmd.iter().map(|s| Bytes::from(s.to_string())).collect();

        let dispatcher = Dispatcher::new();
        let handler = dispatcher.get_handler(&args)?;

        handler.execute(conn, &args).await
    }
}
