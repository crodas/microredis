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
        collections::VecDeque,
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

    pub async fn invalid_type(cmd: &[&str]) {
        let c = create_connection();
        let _ = run_command(&c, &["set", "key", "test"]).await;
        assert_eq!(Err(Error::WrongType), run_command(&c, cmd).await);
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
        let args: VecDeque<Bytes> = cmd.iter().map(|s| Bytes::from(s.to_string())).collect();

        let dispatcher = Dispatcher::new();
        dispatcher.execute(conn, args).await
    }

    #[tokio::test]
    async fn total_connections() {
        let c = create_connection();
        let all_connections = c.all_connections();
        assert_eq!(1, all_connections.total_connections());
        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let c2 = c.all_connections().new_connection(c.db(), client).1;
        c2.block();
        assert_eq!(2, all_connections.total_connections());
        assert_eq!(1, all_connections.total_blocked_connections());
        c2.destroy();
        assert_eq!(1, all_connections.total_connections());
        assert_eq!(0, all_connections.total_blocked_connections());
    }
}
