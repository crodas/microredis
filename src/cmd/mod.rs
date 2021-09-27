pub mod client;
pub mod hash;
pub mod key;
pub mod string;

#[cfg(test)]
mod test {
    use crate::{
        connection::{Connection, Connections},
        db::Db,
        dispatcher::Dispatcher,
        error::Error,
        value::Value,
    };
    use bytes::Bytes;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        ops::Deref,
        sync::Arc,
    };

    pub fn create_connection() -> Arc<Connection> {
        let all_connections = Arc::new(Connections::new());
        let db = Arc::new(Db::new(1000));

        let client = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        all_connections.new_connection(db.clone(), client)
    }

    pub fn run_command(conn: &Connection, cmd: &[&str]) -> Result<Value, Error> {
        let args: Vec<Bytes> = cmd.iter().map(|s| Bytes::from(s.to_string())).collect();

        let handler = Dispatcher::new(&args)?;

        handler.deref().execute(&conn, &args)
    }
}
