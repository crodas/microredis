use crate::{connection::Connection, error::Error, value::bytes_to_number, value::Value};
use bytes::Bytes;
use tokio::time::{Duration, Instant};

pub fn del(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().del(&args[1..]))
}

pub fn expire(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let expires_at: i64 = bytes_to_number(&args[2])?;

    if expires_at <= 0 {
        return Ok(conn.db().del(&args[1..2]));
    }

    let expires_at = Duration::new(expires_at as u64, 0);

    Ok(conn.db().expire(&args[1], expires_at))
}

pub fn persist(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().persist(&args[1]))
}

pub fn ttl(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[1]) {
        Some(Some(ttl)) => (ttl - Instant::now()).as_secs() as i64,
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}
