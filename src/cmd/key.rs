use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant};

pub fn now() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

pub fn del(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().del(&args[1..]))
}

pub fn exists(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().exists(&args[1..]))
}

pub fn expire(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let expires_in: i64 = bytes_to_number(&args[2])?;

    if expires_in <= 0 {
        // Delete key right away
        return Ok(conn.db().del(&args[1..2]));
    }

    let expires_at = if check_arg!(args, 0, "EXPIRES") {
        Duration::from_secs(expires_in as u64)
    } else {
        Duration::from_millis(expires_in as u64)
    };

    Ok(conn.db().set_ttl(&args[1], expires_at))
}

pub fn expire_at(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let secs = check_arg!(args, 0, "EXPIREAT");
    let expires_at: i64 = bytes_to_number(&args[2])?;
    let expires_in: i64 = if secs {
        expires_at - now().as_secs() as i64
    } else {
        expires_at - now().as_millis() as i64
    };

    if expires_in <= 0 {
        // Delete key right away
        return Ok(conn.db().del(&args[1..2]));
    }

    let expires_at = if secs {
        Duration::from_secs(expires_in as u64)
    } else {
        Duration::from_millis(expires_in as u64)
    };

    Ok(conn.db().set_ttl(&args[1], expires_at))
}

pub fn ttl(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[1]) {
        Some(Some(ttl)) => {
            let ttl = ttl - Instant::now();
            if check_arg!(args, 0, "TTL") {
                ttl.as_secs() as i64
            } else {
                ttl.as_millis() as i64
            }
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

pub fn expire_time(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[1]) {
        Some(Some(ttl)) => {
            // Is there a better way? There should be!
            if check_arg!(args, 0, "EXPIRETIME") {
                let secs: i64 = (ttl - Instant::now()).as_secs() as i64;
                secs + (now().as_secs() as i64)
            } else {
                let secs: i64 = (ttl - Instant::now()).as_millis() as i64;
                secs + (now().as_millis() as i64)
            }
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

pub fn persist(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().persist(&args[1]))
}
