//! # Key-related command handlers
use super::now;
use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, Instant};

/// Removes the specified keys. A key is ignored if it does not exist.
pub async fn del(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().del(&args[1..]))
}

/// Returns if key exists.
pub async fn exists(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().exists(&args[1..]))
}

/// Set a timeout on key. After the timeout has expired, the key will automatically be deleted. A
/// key with an associated timeout is often said to be volatile in Redis terminology.
///
/// The timeout will only be cleared by commands that delete or overwrite the contents of the key,
/// including DEL, SET, GETSET and all the *STORE commands. This means that all the operations that
/// conceptually alter the value stored at the key without replacing it with a new one will leave
/// the timeout untouched. For instance, incrementing the value of a key with INCR, pushing a new
/// value into a list with LPUSH, or altering the field value of a hash with HSET are all
/// operations that will leave the timeout untouched.
///
/// The timeout can also be cleared, turning the key back into a persistent key, using the PERSIST
/// command.
pub async fn expire(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let expires_in: i64 = bytes_to_number(&args[2])?;

    if expires_in <= 0 {
        // Delete key right away
        return Ok(conn.db().del(&args[1..2]));
    }

    let expires_in: u64 = expires_in as u64;

    let expires_at = if check_arg!(args, 0, "EXPIRE") {
        Duration::from_secs(expires_in)
    } else {
        Duration::from_millis(expires_in)
    };

    Ok(conn.db().set_ttl(&args[1], expires_at))
}

/// EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of
/// seconds representing the TTL (time to live), it takes an absolute Unix timestamp (seconds since
/// January 1, 1970). A timestamp in the past will delete the key immediately.
pub async fn expire_at(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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

    let expires_in: u64 = expires_in as u64;

    let expires_at = if secs {
        Duration::from_secs(expires_in)
    } else {
        Duration::from_millis(expires_in)
    };

    Ok(conn.db().set_ttl(&args[1], expires_at))
}

/// Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key
/// will expire.
pub async fn expire_time(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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

/// Returns all keys that matches a given pattern
pub async fn keys(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get_all_keys(&args[1])?.into())
}

/// Returns the remaining time to live of a key that has a timeout. This introspection capability
/// allows a Redis client to check how many seconds a given key will continue to be part of the
/// dataset.
pub async fn ttl(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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

/// Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to
/// persistent (a key that will never expire as no timeout is associated).
pub async fn persist(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().persist(&args[1]))
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        value::Value,
    };

    #[tokio::test]
    async fn del() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["incr", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["exists", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["del", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["del", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["exists", "foo"]).await
        );
    }

    #[tokio::test]
    async fn expire_and_persist() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["incr", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["pexpire", "foo", "6000"]).await
        );

        match run_command(&c, &["pttl", "foo"]).await {
            Ok(Value::Integer(n)) => {
                assert!(n < 6000 && n > 5900);
            }
            _ => unreachable!(),
        };

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["persist", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(-1)),
            run_command(&c, &["pttl", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["del", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(-2)),
            run_command(&c, &["pttl", "foo"]).await
        );
    }
}
