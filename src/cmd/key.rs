//! # Key-related command handlers
use super::now;
use crate::{
    check_arg,
    connection::Connection,
    db::{scan::Scan, utils::ExpirationOpts},
    error::Error,
    value::{bytes_to_int, bytes_to_number, cursor::Cursor, typ::Typ, Value},
};
use bytes::Bytes;
use std::{
    convert::TryInto,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, Instant};

/// This command copies the value stored at the source key to the destination
/// key.
///
/// By default, the destination key is created in the logical database used by
/// the connection. The DB option allows specifying an alternative logical
/// database index for the destination key.
///
/// The command returns an error when the destination key already exists. The
/// REPLACE option removes the destination key before copying the value to it.
pub async fn copy(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let mut skip = 3;
    let target_db = if args.len() > 4 && check_arg!(args, 3, "DB") {
        skip += 2;
        Some(
            conn.all_connections()
                .get_databases()
                .get(bytes_to_number(&args[4])?)?
                .clone(),
        )
    } else {
        None
    };
    let replace = match args
        .get(skip)
        .map(|m| String::from_utf8_lossy(m).to_uppercase())
    {
        Some(value) => {
            if value == "REPLACE" {
                true
            } else {
                return Err(Error::Syntax);
            }
        }
        None => false,
    };
    let result = if conn
        .db()
        .copy(&args[1], &args[2], replace.into(), target_db)?
    {
        1
    } else {
        0
    };

    Ok(result.into())
}

/// Removes the specified keys. A key is ignored if it does not exist.
pub async fn del(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().del(&args[1..]))
}

/// Returns if key exists.
pub async fn exists(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().exists(&args[1..]).into())
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
    let expires_in: i64 = bytes_to_int(&args[2])?;

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

    conn.db()
        .set_ttl(&args[1], expires_at, (&args[3..]).try_into()?)
}

/// Returns the string representation of the type of the value stored at key.
/// The different types that can be returned are: string, list, set, zset, hash
/// and stream.
pub async fn data_type(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get_data_type(&args[1]).into())
}

/// EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of
/// seconds representing the TTL (time to live), it takes an absolute Unix timestamp (seconds since
/// January 1, 1970). A timestamp in the past will delete the key immediately.
pub async fn expire_at(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let secs = check_arg!(args, 0, "EXPIREAT");
    let expires_at: i64 = bytes_to_int(&args[2])?;
    let expires_in: i64 = if secs {
        expires_at
            .checked_sub(now().as_secs() as i64)
            .unwrap_or_default()
    } else {
        expires_at
            .checked_sub(now().as_millis() as i64)
            .unwrap_or_default()
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

    conn.db()
        .set_ttl(&args[1], expires_at, (&args[3..]).try_into()?)
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

/// Move key from the currently selected database (see SELECT) to the specified
/// destination database. When key already exists in the destination database,
/// or it does not exist in the source database, it does nothing. It is possible
/// to use MOVE as a locking primitive because of this.
pub async fn move_key(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let target_db = conn
        .all_connections()
        .get_databases()
        .get(bytes_to_number(&args[2])?)?;

    Ok(if conn.db().move_key(&args[1], target_db)? {
        1.into()
    } else {
        0.into()
    })
}

/// Return information about the object/value stored in the database
pub async fn object(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let subcommand = String::from_utf8_lossy(&args[1]).to_lowercase();

    let expected_args = if subcommand == "help" { 2 } else { 3 };

    if expected_args != args.len() {
        return Err(Error::SubCommandNotFound(
            subcommand.into(),
            String::from_utf8_lossy(&args[0]).into(),
        ));
    }

    match subcommand.as_str() {
        "help" => super::help::object(),
        "refcount" => Ok(if conn.db().exists(&[args[2].clone()]) == 1 {
            1.into()
        } else {
            Value::Null
        }),
        _ => Err(Error::SubCommandNotFound(
            subcommand.into(),
            String::from_utf8_lossy(&args[0]).into(),
        )),
    }
}

/// Renames key to newkey. It returns an error when key does not exist. If
/// newkey already exists it is overwritten, when this happens RENAME executes
/// an implicit DEL operation, so if the deleted key contains a very big value
/// it may cause high latency even if RENAME itself is usually a constant-time
/// operation.
pub async fn rename(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_rename = check_arg!(args, 0, "RENAME");
    if conn.db().rename(&args[1], &args[2], is_rename.into())? {
        Ok(if is_rename { Value::Ok } else { 1.into() })
    } else {
        Ok(0.into())
    }
}

/// SCAN is a cursor based iterator. This means that at every call of the
/// command, the server returns an updated cursor that the user needs to use as
/// the cursor argument in the next call.
pub async fn scan(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let cursor: Cursor = (&args[1]).try_into()?;
    let mut current = 2;
    let mut pattern = None;
    let mut count = None;
    let mut typ = None;

    for i in (2..args.len()).step_by(2) {
        let value = args
            .get(i + 1)
            .ok_or(Error::InvalidArgsCount("SCAN".to_owned()))?;
        match String::from_utf8_lossy(&args[i]).to_uppercase().as_str() {
            "MATCH" => pattern = Some(value),
            "COUNT" => {
                count = Some(
                    bytes_to_number(value)
                        .map_err(|_| Error::InvalidArgsCount("SCAN".to_owned()))?,
                )
            }
            "TYPE" => {
                typ = Some(
                    Typ::from_str(&String::from_utf8_lossy(&value)).map_err(|_| Error::Syntax)?,
                )
            }
            _ => return Err(Error::Syntax),
        }
    }

    Ok(conn.db().scan(cursor, pattern, count, typ)?.into())
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
    use std::convert::TryInto;

    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
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
    async fn _type() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["incr", "foo"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hset", "hash", "foo", "bar"]).await
        );
        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["sadd", "set", "foo", "bar"]).await
        );
        assert_eq!(Ok("set".into()), run_command(&c, &["type", "set"]).await);
        assert_eq!(Ok("hash".into()), run_command(&c, &["type", "hash"]).await);
        assert_eq!(Ok("string".into()), run_command(&c, &["type", "foo"]).await);
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

    #[tokio::test]
    async fn copy() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(Ok(1.into()), run_command(&c, &["copy", "foo", "bar"]).await);
        assert_eq!(Ok(2.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(Ok(0.into()), run_command(&c, &["copy", "foo", "bar"]).await);
        assert_eq!(
            Ok(Value::Array(vec!["2".into(), "1".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["copy", "foo", "bar", "replace"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec!["2".into(), "2".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn copy_different_db() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["copy", "foo", "bar", "db", "2"]).await
        );
        assert_eq!(Ok(2.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["copy", "foo", "bar", "db", "2"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec!["2".into(), Value::Null])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "2"]).await);
        assert_eq!(
            Ok(Value::Array(vec![Value::Null, "1".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "0"]).await);
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["copy", "foo", "bar", "db", "2", "replace"]).await
        );
        assert_eq!(
            Ok(Value::Array(vec!["2".into(), Value::Null])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "2"]).await);
        assert_eq!(
            Ok(Value::Array(vec![Value::Null, "2".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn copy_same_db() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Err(Error::SameEntry),
            run_command(&c, &["copy", "foo", "foo"]).await
        );
        assert_eq!(
            Err(Error::SameEntry),
            run_command(&c, &["copy", "foo", "foo", "db", "0"]).await
        );
    }

    #[tokio::test]
    async fn _move() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(Ok(1.into()), run_command(&c, &["move", "foo", "2"]).await);
        assert_eq!(Ok(Value::Null), run_command(&c, &["get", "foo"]).await);
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(Ok(0.into()), run_command(&c, &["move", "foo", "2"]).await);
        assert_eq!(Ok(Value::Ok), run_command(&c, &["select", "2"]).await);
        assert_eq!(Ok("1".into()), run_command(&c, &["get", "foo"]).await);
    }

    #[tokio::test]
    async fn _move_same_db() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Err(Error::SameEntry),
            run_command(&c, &["move", "foo", "0"]).await
        );
    }

    #[tokio::test]
    async fn rename() {
        let c = create_connection();
        assert_eq!(Ok(1.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["rename", "foo", "bar-1650"]).await
        );
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["rename", "bar-1650", "xxx"]).await
        );
        assert_eq!(
            Err(Error::NotFound),
            run_command(&c, &["rename", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn renamenx() {
        let c = create_connection();
        assert_eq!(Ok(1i64.into()), run_command(&c, &["incr", "foo"]).await);
        assert_eq!(
            Ok(1i64.into()),
            run_command(&c, &["renamenx", "foo", "bar-1650"]).await
        );
        assert_eq!(
            Ok(1i64.into()),
            run_command(&c, &["renamenx", "bar-1650", "xxx"]).await
        );
        assert_eq!(
            Err(Error::NotFound),
            run_command(&c, &["renamenx", "foo", "bar"]).await
        );

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "bar-1650", "xxx"]).await
        );
        assert_eq!(
            Ok(0i64.into()),
            run_command(&c, &["renamenx", "xxx", "bar-1650"]).await
        );
        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["get", "xxx"]).await
        );
    }

    #[tokio::test]
    async fn scan_no_args() {
        let c = create_connection();
        for i in (1..100) {
            assert_eq!(
                Ok(1.into()),
                run_command(&c, &["incr", &format!("foo-{}", i)]).await
            );
        }

        let r: Vec<Value> = run_command(&c, &["scan", "0"])
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let values: Vec<Value> = r[1].clone().try_into().unwrap();

        assert_eq!(2, r.len());
        assert_eq!(10, values.len());
    }

    #[tokio::test]
    async fn scan_with_count_match() {
        let c = create_connection();
        for i in (1..100) {
            assert_eq!(
                Ok(1.into()),
                run_command(&c, &["incr", &format!("foo-{}", i)]).await
            );
        }

        let r: Vec<Value> = run_command(&c, &["scan", "0", "match", "foo-1*", "count", "50"])
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let values: Vec<Value> = r[1].clone().try_into().unwrap();

        assert_eq!(2, r.len());
        assert_eq!(11, values.len());
    }

    #[tokio::test]
    async fn scan_with_count() {
        let c = create_connection();
        for i in (1..100) {
            assert_eq!(
                Ok(1.into()),
                run_command(&c, &["incr", &format!("foo-{}", i)]).await
            );
        }

        let r: Vec<Value> = run_command(&c, &["scan", "0", "count", "50"])
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let values: Vec<Value> = r[1].clone().try_into().unwrap();

        assert_eq!(2, r.len());
        assert_eq!(50, values.len());
    }
}
