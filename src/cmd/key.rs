//! # Key-related command handlers
use super::now;
use crate::{
    check_arg,
    connection::Connection,
    db::{scan::Scan, utils::ExpirationOpts},
    error::Error,
    value::{
        bytes_to_int, bytes_to_number, cursor::Cursor, expiration::Expiration, typ::Typ, Value,
    },
};
use bytes::Bytes;
use std::{
    collections::VecDeque,
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
pub async fn copy(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let source = args.pop_front().ok_or(Error::Syntax)?;
    let destination = args.pop_front().ok_or(Error::Syntax)?;
    let target_db = if !args.is_empty() && check_arg!(args, 0, "DB") {
        let _ = args.pop_front();
        let db = args.pop_front().ok_or(Error::Syntax)?;
        Some(
            conn.all_connections()
                .get_databases()
                .get(bytes_to_int(&db)?)?
                .clone(),
        )
    } else {
        None
    };
    let replace = match args
        .pop_front()
        .map(|m| String::from_utf8_lossy(&m).to_uppercase())
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
        .copy(source, destination, replace.into(), target_db)?
    {
        1
    } else {
        0
    };

    Ok(result.into())
}

/// Removes the specified keys. A key is ignored if it does not exist.
pub async fn del(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let keys = args.into_iter().collect::<Vec<_>>();
    Ok(conn.db().del(&keys))
}

/// Returns if key exists.
pub async fn exists(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let keys = args.into_iter().collect::<Vec<_>>();
    Ok(conn.db().exists(&keys).into())
}

async fn expire_ex(
    command: &[u8],
    is_milliseconds: bool,
    conn: &Connection,
    mut args: VecDeque<Bytes>,
) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let expiration = args.pop_front().ok_or(Error::Syntax)?;

    let expires_at = Expiration::new(&expiration, is_milliseconds, false, command)?;

    if expires_at.is_negative {
        // Delete key right away
        return Ok(conn.db().del(&[key]));
    }

    let opts = args.into_iter().collect::<Vec<_>>();

    conn.db()
        .set_ttl(&key, expires_at.try_into()?, opts.try_into()?)
}

/// Set a timeout on key. After the timeout has expired, the key will
/// automatically be deleted. A key with an associated timeout is often said to
/// be volatile in Redis terminology.
///
/// The timeout will only be cleared by commands that delete or overwrite the
/// contents of the key, including DEL, SET, GETSET and all the *STORE commands.
/// This means that all the operations that conceptually alter the value stored
/// at the key without replacing it with a new one will leave the timeout
/// untouched. For instance, incrementing the value of a key with INCR, pushing
/// a new value into a list with LPUSH, or altering the field value of a hash
/// with HSET are all operations that will leave the timeout untouched.
///
/// The timeout can also be cleared, turning the key back into a persistent key,
/// using the PERSIST command.
///
/// If a key is renamed with RENAME, the associated time to live is transferred
/// to the new key name.
///
/// If a key is overwritten by RENAME, like in the case of an existing key Key_A
/// that is overwritten by a call like RENAME Key_B Key_A, it does not matter if
/// the original Key_A had a timeout associated or not, the new key Key_A will
/// inherit all the characteristics of Key_B.
///
/// Note that calling EXPIRE/PEXPIRE with a non-positive timeout or
/// EXPIREAT/PEXPIREAT with a time in the past will result in the key being
/// deleted rather than expired
pub async fn expire(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    expire_ex(b"EXPIRE", false, conn, args).await
}

/// This command works exactly like EXPIRE but the time to live of the key is
/// specified in milliseconds instead of seconds.
pub async fn pexpire(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    expire_ex(b"PEXPIRE", true, conn, args).await
}

/// Returns the string representation of the type of the value stored at key.
/// The different types that can be returned are: string, list, set, zset, hash
/// and stream.
pub async fn data_type(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().get_data_type(&args[0]).into())
}

/// EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of
/// seconds representing the TTL (time to live), it takes an absolute Unix timestamp (seconds since
/// January 1, 1970). A timestamp in the past will delete the key immediately.
pub async fn expire_at(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let expiration = args.pop_front().ok_or(Error::Syntax)?;
    let expires_at = Expiration::new(&expiration, false, true, b"EXPIREAT")?;

    if expires_at.is_negative {
        // Delete key right away
        return Ok(conn.db().del(&[key]));
    }

    conn.db().set_ttl(
        &key,
        expires_at.try_into()?,
        args.into_iter().collect::<Vec<_>>().try_into()?,
    )
}

/// PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at
/// which the key will expire is specified in milliseconds instead of seconds.
pub async fn pexpire_at(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let expiration = args.pop_front().ok_or(Error::Syntax)?;
    let expires_at = Expiration::new(&expiration, true, true, b"PEXPIREAT")?;

    if expires_at.is_negative {
        // Delete key right away
        return Ok(conn.db().del(&[key]));
    }

    conn.db().set_ttl(
        &key,
        expires_at.try_into()?,
        args.into_iter().collect::<Vec<_>>().try_into()?,
    )
}

/// PEXPIRETIME has the same semantic as EXPIRETIME, but returns the absolute
/// Unix expiration timestamp in milliseconds instead of seconds.
pub async fn p_expire_time(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[0]) {
        Some(Some(ttl)) => {
            // Is there a better way? There should be!
            let secs: i64 = (ttl - Instant::now()).as_millis() as i64;
            secs + 1 + (now().as_millis() as i64)
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

/// Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key
/// will expire.
pub async fn expire_time(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[0]) {
        Some(Some(ttl)) => {
            // Is there a better way? There should be!
            let secs: i64 = (ttl - Instant::now()).as_secs() as i64;
            secs + 1 + (now().as_secs() as i64)
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

/// Returns all keys that matches a given pattern
pub async fn keys(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().get_all_keys(&args[0])?.into())
}

/// Move key from the currently selected database (see SELECT) to the specified
/// destination database. When key already exists in the destination database,
/// or it does not exist in the source database, it does nothing. It is possible
/// to use MOVE as a locking primitive because of this.
pub async fn move_key(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let target_db = args.pop_front().ok_or(Error::Syntax)?;
    let target_db = conn
        .all_connections()
        .get_databases()
        .get(bytes_to_int(&target_db)?)?;

    Ok(if conn.db().move_key(key, target_db)? {
        1.into()
    } else {
        0.into()
    })
}

/// Return information about the object/value stored in the database
pub async fn object(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let subcommand = String::from_utf8_lossy(&args[0]).to_lowercase();

    let expected_args = if subcommand == "help" { 1 } else { 2 };

    if expected_args != args.len() {
        return Err(Error::SubCommandNotFound(
            subcommand.into(),
            String::from_utf8_lossy(&args[0]).into(),
        ));
    }

    match subcommand.as_str() {
        "help" => super::help::object(),
        "refcount" => Ok(if conn.db().exists(&[args[1].clone()]) == 1 {
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

/// Return a random key from the currently selected database.
pub async fn randomkey(conn: &Connection, _: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().randomkey()
}

/// Renames key to newkey. It returns an error when key does not exist. If
/// newkey already exists it is overwritten, when this happens RENAME executes
/// an implicit DEL operation, so if the deleted key contains a very big value
/// it may cause high latency even if RENAME itself is usually a constant-time
/// operation.
pub async fn rename(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    if conn.db().rename(&args[0], &args[1], true.into())? {
        Ok(Value::Ok)
    } else {
        Ok(0.into())
    }
}

/// Renames key to newkey if newkey does not yet exist. It returns an error when
/// key does not exist.
pub async fn renamenx(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    if conn.db().rename(&args[0], &args[1], false.into())? {
        Ok(1.into())
    } else {
        Ok(0.into())
    }
}

/// SCAN is a cursor based iterator. This means that at every call of the
/// command, the server returns an updated cursor that the user needs to use as
/// the cursor argument in the next call.
pub async fn scan(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let cursor = args.pop_front().ok_or(Error::Syntax)?;
    let cursor: Cursor = (&cursor).try_into()?;
    let mut pattern = None;
    let mut count = None;
    let mut typ = None;

    loop {
        let key = if let Some(key) = args.pop_front() {
            key
        } else {
            break;
        };
        let value = args.pop_front().ok_or(Error::Syntax)?;
        match String::from_utf8_lossy(&key).to_uppercase().as_str() {
            "MATCH" => pattern = Some(value),
            "COUNT" => {
                count = Some(
                    bytes_to_number(&value)
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
pub async fn ttl(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[0]) {
        Some(Some(ttl)) => {
            let ttl = ttl - Instant::now();
            ttl.as_secs() as i64 + 1
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

/// Like TTL this command returns the remaining time to live of a key that has
/// an expire set, with the sole difference that TTL returns the amount of
/// remaining time in seconds while PTTL returns it in milliseconds.
pub async fn pttl(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let ttl = match conn.db().ttl(&args[0]) {
        Some(Some(ttl)) => {
            let ttl = ttl - Instant::now();
            ttl.as_millis() as i64
        }
        Some(None) => -1,
        None => -2,
    };

    Ok(ttl.into())
}

/// Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to
/// persistent (a key that will never expire as no timeout is associated).
pub async fn persist(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().persist(&args[0]))
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
    async fn expire2() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["incr", "foo"]).await
        );
        assert_eq!(
            Err(Error::InvalidExpire("pexpire".to_owned())),
            run_command(&c, &["pexpire", "foo", "9223372036854770000"]).await
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
    async fn scan_with_type_1() {
        let c = create_connection();
        for i in (1..100) {
            assert_eq!(
                Ok(1.into()),
                run_command(&c, &["incr", &format!("foo-{}", i)]).await
            );
        }

        let r: Vec<Value> = run_command(&c, &["scan", "0", "type", "hash"])
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let values: Vec<Value> = r[1].clone().try_into().unwrap();

        assert_eq!(2, r.len());
        assert_eq!(0, values.len());
    }

    #[tokio::test]
    async fn scan_with_type_2() {
        let c = create_connection();
        for i in (1..100) {
            assert_eq!(
                Ok(1.into()),
                run_command(&c, &["incr", &format!("foo-{}", i)]).await
            );
        }

        let r: Vec<Value> = run_command(&c, &["scan", "0", "type", "!hash"])
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let values: Vec<Value> = r[1].clone().try_into().unwrap();

        assert_eq!(2, r.len());
        assert_eq!(10, values.len());
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
