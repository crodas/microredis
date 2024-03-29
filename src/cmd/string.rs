//! # String command handlers

use crate::{
    check_arg,
    connection::Connection,
    db::utils::Override,
    error::Error,
    value::{bytes_to_number, expiration::Expiration, float::Float, Value},
};
use bytes::Bytes;
use std::{
    cmp::min,
    collections::VecDeque,
    convert::TryInto,
    ops::{Bound, Deref, Neg},
};

/// If key already exists and is a string, this command appends the value at the
/// end of the string. If key does not exist it is created and set as an empty
/// string, so APPEND will be similar to SET in this special case.
pub async fn append(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().append(&args[0], &args[1])
}

/// Increments the number stored at key by one. If the key does not exist, it is set to 0 before
/// performing the operation. An error is returned if the key contains a value of the wrong type or
/// contains a string that can not be represented as integer. This operation is limited to 64 bit
/// signed integers.
pub async fn incr(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().incr(&args[0], 1_i64).map(|n| n.into())
}

/// Increments the number stored at key by increment. If the key does not exist, it is set to 0
/// before performing the operation. An error is returned if the key contains a value of the wrong
/// type or contains a string that can not be represented as integer. This operation is limited to
/// 64 bit signed integers.
pub async fn incr_by(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let by: i64 = bytes_to_number(&args[1])?;
    conn.db().incr(&args[0], by).map(|n| n.into())
}

/// Increment the string representing a floating point number stored at key by the specified
/// increment. By using a negative increment value, the result is that the value stored at the key
/// is decremented (by the obvious properties of addition). If the key does not exist, it is set to
/// 0 before performing the operation.
pub async fn incr_by_float(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let by = bytes_to_number::<Float>(&args[1])?;
    if by.is_infinite() || by.is_nan() {
        return Err(Error::IncrByInfOrNan);
    }
    conn.db().incr(&args[0], by).map(|f| {
        if f.fract() == 0.0 {
            (*f as i64).into()
        } else {
            f.to_string().into()
        }
    })
}

/// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before
/// performing the operation. An error is returned if the key contains a value of the wrong type or
/// contains a string that can not be represented as integer. This operation is limited to 64 bit
/// signed integers.
pub async fn decr(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().incr(&args[0], -1_i64).map(|n| n.into())
}

/// Decrements the number stored at key by decrement. If the key does not exist, it is set to 0
/// before performing the operation. An error is returned if the key contains a value of the wrong
/// type or contains a string that can not be represented as integer. This operation is limited to
/// 64 bit signed integers.
pub async fn decr_by(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let by: i64 = (&Value::new(&args[1])).try_into()?;
    conn.db().incr(&args[0], by.neg()).map(|n| n.into())
}

/// Get the value of key. If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only handles string values.
pub async fn get(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().get(&args[0]).into_inner())
}

/// Get the value of key and optionally set its expiration. GETEX is similar to
/// GET, but is a write command with additional options.
pub async fn getex(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let (expires_in, persist) = match args.len() {
        1 => (None, false),
        2 => {
            if check_arg!(args, 1, "PERSIST") {
                (None, true)
            } else {
                return Err(Error::Syntax);
            }
        }
        3 => match String::from_utf8_lossy(&args[1]).to_uppercase().as_str() {
            "EX" => (
                Some(Expiration::new(&args[2], false, false, b"GETEX")?),
                false,
            ),
            "PX" => (
                Some(Expiration::new(&args[2], true, false, b"GETEX")?),
                false,
            ),
            "EXAT" => (
                Some(Expiration::new(&args[2], false, true, b"GETEX")?),
                false,
            ),
            "PXAT" => (
                Some(Expiration::new(&args[2], true, true, b"GETEX")?),
                false,
            ),
            "PERSIST" => (None, Default::default()),
            _ => return Err(Error::Syntax),
        },
        _ => return Err(Error::Syntax),
    };
    Ok(conn.db().getex(
        &args[0],
        expires_in.map(|t| t.try_into()).transpose()?,
        persist,
    ))
}

/// Get the value of key. If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only handles string values.
pub async fn getrange(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let bytes = if let Some(value) = conn.db().get(&args[0]).inner() {
        match value.deref() {
            Value::Blob(binary) => binary.clone(),
            Value::BlobRw(binary) => binary.clone().freeze(),
            Value::Null => return Ok("".into()),
            _ => return Err(Error::WrongType),
        }
    } else {
        return Ok("".into());
    };

    let start = bytes_to_number::<i64>(&args[1])?;
    let end = bytes_to_number::<i64>(&args[2])?;
    let len = bytes.len();

    // resolve negative positions
    let start: usize = if start < 0 {
        (start + len as i64).try_into().unwrap_or(0)
    } else {
        start.try_into().expect("Positive number")
    };

    // resolve negative positions
    let end: usize = if end < 0 {
        if let Ok(val) = (end + len as i64).try_into() {
            val
        } else {
            return Ok("".into());
        }
    } else {
        end.try_into().expect("Positive number")
    };
    let end = min(end, len.checked_sub(1).unwrap_or_default());

    if end < start {
        return Ok("".into());
    }

    Ok(Value::Blob(
        bytes.slice((Bound::Included(start), Bound::Included(end))),
    ))
}

/// Get the value of key and delete the key. This command is similar to GET, except for the fact
/// that it also deletes the key on success (if and only if the key's value type is a string).
pub async fn getdel(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().getdel(&args[0]))
}

/// Atomically sets key to value and returns the old value stored at key. Returns an error when key
/// exists but does not hold a string value. Any previous time to live associated with the key is
/// discarded on successful SET operation.
pub async fn getset(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().getset(&args[0], Value::new(&args[1])))
}

/// Returns the values of all specified keys. For every key that does not hold a string value or
/// does not exist, the special value nil is returned. Because of this, the operation never fails.
pub async fn mget(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    Ok(conn.db().get_multi(args))
}

/// Set key to hold the string value. If key already holds a value, it is overwritten, regardless
/// of its type. Any previous time to live associated with the key is discarded on successful SET
/// operation.
pub async fn set(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let mut expiration = None;
    let mut keep_ttl = false;
    let mut override_value = Override::Yes;
    let mut return_previous = false;

    let command = b"SET";
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;

    loop {
        let arg = if let Some(arg) = args.pop_front() {
            String::from_utf8_lossy(&arg).to_uppercase()
        } else {
            break;
        };

        match arg.as_str() {
            "EX" => {
                if expiration.is_some() {
                    return Err(Error::Syntax);
                }
                expiration = Some(Expiration::new(
                    &args.pop_front().ok_or(Error::Syntax)?,
                    false,
                    false,
                    command,
                )?);
            }
            "PX" => {
                if expiration.is_some() {
                    return Err(Error::Syntax);
                }
                expiration = Some(Expiration::new(
                    &args.pop_front().ok_or(Error::Syntax)?,
                    true,
                    false,
                    command,
                )?);
            }
            "EXAT" => {
                if expiration.is_some() {
                    return Err(Error::Syntax);
                }
                expiration = Some(Expiration::new(
                    &args.pop_front().ok_or(Error::Syntax)?,
                    false,
                    true,
                    command,
                )?);
            }
            "PXAT" => {
                if expiration.is_some() {
                    return Err(Error::Syntax);
                }
                expiration = Some(Expiration::new(
                    &args.pop_front().ok_or(Error::Syntax)?,
                    true,
                    true,
                    command,
                )?);
            }
            "KEEPTTL" => keep_ttl = true,
            "NX" => override_value = Override::No,
            "XX" => override_value = Override::Only,
            "GET" => return_previous = true,
            _ => return Err(Error::Syntax),
        }
    }
    Ok(
        match conn.db().set_advanced(
            key,
            Value::Blob(value),
            expiration.map(|t| t.try_into()).transpose()?,
            override_value,
            keep_ttl,
            return_previous,
        ) {
            Value::Integer(1) => Value::Ok,
            Value::Integer(0) => Value::Null,
            any_return => any_return,
        },
    )
}

/// Sets the given keys to their respective values. MSET replaces existing
/// values with new values, just as regular SET. See MSETNX if you don't want to
/// overwrite existing values.  MSET is atomic, so all given keys are set at
/// once.
///
/// It is not possible for clients to see that some of the keys were
/// updated while others are unchanged.
pub async fn mset(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().multi_set(args, true).map_err(|e| match e {
        Error::Syntax => Error::WrongNumberArgument("MSET".to_owned()),
        e => e,
    })
}

/// Sets the given keys to their respective values. MSETNX will not perform any
/// operation at all even if just a single key already exists.
///
/// Because of this semantic MSETNX can be used in order to set different keys
/// representing different fields of an unique logic object in a way that
/// ensures that either all the fields or none at all are set.
///
/// MSETNX is atomic, so all given keys are set at once. It is not possible for
/// clients to see that some of the keys were updated while others are
/// unchanged.
pub async fn msetnx(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db().multi_set(args, false).map_err(|e| match e {
        Error::Syntax => Error::WrongNumberArgument("MSETNX".to_owned()),
        e => e,
    })
}

/// Set key to hold the string value and set key to timeout after a given number of seconds. This
/// command is equivalent to executing the following commands:
///
/// SET mykey value
/// EXPIRE mykey seconds
#[inline]
async fn setex_ex(
    command: &[u8],
    is_milliseconds: bool,
    conn: &Connection,
    mut args: VecDeque<Bytes>,
) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let expiration = args.pop_front().ok_or(Error::Syntax)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;

    let expires_in = Expiration::new(&expiration, is_milliseconds, false, command)?;

    Ok(conn
        .db()
        .set(key, Value::Blob(value), Some(expires_in.try_into()?)))
}

/// Set key to hold the string value and set key to timeout after a given number
/// of seconds. This command is equivalent to:
pub async fn setex(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    setex_ex(b"SETEX", false, conn, args).await
}

/// PSETEX works exactly like SETEX with the sole difference that the expire
/// time is specified in milliseconds instead of seconds.
pub async fn psetex(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    setex_ex(b"PSETEX", true, conn, args).await
}

/// Set key to hold string value if key does not exist. In that case, it is
/// equal to SET. When key already holds a value, no operation is performed.
/// SETNX is short for "SET if Not eXists".
pub async fn setnx(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;
    Ok(conn
        .db()
        .set_advanced(key, Value::Blob(value), None, Override::No, false, false))
}

/// Returns the length of the string value stored at key. An error is returned when key holds a
/// non-string value.
pub async fn strlen(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    if let Some(value) = conn.db().get(&args[0]).inner() {
        match value.deref() {
            Value::Blob(x) => Ok(x.len().into()),
            Value::String(x) => Ok(x.len().into()),
            Value::Null => Ok(0.into()),
            _ => Ok(Error::WrongType.into()),
        }
    } else {
        Ok(0.into())
    }
}

/// Overwrites part of the string stored at key, starting at the specified
/// offset, for the entire length of value. If the offset is larger than the
/// current length of the string at key, the string is padded with zero-bytes to
/// make offset fit. Non-existing keys are considered as empty strings, so this
/// command will make sure it holds a string large enough to be able to set
/// value at offset.
pub async fn setrange(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .set_range(&args[0], bytes_to_number(&args[1])?, &args[2])
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };

    #[tokio::test]
    async fn append() {
        let c = create_connection();
        assert_eq!(
            Ok(5.into()),
            run_command(&c, &["append", "foo", "cesar"]).await,
        );
        assert_eq!(
            Ok(10.into()),
            run_command(&c, &["append", "foo", "rodas"]).await,
        );

        let _ = run_command(&c, &["hset", "hash", "foo", "bar"]).await;
        assert_eq!(
            Err(Error::WrongType),
            run_command(&c, &["append", "hash", "rodas"]).await,
        );
    }

    #[tokio::test]
    async fn incr() {
        let c = create_connection();
        let r = run_command(&c, &["incr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(1)), r);
        let r = run_command(&c, &["incr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(2)), r);

        let x = run_command(&c, &["get", "foo"]).await;
        assert_eq!(Ok(Value::Blob("2".into())), x);
    }

    #[tokio::test]
    async fn incr_do_not_affect_ttl() {
        let c = create_connection();
        let r = run_command(&c, &["incr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["expire", "foo", "60"]).await;
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(60)), r);

        let r = run_command(&c, &["incr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(2)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(60)), r);
    }

    #[tokio::test]
    async fn decr() {
        let c = create_connection();
        let r = run_command(&c, &["decr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(-1)), r);
        let r = run_command(&c, &["decr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(-2)), r);
        let x = run_command(&c, &["get", "foo"]).await;
        assert_eq!(Ok(Value::Blob("-2".into())), x);
    }

    #[tokio::test]
    async fn decr_do_not_affect_ttl() {
        let c = create_connection();
        let r = run_command(&c, &["decr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(-1)), r);

        let r = run_command(&c, &["expire", "foo", "60"]).await;
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(60)), r);

        let r = run_command(&c, &["decr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(-2)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(60)), r);
    }

    #[tokio::test]
    async fn setnx() {
        let c = create_connection();
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["setnx", "foo", "bar"]).await
        );

        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["setnx", "foo", "barx"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec!["bar".into()])),
            run_command(&c, &["mget", "foo"]).await
        );
    }

    #[tokio::test]
    async fn mset_incorrect_values() {
        let c = create_connection();
        let x = run_command(&c, &["mset", "foo", "bar", "bar"]).await;
        assert_eq!(Err(Error::WrongNumberArgument("MSET".to_owned())), x);

        assert_eq!(
            Ok(Value::Array(vec![Value::Null, Value::Null])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn mset() {
        let c = create_connection();
        let x = run_command(&c, &["mset", "foo", "bar", "bar", "foo"]).await;
        assert_eq!(Ok(Value::Ok), x);

        assert_eq!(
            Ok(Value::Array(vec!["bar".into(), "foo".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn msetnx() {
        let c = create_connection();
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["msetnx", "foo", "bar", "bar", "foo"]).await
        );

        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["msetnx", "foo", "bar1", "bar", "foo1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec!["bar".into(), "foo".into()])),
            run_command(&c, &["mget", "foo", "bar"]).await
        );
    }

    #[tokio::test]
    async fn get_and_set() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]).await;
        assert_eq!(Ok(Value::Ok), x);

        let x = run_command(&c, &["get", "foo"]).await;
        assert_eq!(Ok(Value::Blob("bar".into())), x);
    }

    #[tokio::test]
    async fn setkeepttl() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar", "ex", "60"]).await
        );
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar1", "keepttl"]).await
        );
        assert_eq!(Ok("bar1".into()), run_command(&c, &["get", "foo"]).await);
        assert_eq!(Ok(60.into()), run_command(&c, &["ttl", "foo"]).await);

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar2"]).await
        );
        assert_eq!(Ok("bar2".into()), run_command(&c, &["get", "foo"]).await);
        assert_eq!(
            Ok(Value::Integer(-1)),
            run_command(&c, &["ttl", "foo"]).await
        );
    }

    #[tokio::test]
    async fn set_and_get_previous_result() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar", "ex", "60"]).await
        );
        assert_eq!(
            Ok("bar".into()),
            run_command(&c, &["set", "foo", "bar1", "get"]).await
        );
    }

    #[tokio::test]
    async fn set_nx() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar", "ex", "60", "nx"]).await
        );
        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["set", "foo", "bar1", "nx"]).await
        );
        assert_eq!(Ok("bar".into()), run_command(&c, &["get", "foo"]).await);
    }

    #[tokio::test]
    async fn set_xx() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["set", "foo", "bar1", "ex", "60", "xx"]).await
        );
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar2", "ex", "60", "nx"]).await
        );
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "bar3", "ex", "60", "xx"]).await
        );
        assert_eq!(Ok("bar3".into()), run_command(&c, &["get", "foo"]).await);
    }

    #[tokio::test]
    async fn set_incorrect_params() {
        let c = create_connection();
        assert_eq!(
            Err(Error::NotANumberType("an integer".to_owned())),
            run_command(&c, &["set", "foo", "bar1", "ex", "xx"]).await
        );
        assert_eq!(
            Err(Error::Syntax),
            run_command(&c, &["set", "foo", "bar1", "ex"]).await
        );
    }

    #[tokio::test]
    async fn getrange() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "this is a long string"]).await;
        assert_eq!(Ok(Value::Ok), x);

        assert_eq!(
            Ok("this is a long str".into()),
            run_command(&c, &["getrange", "foo", "0", "-4"]).await
        );

        assert_eq!(
            Ok("ring".into()),
            run_command(&c, &["getrange", "foo", "-4", "-1"]).await
        );

        assert_eq!(
            Ok("".into()),
            run_command(&c, &["getrange", "foo", "-4", "1"]).await
        );

        assert_eq!(
            Ok("ring".into()),
            run_command(&c, &["getrange", "foo", "-4", "1000000"]).await
        );

        assert_eq!(
            Ok("this is a long string".into()),
            run_command(&c, &["getrange", "foo", "-400", "1000000"]).await
        );

        assert_eq!(
            Ok("".into()),
            run_command(&c, &["getrange", "foo", "-400", "-1000000"]).await
        );

        assert_eq!(
            Ok("t".into()),
            run_command(&c, &["getrange", "foo", "0", "0"]).await
        );

        assert_eq!(Ok(Value::Null), run_command(&c, &["get", "fox"]).await);
    }

    #[tokio::test]
    async fn getdel() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]).await;
        assert_eq!(Ok(Value::Ok), x);

        assert_eq!(
            Ok(Value::Blob("bar".into())),
            run_command(&c, &["getdel", "foo"]).await
        );

        assert_eq!(Ok(Value::Null), run_command(&c, &["get", "foo"]).await);
    }

    #[tokio::test]
    async fn getset() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]).await;
        assert_eq!(Ok(Value::Ok), x);

        assert_eq!(
            Ok(Value::Blob("bar".into())),
            run_command(&c, &["getset", "foo", "1"]).await
        );

        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["get", "foo"]).await
        );
    }

    #[tokio::test]
    async fn strlen() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]).await;
        assert_eq!(Ok(Value::Ok), x);

        let x = run_command(&c, &["strlen", "foo"]).await;
        assert_eq!(Ok(Value::Integer(3)), x);

        let x = run_command(&c, &["strlen", "foxxo"]).await;
        assert_eq!(Ok(Value::Integer(0)), x);
    }

    #[tokio::test]
    async fn setex() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["setex", "foo", "10", "bar"]).await
        );
        assert_eq!(Ok("bar".into()), run_command(&c, &["get", "foo"]).await);
        assert_eq!(Ok(10.into()), run_command(&c, &["ttl", "foo"]).await);
    }

    #[tokio::test]
    async fn wrong_type() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "xxx", "key", "foo"]).await;
        let _ = run_command(&c, &["incr", "foo"]).await;

        let x = run_command(&c, &["strlen", "xxx"]).await;
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["get", "xxx"]).await;
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["get", "xxx"]).await;
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["mget", "xxx", "foo"]).await;
        assert_eq!(
            Ok(Value::Array(vec![Value::Null, Value::Blob("1".into()),])),
            x
        );
    }

    #[tokio::test]
    async fn test_set_range() {
        let c = create_connection();
        assert_eq!(
            Ok(23.into()),
            run_command(&c, &["setrange", "foo", "20", "xxx"]).await,
        );
        assert_eq!(
            Ok(23.into()),
            run_command(&c, &["setrange", "foo", "2", "xxx"]).await,
        );
        assert_eq!(
            Ok(33.into()),
            run_command(&c, &["setrange", "foo", "30", "xxx"]).await,
        );
        assert_eq!(
            Ok(Value::BlobRw(
                "\0\0xxx\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0xxx\0\0\0\0\0\0\0xxx".into()
            )),
            run_command(&c, &["get", "foo"]).await,
        );
    }

    #[tokio::test]
    async fn test_set_px() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["set", "foo", "20", "px", "1234"]).await,
        );
    }

    #[tokio::test]
    async fn test_invalid_ts() {
        let c = create_connection();
        assert_eq!(
            Err(Error::InvalidExpire("set".to_owned())),
            run_command(&c, &["set", "foo", "bar", "EX", "10000000000000000"]).await
        );
    }
}
