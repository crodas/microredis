//! # String command handlers
use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use std::{convert::TryInto, ops::Neg};
use tokio::time::Duration;

/// Increments the number stored at key by one. If the key does not exist, it is set to 0 before
/// performing the operation. An error is returned if the key contains a value of the wrong type or
/// contains a string that can not be represented as integer. This operation is limited to 64 bit
/// signed integers.
pub async fn incr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], 1_i64)
}

/// Increments the number stored at key by increment. If the key does not exist, it is set to 0
/// before performing the operation. An error is returned if the key contains a value of the wrong
/// type or contains a string that can not be represented as integer. This operation is limited to
/// 64 bit signed integers.
pub async fn incr_by(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: i64 = bytes_to_number(&args[2])?;
    conn.db().incr(&args[1], by)
}

/// Increment the string representing a floating point number stored at key by the specified
/// increment. By using a negative increment value, the result is that the value stored at the key
/// is decremented (by the obvious properties of addition). If the key does not exist, it is set to
/// 0 before performing the operation.
pub async fn incr_by_float(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: f64 = bytes_to_number(&args[2])?;
    conn.db().incr(&args[1], by)
}

/// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before
/// performing the operation. An error is returned if the key contains a value of the wrong type or
/// contains a string that can not be represented as integer. This operation is limited to 64 bit
/// signed integers.
pub async fn decr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], -1_i64)
}

/// Decrements the number stored at key by decrement. If the key does not exist, it is set to 0
/// before performing the operation. An error is returned if the key contains a value of the wrong
/// type or contains a string that can not be represented as integer. This operation is limited to
/// 64 bit signed integers.
pub async fn decr_by(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: i64 = (&Value::Blob(args[2].to_owned())).try_into()?;
    conn.db().incr(&args[1], by.neg())
}

/// Get the value of key. If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only handles string values.
pub async fn get(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get(&args[1]))
}

/// Get the value of key and delete the key. This command is similar to GET, except for the fact
/// that it also deletes the key on success (if and only if the key's value type is a string).
pub async fn getdel(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().getdel(&args[1]))
}

/// Atomically sets key to value and returns the old value stored at key. Returns an error when key
/// exists but does not hold a string value. Any previous time to live associated with the key is
/// discarded on successful SET operation.
pub async fn getset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().getset(&args[1], &Value::Blob(args[2].to_owned())))
}

/// Returns the values of all specified keys. For every key that does not hold a string value or
/// does not exist, the special value nil is returned. Because of this, the operation never fails.
pub async fn mget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get_multi(&args[1..]))
}

/// Set key to hold the string value. If key already holds a value, it is overwritten, regardless
/// of its type. Any previous time to live associated with the key is discarded on successful SET
/// operation.
pub async fn set(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn
        .db()
        .set(&args[1], Value::Blob(args[2].to_owned()), None))
}

/// Set key to hold the string value and set key to timeout after a given number of seconds. This
/// command is equivalent to executing the following commands:
///
/// SET mykey value
/// EXPIRE mykey seconds
pub async fn setex(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = if check_arg!(args, 0, "SETEX") {
        Duration::from_secs(bytes_to_number(&args[2])?)
    } else {
        Duration::from_millis(bytes_to_number(&args[2])?)
    };

    Ok(conn
        .db()
        .set(&args[1], Value::Blob(args[2].to_owned()), Some(ttl)))
}

/// Returns the length of the string value stored at key. An error is returned when key holds a
/// non-string value.
pub async fn strlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Blob(x) => Ok(x.len().into()),
        Value::String(x) => Ok(x.len().into()),
        Value::Null => Ok(0.into()),
        _ => Ok(Error::WrongType.into()),
    }
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };

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
        assert_eq!(Ok(Value::Integer(59)), r);

        let r = run_command(&c, &["incr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(2)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(59)), r);
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
        assert_eq!(Ok(Value::Integer(59)), r);

        let r = run_command(&c, &["decr", "foo"]).await;
        assert_eq!(Ok(Value::Integer(-2)), r);

        let r = run_command(&c, &["ttl", "foo"]).await;
        assert_eq!(Ok(Value::Integer(59)), r);
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
}
