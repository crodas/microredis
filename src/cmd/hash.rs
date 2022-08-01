//! # Hash command handlers
use crate::{
    check_arg,
    connection::Connection,
    error::Error,
    value::Value,
    value::{bytes_to_number, float::Float},
};
use bytes::Bytes;
use rand::Rng;
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    ops::AddAssign,
    str::FromStr,
};

/// Removes the specified fields from the hash stored at key. Specified fields that do not exist
/// within this hash are ignored. If key does not exist, it is treated as an empty hash and this
/// command returns 0.
pub async fn hdel(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let mut is_empty = false;
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut h = h.write();
                let mut total: i64 = 0;

                for key in (&args[2..]).iter() {
                    if h.remove(key).is_some() {
                        total += 1;
                    }
                }

                is_empty = h.len() == 0;

                Ok(total.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )?;

    if is_empty {
        let _ = conn.db().del(&[args[1].clone()]);
    } else {
        conn.db().bump_version(&args[1]);
    }

    Ok(result)
}

/// Returns if field is an existing field in the hash stored at key.
pub async fn hexists(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(if h.read().get(&args[2]).is_some() {
                1.into()
            } else {
                0.into()
            }),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

/// Returns the value associated with field in the hash stored at key.
pub async fn hget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(h
                .read()
                .get(&args[2])
                .map(|v| Value::new(v))
                .unwrap_or_default()),
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Null),
    )
}

/// Returns all fields and values of the hash stored at key. In the returned value, every field
/// name is followed by its value, so the length of the reply is twice the size of the hash.
pub async fn hgetall(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for (key, value) in h.read().iter() {
                    ret.push(Value::new(&key));
                    ret.push(Value::new(&value));
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

/// Increment the specified field of a hash stored at key, and representing a number, by the
/// specified increment. If the increment value is negative, the result is to have the hash field
/// value decremented instead of incremented. If the field does not exist, it is set to 0 before
/// performing the operation.
pub async fn hincrby_int(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn
        .db()
        .hincrby::<i64>(&args[1], &args[2], &args[3], "an integer")?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

/// Increment the specified field of a hash stored at key, and representing a number, by the
/// specified increment. If the increment value is negative, the result is to have the hash field
/// value decremented instead of incremented. If the field does not exist, it is set to 0 before
/// performing the operation.
pub async fn hincrby_float(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn
        .db()
        .hincrby::<Float>(&args[1], &args[2], &args[3], "a float")?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

/// Returns all field names in the hash stored at key.
pub async fn hkeys(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for key in h.read().keys() {
                    ret.push(Value::new(&key));
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

/// Returns the number of fields contained in the hash stored at key.
pub async fn hlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(h.read().len().into()),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

/// Returns the values associated with the specified fields in the hash stored at key.
pub async fn hmget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let h = h.read();

                Ok((&args[2..])
                    .iter()
                    .map(|key| h.get(key).map(|v| Value::new(v)).unwrap_or_default())
                    .collect::<Vec<Value>>()
                    .into())
            }
            _ => Err(Error::WrongType),
        },
        || {
            Ok((&args[2..])
                .iter()
                .map(|_| Value::Null)
                .collect::<Vec<Value>>()
                .into())
        },
    )
}

/// Returns random keys (or values) from a hash
pub async fn hrandfield(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let (count, with_values) = match args.len() {
        2 => (None, false),
        3 => (Some(bytes_to_number::<i64>(&args[2])?), false),
        4 => {
            if !(check_arg!(args, 3, "WITHVALUES")) {
                return Err(Error::Syntax);
            }
            (Some(bytes_to_number::<i64>(&args[2])?), true)
        }
        _ => return Err(Error::InvalidArgsCount("hrandfield".to_owned())),
    };

    let (count, single, repeat) = match count {
        Some(count) if count > 0 => (count, false, 1),
        Some(count) => (count.abs(), false, count.abs()),
        _ => (1, true, 1),
    };

    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];
                let mut i = 0;
                let mut rand_sorted = BTreeMap::new();
                let mut rng = rand::thread_rng();
                let h = h.read();

                for _ in 0..repeat {
                    for (key, value) in h.iter() {
                        let rand = rng.gen::<u64>();
                        rand_sorted.insert((rand, i), (key, value));
                        i += 1;
                    }
                }

                i = 0;
                for val in rand_sorted.values() {
                    if single {
                        return Ok(Value::new(&val.0));
                    }

                    if i == count {
                        break;
                    }

                    ret.push(Value::new(&val.0));

                    if with_values {
                        ret.push(Value::new(&val.1));
                    }

                    i += 1;
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

/// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash
/// is created. If field already exists in the hash, it is overwritten.
pub async fn hset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_hmset = check_arg!(args, 0, "HMSET");
    if args.len() % 2 == 1 {
        return Err(Error::InvalidArgsCount("hset".to_owned()));
    }
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut h = h.write();
                let mut e: i64 = 0;
                for i in (2..args.len()).step_by(2) {
                    if h.insert(args[i].clone(), args[i + 1].clone()).is_none() {
                        e += 1;
                    }
                }
                if is_hmset {
                    Ok(Value::Ok)
                } else {
                    Ok(e.into())
                }
            }
            _ => Err(Error::WrongType),
        },
        || {
            #[allow(clippy::mutable_key_type)]
            let mut h = HashMap::new();
            for i in (2..args.len()).step_by(2) {
                h.insert(args[i].clone(), args[i + 1].clone());
            }
            let len = h.len();
            conn.db().set(&args[1], h.into(), None);
            if is_hmset {
                Ok(Value::Ok)
            } else {
                Ok(len.into())
            }
        },
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

/// Sets field in the hash stored at key to value, only if field does not yet exist. If key does
/// not exist, a new key holding a hash is created. If field already exists, this operation has no
/// effect.
pub async fn hsetnx(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut h = h.write();

                if h.get(&args[2]).is_some() {
                    Ok(0.into())
                } else {
                    h.insert(args[2].clone(), args[3].clone());
                    Ok(1.into())
                }
            }
            _ => Err(Error::WrongType),
        },
        || {
            #[allow(clippy::mutable_key_type)]
            let mut h = HashMap::new();
            for i in (2..args.len()).step_by(2) {
                h.insert(args[i].clone(), args[i + 1].clone());
            }
            let len = h.len();
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )?;

    if result == Value::Integer(1) {
        conn.db().bump_version(&args[1]);
    }

    Ok(result)
}

/// Returns the string length of the value associated with field in the hash stored at key. If the
/// key or the field do not exist, 0 is returned.
pub async fn hstrlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(h
                .read()
                .get(&args[2])
                .map(|v| v.len())
                .unwrap_or_default()
                .into()),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

/// Returns all values in the hash stored at key.
pub async fn hvals(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for value in h.read().values() {
                    ret.push(Value::new(&value));
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, invalid_type, run_command},
        value::Value,
    };

    #[tokio::test]
    async fn hget() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hget", "foo", "f1"]).await;
        assert_eq!(Ok(Value::Blob("1".into())), r);
    }

    #[tokio::test]
    async fn hgetall() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hgetall", "foo"]).await;
        match r {
            Ok(Value::Array(x)) => {
                assert_eq!(6, x.len());
                assert!(
                    x[0] == Value::Blob("f1".into())
                        || x[0] == Value::Blob("f2".into())
                        || x[0] == Value::Blob("f3".into())
                )
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn hrandfield() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hrandfield", "foo"]).await;
        match r {
            Ok(Value::Blob(x)) => {
                let x = String::from_utf8_lossy(&x);
                assert!(x == *"f1" || x == *"f2" || x == *"f3");
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn hmget() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hmget", "foo", "f1", "f2"]).await;
        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
            ])),
            r
        );
    }

    #[tokio::test]
    async fn hexists() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hexists", "foo", "f1"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hexists", "foo", "f3"]).await
        );
        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["hexists", "foo", "f4"]).await
        );
    }
    #[tokio::test]
    async fn hstrlen() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hstrlen", "foo", "f1"]).await;
        assert_eq!(Ok(Value::Integer(1)), r);
    }

    #[tokio::test]
    async fn hlen() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1", "f2", "2", "f3", "3"]).await;

        assert_eq!(Ok(Value::Integer(3)), r);

        let r = run_command(&c, &["hset", "foo", "f1", "2", "f4", "2", "f5", "3"]).await;
        assert_eq!(Ok(Value::Integer(2)), r);

        let r = run_command(&c, &["hlen", "foo"]).await;
        assert_eq!(Ok(Value::Integer(5)), r);
    }

    #[tokio::test]
    async fn hkeys() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1"]).await;

        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["hkeys", "foo"]).await;
        assert_eq!(Ok(Value::Array(vec![Value::Blob("f1".into()),])), r);
    }

    #[tokio::test]
    async fn hvals() {
        let c = create_connection();
        let r = run_command(&c, &["hset", "foo", "f1", "1"]).await;

        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["hvals", "foo"]).await;
        assert_eq!(Ok(Value::Array(vec![Value::Blob("1".into()),])), r);
    }

    #[tokio::test]
    async fn hdel_remove_empty_hash() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["hset", "foo", "f1", "1", "f2", "1"]).await
        );

        assert_eq!(Ok(1.into()), run_command(&c, &["hdel", "foo", "f1",]).await);
        assert_eq!(
            Ok(Value::Integer(-1)),
            run_command(&c, &["ttl", "foo"]).await
        );
        assert_eq!(Ok(1.into()), run_command(&c, &["hdel", "foo", "f2",]).await);
        assert_eq!(
            Ok(Value::Integer(-2)),
            run_command(&c, &["ttl", "foo"]).await
        );
    }

    #[tokio::test]
    async fn hincrby() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hincrby", "foo", "f1", "1"]).await
        );
        assert_eq!(
            Ok(Value::Integer(-9)),
            run_command(&c, &["hincrby", "foo", "f1", "-10"]).await
        );

        assert_eq!(
            Ok(Value::Blob("-9".into())),
            run_command(&c, &["hget", "foo", "f1"]).await
        );
    }

    #[tokio::test]
    async fn hsetnx() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hsetnx", "foo", "xxx", "1"]).await
        );
        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["hsetnx", "foo", "xxx", "1"]).await
        );
        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["hsetnx", "foo", "bar", "1"]).await
        );
        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["hlen", "foo"]).await
        );
    }

    #[tokio::test]
    async fn hlen_non_existing() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["hlen", "foo"]).await
        );
    }

    #[tokio::test]
    async fn invalid_types() {
        invalid_type(&["hdel", "key", "bar", "1"]).await;
        invalid_type(&["hexists", "key", "bar"]).await;
        invalid_type(&["hget", "key", "bar"]).await;
        invalid_type(&["hgetall", "key"]).await;
        invalid_type(&["hincrby", "key", "bar", "1"]).await;
        invalid_type(&["hincrbyfloat", "key", "bar", "1"]).await;
        invalid_type(&["hkeys", "key"]).await;
        invalid_type(&["hlen", "key"]).await;
        invalid_type(&["hstrlen", "key", "foo"]).await;
        invalid_type(&["hmget", "key", "1", "2"]).await;
        invalid_type(&["hrandfield", "key"]).await;
        invalid_type(&["hset", "key", "bar", "1"]).await;
        invalid_type(&["hsetnx", "key", "bar", "1"]).await;
        invalid_type(&["hvals", "key"]).await;
    }
}
