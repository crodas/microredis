use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use rand::Rng;
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    ops::AddAssign,
    str::FromStr,
};

pub async fn hdel(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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

                Ok(total.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

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

pub async fn hget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(if let Some(v) = h.read().get(&args[2]) {
                Value::Blob(v.clone())
            } else {
                Value::Null
            }),
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Null),
    )
}

pub async fn hgetall(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for (key, value) in h.read().iter() {
                    ret.push(Value::Blob(key.clone()));
                    ret.push(Value::Blob(value.clone()));
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

pub async fn hincrby<
    T: ToString + FromStr + AddAssign + for<'a> TryFrom<&'a Value, Error = Error> + Into<Value> + Copy,
>(
    conn: &Connection,
    args: &[Bytes],
) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut incr_by: T = bytes_to_number(&args[3])?;
                let mut h = h.write();
                if let Some(n) = h.get(&args[2]) {
                    incr_by += bytes_to_number(n)?;
                }

                h.insert(args[2].clone(), incr_by.to_string().into());

                Ok(incr_by.into())
            }
            _ => Err(Error::WrongType),
        },
        || {
            let incr_by: T = bytes_to_number(&args[3])?;
            #[allow(clippy::mutable_key_type)]
            let mut h = HashMap::new();
            h.insert(args[2].clone(), incr_by.to_string().into());
            conn.db().set(&args[1], h.into(), None);
            Ok(incr_by.into())
        },
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

pub async fn hkeys(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for key in h.read().keys() {
                    ret.push(Value::Blob(key.clone()));
                }

                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

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

pub async fn hmget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let h = h.read();

                Ok((&args[2..])
                    .iter()
                    .map(|key| {
                        if let Some(value) = h.get(key) {
                            Value::Blob(value.clone())
                        } else {
                            Value::Null
                        }
                    })
                    .collect::<Vec<Value>>()
                    .into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

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
                        return Ok(Value::Blob(val.0.clone()));
                    }

                    if i == count {
                        break;
                    }

                    ret.push(Value::Blob(val.0.clone()));

                    if with_values {
                        ret.push(Value::Blob(val.1.clone()));
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

pub async fn hset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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
                Ok(e.into())
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

    conn.db().bump_version(&args[1]);

    Ok(result)
}

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

pub async fn hstrlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => Ok(if let Some(v) = h.read().get(&args[2]) {
                v.len().into()
            } else {
                0.into()
            }),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

pub async fn hvals(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut ret = vec![];

                for value in h.read().values() {
                    ret.push(Value::Blob(value.clone()));
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
        cmd::test::{create_connection, run_command},
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
            _ => assert!(false),
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
                assert!(x == "f1".to_owned() || x == "f2".to_owned() || x == "f3".to_owned());
            }
            _ => assert!(false),
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
}
