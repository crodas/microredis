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

pub fn hdel(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map(
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
        || Ok(0_i64.into()),
    )
}

pub fn hexists(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => Ok(if h.read().get(&args[2]).is_some() {
            1_i64.into()
        } else {
            0_i64.into()
        }),
        Value::Null => Ok(0_i64.into()),
        _ => Err(Error::WrongType),
    }
}

pub fn hget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => Ok(if let Some(v) = h.read().get(&args[2]) {
            Value::Blob(v.clone())
        } else {
            Value::Null
        }),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::WrongType),
    }
}

pub fn hgetall(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => {
            let mut ret = vec![];

            for (key, value) in h.read().iter() {
                ret.push(Value::Blob(key.clone()));
                ret.push(Value::Blob(value.clone()));
            }

            Ok(ret.into())
        }
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err(Error::WrongType),
    }
}

pub fn hincrby<
    T: ToString + FromStr + AddAssign + for<'a> TryFrom<&'a Value, Error = Error> + Into<Value> + Copy,
>(
    conn: &Connection,
    args: &[Bytes],
) -> Result<Value, Error> {
    conn.db().get_map(
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
    )
}

pub fn hkeys(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => {
            let mut ret = vec![];

            for key in h.read().keys() {
                ret.push(Value::Blob(key.clone()));
            }

            Ok(ret.into())
        }
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err(Error::WrongType),
    }
}

pub fn hlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => Ok((h.read().len() as i64).into()),
        Value::Null => Ok(0_i64.into()),
        _ => Err(Error::WrongType),
    }
}

pub fn hmget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
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
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err(Error::WrongType),
    }
}

pub fn hrandfield(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
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
    let (count, single, repeat) = if let Some(count) = count {
        if count > 0 {
            (count, false, 1)
        } else {
            (count.abs(), false, count.abs())
        }
    } else {
        (1, true, 1)
    };

    match conn.db().get(&args[1]) {
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
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err(Error::WrongType),
    }
}

pub fn hset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    if args.len() % 2 == 1 {
        return Err(Error::InvalidArgsCount("hset".to_owned()));
    }
    conn.db().get_map(
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
            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )
}

pub fn hsetnx(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map(
        &args[1],
        |v| match v {
            Value::Hash(h) => {
                let mut h = h.write();

                if h.get(&args[2]).is_some() {
                    Ok(0_i64.into())
                } else {
                    h.insert(args[2].clone(), args[3].clone());
                    Ok(1_i64.into())
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
            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )
}

pub fn hstrlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => Ok(if let Some(v) = h.read().get(&args[2]) {
            (v.len() as i64).into()
        } else {
            0_i64.into()
        }),
        Value::Null => Ok(0_i64.into()),
        _ => Err(Error::WrongType),
    }
}

pub fn hvals(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Hash(h) => {
            let mut ret = vec![];

            for value in h.read().values() {
                ret.push(Value::Blob(value.clone()));
            }

            Ok(ret.into())
        }
        Value::Null => Ok(Value::Array(vec![])),
        _ => Err(Error::WrongType),
    }
}
