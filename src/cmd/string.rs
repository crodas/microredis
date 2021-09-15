use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::Value,
};
use bytes::Bytes;
use std::{convert::TryInto, ops::Neg};
use tokio::time::Duration;

pub fn incr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], 1)
}

pub fn incr_by(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: i64 = (&Value::Blob(args[2].to_owned())).try_into()?;
    conn.db().incr(&args[1], by)
}

pub fn incr_by_float(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: f64 = (&Value::Blob(args[2].to_owned())).try_into()?;
    conn.db().incr(&args[1], by)
}

pub fn decr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], -1)
}

pub fn decr_by(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: i64 = (&Value::Blob(args[2].to_owned())).try_into()?;
    conn.db().incr(&args[1], by.neg())
}

pub fn get(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get(&args[1]))
}

pub fn getdel(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().getdel(&args[1]))
}

pub fn getset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().getset(&args[1], &Value::Blob(args[2].to_owned())))
}

pub fn mget(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn.db().get_multi(&args[1..]))
}

pub fn set(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(conn
        .db()
        .set(&args[1], &Value::Blob(args[2].to_owned()), None))
}

pub fn setex(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = if check_arg!(args, 0, "SETEX") {
        Duration::from_secs(bytes_to_number(&args[2])?)
    } else {
        Duration::from_millis(bytes_to_number(&args[2])?)
    };

    Ok(conn
        .db()
        .set(&args[1], &Value::Blob(args[2].to_owned()), Some(ttl)))
}

pub fn strlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Blob(x) => Ok((x.len() as i64).into()),
        Value::String(x) => Ok((x.len() as i64).into()),
        Value::Null => Ok(0_i64.into()),
        _ => Err(Error::WrongType),
    }
}
