use crate::{connection::Connection, error::Error, value::Value};
use std::convert::TryInto;
use bytes::Bytes;

pub fn incr_by(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: i64 = (&Value::Blob(args[2].to_owned())).try_into()?;
    conn.db().incr(&args[1], by)
}

pub fn incr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], 1)
}

pub fn decr(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().incr(&args[1], -1)
}

pub fn get(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get(&args[1])
}

pub fn set(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().set(&args[1], &Value::Blob(args[2].to_owned()))
}

pub fn getset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().getset(&args[1], &Value::Blob(args[2].to_owned()))
}
