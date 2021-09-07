use crate::{db::Db, error::Error, value::Value};
use bytes::Bytes;

pub fn incr(db: &Db, args: &[Bytes]) -> Result<Value, Error> {
    db.incr(&args[1], 1)
}

pub fn decr(db: &Db, args: &[Bytes]) -> Result<Value, Error> {
    db.incr(&args[1], -1)
}

pub fn get(db: &Db, args: &[Bytes]) -> Result<Value, Error> {
    db.get(&args[1])
}

pub fn set(db: &Db, args: &[Bytes]) -> Result<Value, Error> {
    db.set(&args[1], &Value::Blob(args[2].to_owned()))
}
