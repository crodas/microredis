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
    let by: i64 = bytes_to_number(&args[2])?;
    conn.db().incr(&args[1], by)
}

pub fn incr_by_float(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let by: f64 = bytes_to_number(&args[2])?;
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
        .set(&args[1], Value::Blob(args[2].to_owned()), None))
}

pub fn setex(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let ttl = if check_arg!(args, 0, "SETEX") {
        Duration::from_secs(bytes_to_number(&args[2])?)
    } else {
        Duration::from_millis(bytes_to_number(&args[2])?)
    };

    Ok(conn
        .db()
        .set(&args[1], Value::Blob(args[2].to_owned()), Some(ttl)))
}

pub fn strlen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match conn.db().get(&args[1]) {
        Value::Blob(x) => Ok((x.len() as i64).into()),
        Value::String(x) => Ok((x.len() as i64).into()),
        Value::Null => Ok(0_i64.into()),
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

    #[test]
    fn incr() {
        let c = create_connection();
        let r = run_command(&c, &["incr", "foo"]);
        assert_eq!(Ok(Value::Integer(1)), r);
        let r = run_command(&c, &["incr", "foo"]);
        assert_eq!(Ok(Value::Integer(2)), r);

        let x = run_command(&c, &["get", "foo"]);
        assert_eq!(Ok(Value::Blob("2".into())), x);
    }

    #[test]
    fn incr_do_not_affect_ttl() {
        let c = create_connection();
        let r = run_command(&c, &["incr", "foo"]);
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["expire", "foo", "60"]);
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["ttl", "foo"]);
        assert_eq!(Ok(Value::Integer(59)), r);

        let r = run_command(&c, &["incr", "foo"]);
        assert_eq!(Ok(Value::Integer(2)), r);

        let r = run_command(&c, &["ttl", "foo"]);
        assert_eq!(Ok(Value::Integer(59)), r);
    }

    #[test]
    fn decr() {
        let c = create_connection();
        let r = run_command(&c, &["decr", "foo"]);
        assert_eq!(Ok(Value::Integer(-1)), r);
        let r = run_command(&c, &["decr", "foo"]);
        assert_eq!(Ok(Value::Integer(-2)), r);
        let x = run_command(&c, &["get", "foo"]);
        assert_eq!(Ok(Value::Blob("-2".into())), x);
    }

    #[test]
    fn decr_do_not_affect_ttl() {
        let c = create_connection();
        let r = run_command(&c, &["decr", "foo"]);
        assert_eq!(Ok(Value::Integer(-1)), r);

        let r = run_command(&c, &["expire", "foo", "60"]);
        assert_eq!(Ok(Value::Integer(1)), r);

        let r = run_command(&c, &["ttl", "foo"]);
        assert_eq!(Ok(Value::Integer(59)), r);

        let r = run_command(&c, &["decr", "foo"]);
        assert_eq!(Ok(Value::Integer(-2)), r);

        let r = run_command(&c, &["ttl", "foo"]);
        assert_eq!(Ok(Value::Integer(59)), r);
    }

    #[test]
    fn get_and_set() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]);
        assert_eq!(Ok(Value::OK), x);

        let x = run_command(&c, &["get", "foo"]);
        assert_eq!(Ok(Value::Blob("bar".into())), x);
    }

    #[test]
    fn getdel() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]);
        assert_eq!(Ok(Value::OK), x);

        assert_eq!(
            Ok(Value::Blob("bar".into())),
            run_command(&c, &["getdel", "foo"])
        );

        assert_eq!(Ok(Value::Null), run_command(&c, &["get", "foo"]));
    }

    #[test]
    fn getset() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]);
        assert_eq!(Ok(Value::OK), x);

        assert_eq!(
            Ok(Value::Blob("bar".into())),
            run_command(&c, &["getset", "foo", "1"])
        );

        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["get", "foo"])
        );
    }

    #[test]
    fn strlen() {
        let c = create_connection();
        let x = run_command(&c, &["set", "foo", "bar"]);
        assert_eq!(Ok(Value::OK), x);

        let x = run_command(&c, &["strlen", "foo"]);
        assert_eq!(Ok(Value::Integer(3)), x);

        let x = run_command(&c, &["strlen", "foxxo"]);
        assert_eq!(Ok(Value::Integer(0)), x);
    }

    #[test]
    fn wrong_type() {
        let c = create_connection();
        let _ = run_command(&c, &["hset", "xxx", "key", "foo"]);
        let _ = run_command(&c, &["incr", "foo"]);

        let x = run_command(&c, &["strlen", "xxx"]);
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["get", "xxx"]);
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["get", "xxx"]);
        assert_eq!(Ok(Error::WrongType.into()), x);

        let x = run_command(&c, &["mget", "xxx", "foo"]);
        assert_eq!(
            Ok(Value::Array(vec![Value::Null, Value::Blob("1".into()),])),
            x
        );
    }
}
