use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::checksum,
    value::Value,
};
use bytes::Bytes;
use std::collections::LinkedList;
use tokio::time::{sleep, Duration, Instant};

#[allow(clippy::needless_range_loop)]
fn remove_element(
    conn: &Connection,
    key: &Bytes,
    count: usize,
    front: bool,
) -> Result<Value, Error> {
    conn.db().get_map_or(
        key,
        |v| match v {
            Value::List(x) => {
                let mut x = x.write();

                if count == 0 {
                    // Return a single element
                    return Ok((if front { x.pop_front() } else { x.pop_back() })
                        .map_or(Value::Null, |x| x.clone_value()));
                }

                let mut ret = vec![None; count];

                for i in 0..count {
                    if front {
                        ret[i] = x.pop_front();
                    } else {
                        ret[i] = x.pop_back();
                    }
                }

                let ret: Vec<Value> = ret
                    .iter()
                    .filter(|v| v.is_some())
                    .map(|x| x.as_ref().unwrap().clone_value())
                    .collect();

                Ok(if ret.is_empty() {
                    Value::Null
                } else {
                    ret.into()
                })
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Null),
    )
}

pub async fn blpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let timeout = Instant::now() + Duration::from_secs(bytes_to_number(&args[args.len() - 1])?);
    let len = args.len() - 1;

    loop {
        for key in args[1..len].iter() {
            match remove_element(conn, key, 0, true)? {
                Value::Null => (),
                n => return Ok(vec![Value::Blob(key.clone()), n].into()),
            };
        }

        if Instant::now() >= timeout {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    Ok(Value::Null)
}

pub async fn brpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let timeout = Instant::now() + Duration::from_secs(bytes_to_number(&args[args.len() - 1])?);
    let len = args.len() - 1;

    loop {
        for key in args[1..len].iter() {
            match remove_element(conn, key, 0, false)? {
                Value::Null => (),
                n => return Ok(vec![Value::Blob(key.clone()), n].into()),
            };
        }

        if Instant::now() >= timeout {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    Ok(Value::Null)
}

pub async fn llen(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => Ok((x.read().len() as i64).into()),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

pub async fn lpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let count = if args.len() > 2 {
        bytes_to_number(&args[2])?
    } else {
        0
    };

    remove_element(conn, &args[1], count, true)
}

pub async fn lpush(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_push_x = check_arg!(args, 0, "LPUSHX");

    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut x = x.write();
                for val in args.iter().skip(2) {
                    x.push_front(checksum::Value::new(val.clone()));
                }
                Ok((x.len() as i64).into())
            }
            _ => Err(Error::WrongType),
        },
        || {
            if is_push_x {
                return Ok(0.into());
            }
            let mut h = LinkedList::new();

            for val in args.iter().skip(2) {
                h.push_front(checksum::Value::new(val.clone()));
            }

            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )
}

pub async fn lrange(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut start: i64 = bytes_to_number(&args[2])?;
                let mut end: i64 = bytes_to_number(&args[3])?;
                let mut ret = vec![];
                let x = x.read();

                if start < 0 {
                    start += x.len() as i64;
                }

                if end < 0 {
                    end += x.len() as i64;
                }

                for (i, val) in x.iter().enumerate() {
                    if i >= start as usize && i <= end as usize {
                        ret.push(val.clone_value());
                    }
                }
                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

pub async fn rpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let count = if args.len() > 2 {
        bytes_to_number(&args[2])?
    } else {
        0
    };

    remove_element(conn, &args[1], count, false)
}

pub async fn rpush(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_push_x = check_arg!(args, 0, "RPUSHX");

    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut x = x.write();
                for val in args.iter().skip(2) {
                    x.push_back(checksum::Value::new(val.clone()));
                }
                Ok((x.len() as i64).into())
            }
            _ => Err(Error::WrongType),
        },
        || {
            if is_push_x {
                return Ok(0.into());
            }
            let mut h = LinkedList::new();

            for val in args.iter().skip(2) {
                h.push_back(checksum::Value::new(val.clone()));
            }

            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        value::Value,
    };

    #[tokio::test]
    async fn llen() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
            run_command(&c, &["llen", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["llen", "foobar"]).await
        );
    }

    #[tokio::test]
    async fn lpop() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Blob("5".into())),
            run_command(&c, &["lpop", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("4".into())])),
            run_command(&c, &["lpop", "foo", "1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("3".into()),
                Value::Blob("2".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["lpop", "foo", "55"]).await
        );

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["lpop", "foo", "55"]).await
        );

        assert_eq!(Ok(Value::Null), run_command(&c, &["lpop", "foo"]).await);

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["llen", "foobar"]).await
        );
    }

    #[tokio::test]
    async fn lpush() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("5".into()),
                Value::Blob("4".into()),
                Value::Blob("3".into()),
                Value::Blob("2".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(10)),
            run_command(&c, &["lpush", "foo", "6", "7", "8", "9", "10"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("10".into()),
                Value::Blob("9".into()),
                Value::Blob("8".into()),
                Value::Blob("7".into()),
                Value::Blob("6".into()),
                Value::Blob("5".into()),
                Value::Blob("4".into()),
                Value::Blob("3".into()),
                Value::Blob("2".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lpush_simple() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["lpush", "foo", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["lpush", "foo", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("world".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn rpop() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::Blob("5".into())),
            run_command(&c, &["rpop", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("4".into())])),
            run_command(&c, &["rpop", "foo", "1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("3".into()),
                Value::Blob("2".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["rpop", "foo", "55"]).await
        );

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["rpop", "foo", "55"]).await
        );

        assert_eq!(Ok(Value::Null), run_command(&c, &["rpop", "foo"]).await);

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["llen", "foobar"]).await
        );
    }

    #[tokio::test]
    async fn rpush_simple() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["rpush", "foo", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("world".into()),
                Value::Blob("hello".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lrange() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-2"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("4".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["lrange", "foo", "-2", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("3".into()),])),
            run_command(&c, &["lrange", "foo", "-3", "-3"]).await
        );
    }

    #[tokio::test]
    async fn rpush() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(10)),
            run_command(&c, &["rpush", "foo", "6", "7", "8", "9", "10"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
                Value::Blob("5".into()),
                Value::Blob("6".into()),
                Value::Blob("7".into()),
                Value::Blob("8".into()),
                Value::Blob("9".into()),
                Value::Blob("10".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn rpushx() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(10)),
            run_command(&c, &["rpushx", "foo", "6", "7", "8", "9", "10"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("1".into()),
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
                Value::Blob("5".into()),
                Value::Blob("6".into()),
                Value::Blob("7".into()),
                Value::Blob("8".into()),
                Value::Blob("9".into()),
                Value::Blob("10".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["rpushx", "foobar", "6", "7", "8", "9", "10"]).await
        );
    }
}
