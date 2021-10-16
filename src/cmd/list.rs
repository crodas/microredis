use crate::{
    check_arg, connection::Connection, error::Error, value::bytes_to_number, value::checksum,
    value::Value,
};
use bytes::Bytes;
use std::collections::VecDeque;
use tokio::time::{sleep, Duration, Instant};

#[allow(clippy::needless_range_loop)]
fn remove_element(
    conn: &Connection,
    key: &Bytes,
    count: usize,
    front: bool,
) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
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
    )?;

    conn.db().bump_version(key);

    Ok(result)
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

pub async fn lindex(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut index: i64 = bytes_to_number(&args[2])?;
                let x = x.read();

                if index < 0 {
                    index += x.len() as i64;
                }

                Ok(x.get(index as usize)
                    .map_or(Value::Null, |x| x.clone_value()))
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

pub async fn linsert(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_before = if check_arg!(args, 2, "BEFORE") {
        true
    } else if check_arg!(args, 2, "AFTER") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let pivot = checksum::Ref::new(&args[3]);
                let mut x = x.write();
                let mut found = false;

                for (key, val) in x.iter().enumerate() {
                    if *val == pivot {
                        let id = if is_before { key } else { key + 1 };

                        let value = checksum::Value::new(args[4].clone());

                        if id > x.len() {
                            x.push_back(value);
                        } else {
                            x.insert(id as usize, value);
                        }

                        found = true;
                        break;
                    }
                }

                if found {
                    Ok((x.len() as i64).into())
                } else {
                    Ok((-1).into())
                }
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
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

pub async fn lmove(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let source_is_left = if check_arg!(args, 3, "LEFT") {
        true
    } else if check_arg!(args, 3, "RIGHT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let target_is_left = if check_arg!(args, 4, "LEFT") {
        true
    } else if check_arg!(args, 4, "RIGHT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(source) => conn.db().get_map_or(
                &args[2],
                |v| match v {
                    Value::List(target) => {
                        let element = if source_is_left {
                            source.write().pop_front()
                        } else {
                            source.write().pop_back()
                        };

                        if let Some(element) = element {
                            let ret = element.clone_value();
                            if target_is_left {
                                target.write().push_front(element);
                            } else {
                                target.write().push_back(element);
                            }
                            Ok(ret)
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Err(Error::WrongType),
                },
                || {
                    let element = if source_is_left {
                        source.write().pop_front()
                    } else {
                        source.write().pop_back()
                    };

                    if let Some(element) = element {
                        let ret = element.clone_value();
                        let mut h = VecDeque::new();
                        h.push_front(element);
                        conn.db().set(&args[2], h.into(), None);
                        Ok(ret)
                    } else {
                        Ok(Value::Null)
                    }
                },
            ),
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Null),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

pub async fn lpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let count = if args.len() > 2 {
        bytes_to_number(&args[2])?
    } else {
        0
    };

    remove_element(conn, &args[1], count, true)
}

pub async fn lpos(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let mut index = 3;
    let element = checksum::Ref::new(&args[2]);
    let rank = if check_arg!(args, index, "RANK") {
        index += 2;
        Some(bytes_to_number::<usize>(&args[index - 1])?)
    } else {
        None
    };
    let count = if check_arg!(args, index, "COUNT") {
        index += 2;
        Some(bytes_to_number::<usize>(&args[index - 1])?)
    } else {
        None
    };
    let max_len = if check_arg!(args, index, "MAXLEN") {
        index += 2;
        bytes_to_number::<i64>(&args[index - 1])?
    } else {
        -1
    };

    if index != args.len() {
        return Err(Error::Syntax);
    }

    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let x = x.read();
                let mut ret: Vec<Value> = vec![];

                for (i, val) in x.iter().enumerate() {
                    if *val == element {
                        // Match!
                        if let Some(count) = count {
                            ret.push((i as i64).into());
                            if ret.len() > count {
                                return Ok(ret.into());
                            }
                        } else if let Some(rank) = rank {
                            ret.push((i as i64).into());
                            if ret.len() == rank {
                                return Ok(ret[rank - 1].clone());
                            }
                        } else {
                            // return first match!
                            return Ok((i as i64).into());
                        }
                    }
                    if (i as i64) == max_len {
                        break;
                    }
                }

                if count.is_some() {
                    Ok(ret.into())
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::WrongType),
        },
        || {
            Ok(if count.is_some() {
                Value::Array(vec![])
            } else {
                Value::Null
            })
        },
    )
}

pub async fn lpush(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_push_x = check_arg!(args, 0, "LPUSHX");

    let result = conn.db().get_map_or(
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
            let mut h = VecDeque::new();

            for val in args.iter().skip(2) {
                h.push_front(checksum::Value::new(val.clone()));
            }

            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
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

pub async fn lrem(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let element = checksum::Ref::new(&args[3]);
                let limit: i64 = bytes_to_number(&args[2])?;
                let mut x = x.write();

                let (is_reverse, limit) = if limit < 0 {
                    (true, -limit)
                } else {
                    (false, limit)
                };

                let mut keep = vec![true; x.len()];
                let mut removed = 0;
                let len = x.len();

                for i in 0..len {
                    let i = if is_reverse { len - 1 - i } else { i };

                    if let Some(value) = x.get(i) {
                        if *value == element {
                            keep[i] = false;
                            removed += 1;
                            if removed == limit {
                                break;
                            }
                        }
                    }
                }

                let mut i = 0;
                x.retain(|_| {
                    i += 1;
                    keep[i - 1]
                });

                Ok(removed.into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)

}

pub async fn lset(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut index: i64 = bytes_to_number(&args[2])?;
                let mut x = x.write();

                if index < 0 {
                    index += x.len() as i64;
                }

                if let Some(x) = x.get_mut(index as usize) {
                    *x = checksum::Value::new(args[3].clone());
                    Ok(Value::OK)
                } else {
                    Err(Error::OutOfRange)
                }
            }
            _ => Err(Error::WrongType),
        },
        || Err(Error::NotFound),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

pub async fn ltrim(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let result = conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::List(x) => {
                let mut start: i64 = bytes_to_number(&args[2])?;
                let mut end: i64 = bytes_to_number(&args[3])?;
                let mut x = x.write();

                if start < 0 {
                    start += x.len() as i64;
                }

                if end < 0 {
                    end += x.len() as i64;
                }

                let mut i = 0;
                x.retain(|_| {
                    let retain = i >= start && i <= end;
                    i += 1;
                    retain
                });

                Ok(Value::OK)
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::OK),
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

pub async fn rpop(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let count = if args.len() > 2 {
        bytes_to_number(&args[2])?
    } else {
        0
    };

    remove_element(conn, &args[1], count, false)
}

pub async fn rpoplpush(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    lmove(
        conn,
        &[
            "lmove".into(),
            args[1].clone(),
            args[2].clone(),
            "RIGHT".into(),
            "LEFT".into(),
        ],
    )
    .await
}

pub async fn rpush(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    let is_push_x = check_arg!(args, 0, "RPUSHX");

    let result = conn.db().get_map_or(
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
            let mut h = VecDeque::new();

            for val in args.iter().skip(2) {
                h.push_back(checksum::Value::new(val.clone()));
            }

            let len = h.len() as i64;
            conn.db().set(&args[1], h.into(), None);
            Ok(len.into())
        },
    )?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };
    use tokio::time::{sleep, Duration, Instant};

    #[tokio::test]
    async fn blpop_no_waiting() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["blpop", "foo", "1"]).await
        );
    }

    #[tokio::test]
    async fn blpop_timeout() {
        let c = create_connection();
        let x = Instant::now();

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["blpop", "foobar", "1"]).await
        );

        assert!(Instant::now() - x > Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn blpop_wait_insert() {
        let c = create_connection();
        let x = Instant::now();

        // Query command that will block connection until some data is inserted
        // to foobar, foo, bar or the 5 seconds timeout happens.
        //
        // We are issuing the command, sleeping a little bit then adding data to
        // foobar, before actually waiting on the result.
        let waiting = run_command(&c, &["blpop", "foobar", "foo", "bar", "5"]);

        // Sleep 1 second before inserting new data
        sleep(Duration::from_millis(1000)).await;

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        // Read the output of the first blpop command now.
        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            waiting.await
        );

        assert!(Instant::now() - x > Duration::from_millis(1000));
        assert!(Instant::now() - x < Duration::from_millis(5000));
    }

    #[tokio::test]
    async fn lrem_1() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(
                &c,
                &["rpush", "mylist", "hello", "hello", "world", "hello", "hello"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["lrem", "mylist", "3", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("world".into()),
                Value::Blob("hello".into()),
            ])),
            run_command(&c, &["lrange", "mylist", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lrem_2() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(
                &c,
                &["rpush", "mylist", "hello", "hello", "world", "hello", "hello"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["lrem", "mylist", "-2", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("hello".into()),
                Value::Blob("world".into()),
            ])),
            run_command(&c, &["lrange", "mylist", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["lrem", "mylist", "1", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("world".into()),
            ])),
            run_command(&c, &["lrange", "mylist", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lrem_3() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(
                &c,
                &["rpush", "mylist", "hello", "hello", "world", "hello", "hello"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(4)),
            run_command(&c, &["lrem", "mylist", "-100", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("world".into()),])),
            run_command(&c, &["lrange", "mylist", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lrem_4() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(
                &c,
                &["rpush", "mylist", "hello", "hello", "world", "hello", "hello"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(4)),
            run_command(&c, &["lrem", "mylist", "100", "hello"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("world".into()),])),
            run_command(&c, &["lrange", "mylist", "0", "-1"]).await
        );
    }

    #[tokio::test]
    async fn brpop_no_waiting() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            run_command(&c, &["brpop", "foo", "1"]).await
        );
    }

    #[tokio::test]
    async fn brpop_timeout() {
        let c = create_connection();
        let x = Instant::now();

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["brpop", "foobar", "1"]).await
        );

        assert!(Instant::now() - x > Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn brpop_wait_insert() {
        let c = create_connection();
        let x = Instant::now();

        // Query command that will block connection until some data is inserted
        // to foobar, foo, bar or the 5 seconds timeout happens.
        //
        // We are issuing the command, sleeping a little bit then adding data to
        // foobar, before actually waiting on the result.
        let waiting = run_command(&c, &["brpop", "foobar", "foo", "bar", "5"]);

        // Sleep 1 second before inserting new data
        sleep(Duration::from_millis(1000)).await;

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        // Read the output of the first blpop command now.
        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            waiting.await
        );

        assert!(Instant::now() - x > Duration::from_millis(1000));
        assert!(Instant::now() - x < Duration::from_millis(5000));
    }

    #[tokio::test]
    async fn lindex() {
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
            Ok(Value::Blob("5".into())),
            run_command(&c, &["lindex", "foo", "0"]).await
        );

        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["lindex", "foo", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["lindex", "foo", "-100"]).await
        );

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["lindex", "foo", "100"]).await
        );
    }

    #[tokio::test]
    async fn linsert_syntax_err() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello", "world"]).await
        );

        assert_eq!(
            Err(Error::Syntax),
            run_command(&c, &["linsert", "foo", "beforex", "world", "there"]).await
        );
    }

    #[tokio::test]
    async fn linsert_before() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["linsert", "foo", "before", "world", "there"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("there".into()),
                Value::Blob("world".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await,
        );
    }

    #[tokio::test]
    async fn linsert_after() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["linsert", "foo", "after", "world", "there"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("world".into()),
                Value::Blob("there".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await,
        );
    }

    #[tokio::test]
    async fn linsert_before_after() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["linsert", "foo", "after", "world", "there1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(4)),
            run_command(&c, &["linsert", "foo", "before", "world", "there2"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("hello".into()),
                Value::Blob("there2".into()),
                Value::Blob("world".into()),
                Value::Blob("there1".into()),
            ])),
            run_command(&c, &["lrange", "foo", "0", "-1"]).await,
        );
    }

    #[tokio::test]
    async fn linsert_not_found() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["rpush", "foo", "hello", "world"]).await
        );

        assert_eq!(
            Ok(Value::Integer(-1)),
            run_command(&c, &["linsert", "foo", "after", "worldx", "there"]).await
        );

        assert_eq!(
            Ok(Value::Integer(-1)),
            run_command(&c, &["linsert", "foo", "before", "worldx", "there"]).await
        );
    }

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
    async fn lmove_1() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await,
            run_command(&c, &["llen", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Blob("1".into())),
            run_command(&c, &["lmove", "foo", "bar", "left", "left"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("1".into()),])),
            run_command(&c, &["lrange", "bar", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Blob("5".into())),
            run_command(&c, &["lmove", "foo", "bar", "right", "left"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("5".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["lrange", "bar", "0", "-1"]).await
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
    async fn lpos_single_match() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(11)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(6)),
            run_command(&c, &["lpos", "mylist", "3"]).await
        );
    }

    #[tokio::test]
    async fn lpos_single_skip() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(11)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(&c, &["lpos", "mylist", "3", "rank", "2"]).await
        );
    }

    #[tokio::test]
    async fn lpos_single_skip_max_len() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(11)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["lpos", "mylist", "3", "rank", "2", "maxlen", "7"]).await
        );
    }

    #[tokio::test]
    async fn lpos_not_found() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Array(vec![])),
            run_command(&c, &["lpos", "mylist", "3", "count", "5", "maxlen", "9"]).await
        );
        assert_eq!(
            Ok(Value::Null),
            run_command(&c, &["lpos", "mylist", "3"]).await
        );
    }

    #[tokio::test]
    async fn lpos() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(11)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Integer(6),
                Value::Integer(8),
                Value::Integer(9),
            ])),
            run_command(&c, &["lpos", "mylist", "3", "count", "5", "maxlen", "9"]).await
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
    async fn lset() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::OK),
            run_command(&c, &["lset", "foo", "-1", "6"]).await,
        );

        assert_eq!(
            Ok(Value::OK),
            run_command(&c, &["lset", "foo", "-2", "7"]).await,
        );

        assert_eq!(
            Ok(Value::OK),
            run_command(&c, &["lset", "foo", "0", "8"]).await,
        );

        assert_eq!(
            Err(Error::OutOfRange),
            run_command(&c, &["lset", "foo", "55", "8"]).await,
        );

        assert_eq!(
            Err(Error::OutOfRange),
            run_command(&c, &["lset", "foo", "-55", "8"]).await,
        );

        assert_eq!(
            Err(Error::NotFound),
            run_command(&c, &["lset", "key_not_exists", "-55", "8"]).await,
        );

        assert_eq!(
            Ok(Value::Blob("6".into())),
            run_command(&c, &["rpop", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Blob("7".into())),
            run_command(&c, &["rpop", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Blob("8".into())),
            run_command(&c, &["lpop", "foo"]).await
        );
    }

    #[tokio::test]
    async fn ltrim() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await
        );

        assert_eq!(
            Ok(Value::OK),
            run_command(&c, &["ltrim", "foo", "1", "-2"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("2".into()),
                Value::Blob("3".into()),
                Value::Blob("4".into()),
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
