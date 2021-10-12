use crate::{connection::Connection, error::Error, value::checksum, value::Value};
use bytes::Bytes;
use std::collections::HashSet;

async fn compare_sets<F1>(conn: &Connection, keys: &[Bytes], op: F1) -> Result<Value, Error>
where
    F1: Fn(&mut HashSet<checksum::Value>, &HashSet<checksum::Value>) -> bool,
{
    conn.db().get_map_or(
        &keys[0],
        |v| match v {
            Value::Set(x) => {
                #[allow(clippy::mutable_key_type)]
                let mut all_entries = x.read().clone();
                for key in keys[1..].iter() {
                    let mut do_break = false;
                    let _ = conn.db().get_map_or(
                        key,
                        |v| match v {
                            Value::Set(x) => {
                                if !op(&mut all_entries, &x.read()) {
                                    do_break = true;
                                }
                                Ok(Value::Null)
                            }
                            _ => Err(Error::WrongType),
                        },
                        || Ok(Value::Null),
                    )?;
                    if do_break {
                        break;
                    }
                }

                Ok(all_entries
                    .iter()
                    .map(|entry| entry.clone_value())
                    .collect::<Vec<Value>>()
                    .into())
            }
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

pub async fn sadd(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Set(x) => {
                let mut x = x.write();

                let mut len = 0;

                for val in (&args[2..]).iter() {
                    if x.insert(checksum::Value::new(val.clone())) {
                        len += 1;
                    }
                }

                Ok(len.into())
            }
            _ => Err(Error::WrongType),
        },
        || {
            #[allow(clippy::mutable_key_type)]
            let mut x = HashSet::new();
            let mut len = 0;

            for val in (&args[2..]).iter() {
                if x.insert(checksum::Value::new(val.clone())) {
                    len += 1;
                }
            }

            conn.db().set(&args[1], x.into(), None);

            Ok(len.into())
        },
    )
}

pub async fn scard(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Set(x) => Ok((x.read().len() as i64).into()),
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

pub async fn sdiff(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    compare_sets(conn, &args[1..], |all_entries, elements| {
        for element in elements.iter() {
            if all_entries.contains(element) {
                all_entries.remove(element);
            }
        }
        true
    })
    .await
}

pub async fn sdiffstore(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    if let Value::Array(values) = sdiff(conn, &args[1..]).await? {
        #[allow(clippy::mutable_key_type)]
        let mut x = HashSet::new();
        let mut len = 0;

        for val in values.iter() {
            if let Value::Blob(blob) = val {
                if x.insert(checksum::Value::new(blob.clone())) {
                    len += 1;
                }
            }
        }

        conn.db().set(&args[1], x.into(), None);

        Ok(len.into())
    } else {
        Ok(0.into())
    }
}

pub async fn sinter(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    compare_sets(conn, &args[1..], |all_entries, elements| {
        all_entries.retain(|element| elements.contains(element));

        for element in elements.iter() {
            if !all_entries.contains(element) {
                all_entries.remove(element);
            }
        }

        !all_entries.is_empty()
    })
    .await
}

pub async fn sintercard(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    if let Ok(Value::Array(x)) = sinter(conn, args).await {
        Ok((x.len() as i64).into())
    } else {
        Ok(0.into())
    }
}

pub async fn sinterstore(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    if let Value::Array(values) = sinter(conn, &args[1..]).await? {
        #[allow(clippy::mutable_key_type)]
        let mut x = HashSet::new();
        let mut len = 0;

        for val in values.iter() {
            if let Value::Blob(blob) = val {
                if x.insert(checksum::Value::new(blob.clone())) {
                    len += 1;
                }
            }
        }

        conn.db().set(&args[1], x.into(), None);

        Ok(len.into())
    } else {
        Ok(0.into())
    }
}

pub async fn sismember(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Set(x) => {
                if x.read().contains(&checksum::Value::new(args[2].clone())) {
                    Ok(1.into())
                } else {
                    Ok(0.into())
                }
            }
            _ => Err(Error::WrongType),
        },
        || Ok(0.into()),
    )
}

pub async fn smembers(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Set(x) => Ok(x
                .read()
                .iter()
                .map(|x| x.clone_value())
                .collect::<Vec<Value>>()
                .into()),
            _ => Err(Error::WrongType),
        },
        || Ok(Value::Array(vec![])),
    )
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
        value::Value,
    };

    #[tokio::test]
    async fn test_set_wrong_type() {
        let c = create_connection();

        let _ = run_command(&c, &["set", "foo", "1"]).await;

        assert_eq!(
            Err(Error::WrongType),
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "5"]).await,
        );
    }

    #[tokio::test]
    async fn sadd() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "6"]).await,
        );
    }

    #[tokio::test]
    async fn scard() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "5"]).await,
            run_command(&c, &["scard", "foo"]).await
        );
    }

    #[tokio::test]
    async fn sdiff() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "2", "c"]).await,
            run_command(&c, &["scard", "2"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "3", "a", "c", "e"]).await,
            run_command(&c, &["scard", "3"]).await
        );

        match run_command(&c, &["sdiff", "1", "2", "3"]).await {
            Ok(Value::Array(v)) => {
                assert_eq!(2, v.len());
                if v[0] == Value::Blob("b".into()) {
                    assert_eq!(v[1], Value::Blob("d".into()));
                } else {
                    assert_eq!(v[1], Value::Blob("b".into()));
                }
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn sdiffstore() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "2", "c"]).await,
            run_command(&c, &["scard", "2"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "3", "a", "c", "e"]).await,
            run_command(&c, &["scard", "3"]).await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["sdiffstore", "4", "1", "2", "3"]).await
        );

        match run_command(&c, &["smembers", "4"]).await {
            Ok(Value::Array(v)) => {
                assert_eq!(2, v.len());
                if v[0] == Value::Blob("b".into()) {
                    assert_eq!(v[1], Value::Blob("d".into()));
                } else {
                    assert_eq!(v[1], Value::Blob("b".into()));
                }
            }
            _ => unreachable!(),
        };
    }

    #[tokio::test]
    async fn sinter() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "2", "c", "x"]).await,
            run_command(&c, &["scard", "2"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "3", "a", "c", "e"]).await,
            run_command(&c, &["scard", "3"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("c".into())])),
            run_command(&c, &["sinter", "1", "2", "3"]).await
        );
    }

    #[tokio::test]
    async fn sintercard() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "2", "c", "x"]).await,
            run_command(&c, &["scard", "2"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "3", "a", "c", "e"]).await,
            run_command(&c, &["scard", "3"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["sintercard", "1", "2", "3"]).await
        );
    }

    #[tokio::test]
    async fn sinterstore() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "2", "c", "x"]).await,
            run_command(&c, &["scard", "2"]).await
        );

        assert_eq!(
            run_command(&c, &["sadd", "3", "a", "c", "e"]).await,
            run_command(&c, &["scard", "3"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["sinterstore", "foo", "1", "2", "3"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("c".into())])),
            run_command(&c, &["smembers", "foo"]).await
        );
    }

    #[tokio::test]
    async fn sismember() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "5"]).await,
            run_command(&c, &["scard", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["sismember", "foo", "5"]).await
        );

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["sismember", "foo", "6"]).await
        );

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["sismember", "foobar", "5"]).await
        );
    }
}
