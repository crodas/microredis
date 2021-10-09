use crate::{connection::Connection, error::Error, value::checksum, value::Value};
use bytes::Bytes;
use std::collections::HashSet;

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

pub async fn sismember(conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    conn.db().get_map_or(
        &args[1],
        |v| match v {
            Value::Set(x) => {
                if x.read()
                    .get(&checksum::Value::new(args[2].clone()))
                    .is_some()
                {
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
