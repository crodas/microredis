//! # Sorted Set command handlers
use crate::{
    connection::Connection,
    error::Error,
    value::{
        bytes_to_number, bytes_to_range_floatord,
        sorted_set::{IOption, IResult},
    },
    value::{sorted_set::SortedSet, Value},
};
use bytes::Bytes;
use float_ord::FloatOrd;
use std::collections::VecDeque;

/// Adds all the specified members with the specified scores to the sorted set
/// stored at key. It is possible to specify multiple score / member pairs. If a
/// specified member is already a member of the sorted set, the score is updated
/// and the element reinserted at the right position to ensure the correct
/// ordering.
///
/// If key does not exist, a new sorted set with the specified members as sole
/// members is created, like if the sorted set was empty. If the key exists but
/// does not hold a sorted set, an error is returned.
///
/// The score values should be the string representation of a double precision
/// floating point number. +inf and -inf values are valid values as well.
pub async fn zadd(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let option = IOption::new(&mut args)?;
    if args.is_empty() {
        return Err(Error::InvalidArgsCount("ZADD".to_owned()));
    }
    if args.len() % 2 != 0 {
        return Err(Error::Syntax);
    }
    if args.len() != 2 && option.incr {
        return Err(Error::OptsNotCompatible(
            "INCR option supports a single increment-element pair".to_owned(),
        ));
    }
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::SortedSet(x) => {
                let mut insert: usize = 0;
                let mut updated: usize = 0;

                loop {
                    let score = match args.pop_front() {
                        Some(x) => bytes_to_number::<f64>(&x)?,
                        None => break,
                    };
                    let value = args.pop_front().ok_or(Error::Syntax)?;
                    match x.insert(FloatOrd(score), value, &option) {
                        IResult::Inserted => insert += 1,
                        IResult::Updated => updated += 1,
                        _ => {}
                    }
                }

                Ok(if option.return_change {
                    updated
                } else {
                    insert
                }
                .into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            let mut x = SortedSet::new();
            let mut insert: usize = 0;
            let mut updated: usize = 0;

            loop {
                let score = match args.pop_front() {
                    Some(x) => bytes_to_number::<f64>(&x)?,
                    None => break,
                };
                let value = args.pop_front().ok_or(Error::Syntax)?;
                match x.insert(FloatOrd(score), value, &option) {
                    IResult::Inserted => insert += 1,
                    IResult::Updated => updated += 1,
                    _ => {}
                }
            }

            conn.db().set(key.clone(), x.into(), None);

            Ok(if option.return_change {
                updated
            } else {
                insert
            }
            .into())
        })?;

    conn.db().bump_version(&key);

    Ok(result)
}

/// Increments the score of member in the sorted set stored at key by increment.
/// If member does not exist in the sorted set, it is added with increment as
/// its score (as if its previous score was 0.0). If key does not exist, a new
/// sorted set with the specified member as its sole member is created.
pub async fn zincr_by(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let score = bytes_to_number::<f64>(&args.pop_front().ok_or(Error::Syntax)?)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;
    let option = IOption::incr();
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::SortedSet(x) => {
                let _ = x.insert(FloatOrd(score), value.clone(), &option);
                Ok(x.get_score(&value).unwrap_or_default().into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            #[allow(clippy::mutable_key_type)]
            let mut x = SortedSet::new();
            let _ = x.insert(FloatOrd(score), value.clone(), &option);
            Ok(x.get_score(&value).unwrap_or_default().into())
        })?;

    conn.db().bump_version(&key);
    Ok(result)
}

/// Returns the sorted set cardinality (number of elements) of the sorted set
/// stored at key.
pub async fn zcard(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::SortedSet(x) => Ok(x.len().into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))
}

/// Returns the number of elements in the sorted set at key with a score between
/// min and max.
///
/// The min and max arguments have the same semantic as described for
/// ZRANGEBYSCORE.
pub async fn zcount(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let min = bytes_to_range_floatord(&args[1])?;
    let max = bytes_to_range_floatord(&args[2])?;
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::SortedSet(x) => Ok(x.count_values_by_score_range(min, max).into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, run_command},
        error::Error,
    };

    #[tokio::test]
    async fn test_set_wrong_type() {
        let c = create_connection();

        let _ = run_command(&c, &["set", "foo", "1"]).await;

        assert_eq!(
            Err(Error::WrongType),
            run_command(&c, &["zadd", "foo", "5", "bar", "1", "foo"]).await,
        );
    }

    #[tokio::test]
    async fn test_zadd() {
        let c = create_connection();

        assert_eq!(
            Ok(2.into()),
            run_command(&c, &["zadd", "foo", "5", "bar", "1", "foo"]).await,
        );
        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["zadd", "foo", "5", "bar", "1", "foo"]).await,
        );
        assert_eq!(Ok(2.into()), run_command(&c, &["zcard", "foo"]).await,);
    }

    #[tokio::test]
    async fn test_zcount() {
        let c = create_connection();

        assert_eq!(
            Ok(3.into()),
            run_command(
                &c,
                &["zadd", "foo", "5", "bar", "1", "foo", "5.9", "foobar"]
            )
            .await,
        );
        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["zadd", "foo", "nx", "511", "bar", "10", "foo"]).await,
        );
        assert_eq!(
            Ok(2.into()),
            run_command(&c, &["zcount", "foo", "1", "5"]).await,
        );
        assert_eq!(
            Ok(1.into()),
            run_command(&c, &["zcount", "foo", "1", "(5"]).await,
        );
        assert_eq!(
            Ok(0.into()),
            run_command(&c, &["zcount", "foo", "(1", "(5"]).await,
        );
        assert_eq!(
            Ok(3.into()),
            run_command(&c, &["zcount", "foo", "-inf", "+inf"]).await,
        );
    }
}
