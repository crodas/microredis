//! # Set command handlers
use crate::{connection::Connection, error::Error, value::bytes_to_number, value::Value};
use bytes::Bytes;
use rand::Rng;
use std::{
    cmp::min,
    collections::{HashSet, VecDeque},
};

fn store_key_values(conn: &Connection, key: Bytes, values: Vec<Value>) -> i64 {
    #[allow(clippy::mutable_key_type)]
    let mut x = HashSet::new();
    let mut len = 0;

    for val in values.into_iter() {
        if let Value::Blob(blob) = val {
            if x.insert(blob) {
                len += 1;
            }
        }
    }
    conn.db().set(key, x.into(), None);
    len
}

async fn compare_sets<F1>(
    conn: &Connection,
    mut keys: VecDeque<Bytes>,
    op: F1,
) -> Result<Value, Error>
where
    F1: Fn(&mut HashSet<Bytes>, &HashSet<Bytes>) -> bool,
{
    let top_key = keys.pop_front().ok_or(Error::Syntax)?;
    conn.db()
        .get(&top_key)
        .map(|v| match v {
            Value::Set(x) => {
                #[allow(clippy::mutable_key_type)]
                let mut all_entries = x.clone();
                for key in keys.iter() {
                    let mut do_break = false;
                    let mut found = false;
                    let _ = conn
                        .db()
                        .get(key)
                        .map(|v| match v {
                            Value::Set(x) => {
                                found = true;
                                if !op(&mut all_entries, x) {
                                    do_break = true;
                                }
                                Ok(Value::Null)
                            }
                            _ => Err(Error::WrongType),
                        })
                        .unwrap_or(Ok(Value::Null))?;
                    if !found && !op(&mut all_entries, &HashSet::new()) {
                        break;
                    }
                    if do_break {
                        break;
                    }
                }

                Ok(all_entries
                    .iter()
                    .map(|entry| Value::new(entry))
                    .collect::<Vec<Value>>()
                    .into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            #[allow(clippy::mutable_key_type)]
            let mut all_entries: HashSet<Bytes> = HashSet::new();
            for key in keys.iter() {
                let mut do_break = false;
                let _ = conn
                    .db()
                    .get(key)
                    .map(|v| match v {
                        Value::Set(x) => {
                            if !op(&mut all_entries, x) {
                                do_break = true;
                            }
                            Ok(Value::Null)
                        }
                        _ => Err(Error::WrongType),
                    })
                    .unwrap_or(Ok(Value::Null))?;
                if do_break {
                    break;
                }
            }

            Ok(all_entries
                .iter()
                .map(|entry| Value::new(entry))
                .collect::<Vec<Value>>()
                .into())
        })
}

/// Add the specified members to the set stored at key. Specified members that are already a member
/// of this set are ignored. If key does not exist, a new set is created before adding the
/// specified members.
pub async fn sadd(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let key_for_not_found = key.clone();
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::Set(x) => {
                let mut len = 0;

                for val in args.clone().into_iter() {
                    if x.insert(val) {
                        len += 1;
                    }
                }

                Ok(len.into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            #[allow(clippy::mutable_key_type)]
            let mut x = HashSet::new();
            let mut len = 0;

            for val in args.into_iter() {
                if x.insert(val) {
                    len += 1;
                }
            }

            conn.db().set(key_for_not_found, x.into(), None);
            Ok(len.into())
        })?;

    conn.db().bump_version(&key);

    Ok(result)
}

/// Returns the set cardinality (number of elements) of the set stored at key.
pub async fn scard(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::Set(x) => Ok(x.len().into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))
}

/// Returns the members of the set resulting from the difference between the first set and all the
/// successive sets.
///
/// Keys that do not exist are considered to be empty sets.
pub async fn sdiff(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    compare_sets(conn, args, |all_entries, elements| {
        for element in elements.iter() {
            if all_entries.contains(element) {
                all_entries.remove(element);
            }
        }
        true
    })
    .await
}

/// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in
/// destination.
///
/// If destination already exists, it is overwritten.
pub async fn sdiffstore(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key_name = args.pop_front().ok_or(Error::Syntax)?;
    if let Value::Array(values) = sdiff(conn, args).await? {
        if !values.is_empty() {
            Ok(store_key_values(conn, key_name, values).into())
        } else {
            let _ = conn.db().del(&[key_name]);
            Ok(0.into())
        }
    } else {
        Ok(0.into())
    }
}

/// Returns the members of the set resulting from the intersection of all the given sets.
///
/// Keys that do not exist are considered to be empty sets. With one of the keys being an empty
/// set, the resulting set is also empty (since set intersection with an empty set always results
/// in an empty set).
pub async fn sinter(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    compare_sets(conn, args, |all_entries, elements| {
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

/// This command is similar to SINTER, but instead of returning the result set, it returns just the
/// cardinality of the result. Returns the cardinality of the set which would result from the
/// intersection of all the given sets.
///
/// Keys that do not exist are considered to be empty sets. With one of the keys being an empty
/// set, the resulting set is also empty (since set intersection with an empty set always results
/// in an empty set).
pub async fn sintercard(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    if let Ok(Value::Array(x)) = sinter(conn, args).await {
        Ok(x.len().into())
    } else {
        Ok(0.into())
    }
}

/// This command is equal to SINTER, but instead of returning the resulting set, it is stored in
/// destination.
///
/// If destination already exists, it is overwritten.
pub async fn sinterstore(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key_name = args.pop_front().ok_or(Error::Syntax)?;
    if let Value::Array(values) = sinter(conn, args).await? {
        if !values.is_empty() {
            Ok(store_key_values(conn, key_name, values).into())
        } else {
            let _ = conn.db().del(&[key_name]);
            Ok(0.into())
        }
    } else {
        Ok(0.into())
    }
}

/// Returns if member is a member of the set stored at key.
pub async fn sismember(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::Set(x) => {
                if x.contains(&args[1]) {
                    Ok(1.into())
                } else {
                    Ok(0.into())
                }
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))
}

/// Returns all the members of the set value stored at key.
///
/// This has the same effect as running SINTER with one argument key.
pub async fn smembers(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::Set(x) => Ok(x
                .iter()
                .map(|x| Value::new(x))
                .collect::<Vec<Value>>()
                .into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Array(vec![])))
}

/// Returns whether each member is a member of the set stored at key.
///
/// For every member, 1 is returned if the value is a member of the set, or 0 if the element is not
/// a member of the set or if key does not exist.
pub async fn smismember(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    conn.db()
        .get(&key)
        .map(|v| match v {
            Value::Set(x) => Ok(args
                .iter()
                .map(|member| if x.contains(member) { 1 } else { 0 })
                .collect::<Vec<i32>>()
                .into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| Ok(args.iter().map(|_| 0.into()).collect::<Vec<Value>>().into()))
}

/// Move member from the set at source to the set at destination. This operation is atomic. In
/// every given moment the element will appear to be a member of source or destination for other
/// clients.
///
/// If the source set does not exist or does not contain the specified element, no operation is
/// performed and 0 is returned. Otherwise, the element is removed from the source set and added to
/// the destination set. When the specified element already exists in the destination set, it is
/// only removed from the source set.
///
/// TODO: FIXME: This implementation is flaky. It should be rewritten to use a new db
/// method that allows to return multiple keys, even if they are stored in the
/// same bucked. Right now, this can block a connection
pub async fn smove(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let source = args.pop_front().ok_or(Error::Syntax)?;
    let destination = args.pop_front().ok_or(Error::Syntax)?;
    let member = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&source)
        .map_mut(|v| match v {
            Value::Set(set1) => conn
                .db()
                .get(&destination)
                .map_mut(|v| match v {
                    Value::Set(set2) => {
                        if !set1.contains(&member) {
                            return Ok(0.into());
                        }

                        if source == destination {
                            return Ok(1.into());
                        }

                        set1.remove(&member);
                        if set2.insert(member.clone()) {
                            Ok(1.into())
                        } else {
                            Ok(0.into())
                        }
                    }
                    _ => Err(Error::WrongType),
                })
                .unwrap_or_else(|| {
                    set1.remove(&member);
                    #[allow(clippy::mutable_key_type)]
                    let mut x = HashSet::new();
                    x.insert(member.clone());
                    conn.db().set(destination.clone(), x.into(), None);
                    Ok(1.into())
                }),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))?;

    if result == Value::Integer(1) {
        conn.db().bump_version(&source);
        conn.db().bump_version(&destination);
    }

    Ok(result)
}

/// Removes and returns one or more random members from the set value store at key.
///
/// This operation is similar to SRANDMEMBER, that returns one or more random elements from a set
/// but does not remove it.
///
/// By default, the command pops a single member from the set. When provided with the optional
/// count argument, the reply will consist of up to count members, depending on the set's
/// cardinality.
pub async fn spop(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let rand = srandmember(conn, args.clone()).await?;
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let mut should_remove = false;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::Set(x) => {
                match &rand {
                    Value::Blob(value) => {
                        x.remove(value.as_ref());
                    }
                    Value::Array(values) => {
                        for value in values.iter() {
                            if let Value::Blob(value) = value {
                                x.remove(value.as_ref());
                            }
                        }
                    }
                    _ => unreachable!(),
                };

                should_remove = x.is_empty();
                Ok(rand)
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Null))?;

    if should_remove {
        let _ = conn.db().del(&[key]);
    } else {
        conn.db().bump_version(&key);
    }

    Ok(result)
}

/// When called with just the key argument, return a random element from the set value stored at
/// key.
///
/// If the provided count argument is positive, return an array of distinct elements. The array's
/// length is either count or the set's cardinality (SCARD), whichever is lower.
///
/// If called with a negative count, the behavior changes and the command is allowed to return the
/// same element multiple times. In this case, the number of returned elements is the absolute
/// value of the specified count.
pub async fn srandmember(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::Set(set) => {
                let mut rng = rand::thread_rng();

                let mut items = set
                    .iter()
                    .map(|x| (x, rng.gen()))
                    .collect::<Vec<(&Bytes, i128)>>();

                items.sort_by(|a, b| a.1.cmp(&b.1));

                if args.len() == 1 {
                    // Two arguments provided, return the first element or null if the array is null
                    if items.is_empty() {
                        Ok(Value::Null)
                    } else {
                        let item = items[0].0.clone();
                        Ok(Value::new(&item))
                    }
                } else {
                    if items.is_empty() {
                        return Ok(Value::Array(vec![]));
                    }
                    let len = bytes_to_number::<i64>(&args[1])?;

                    if len > 0 {
                        // required length is positive, return *up* to the requested number and no duplicated allowed
                        let len: usize = min(items.len(), len as usize);
                        Ok(items[0..len]
                            .iter()
                            .map(|item| Value::new(item.0))
                            .collect::<Vec<Value>>()
                            .into())
                    } else {
                        // duplicated results are allowed and the requested number must be returned
                        let len = -len as usize;
                        let total = items.len() - 1;
                        let mut i = 0;
                        let items = (0..len)
                            .map(|_| {
                                let r = (items[i].0, rng.gen());
                                i = if i >= total { 0 } else { i + 1 };
                                r
                            })
                            .collect::<Vec<(&Bytes, i128)>>();
                        Ok(items
                            .iter()
                            .map(|item| Value::new(item.0))
                            .collect::<Vec<Value>>()
                            .into())
                    }
                }
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(if args.len() == 1 {
            Value::Null
        } else {
            Value::Array(vec![])
        }))
}

/// Remove the specified members from the set stored at key. Specified members that are not a
/// member of this set are ignored. If key does not exist, it is treated as an empty set and this
/// command returns 0.
pub async fn srem(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::Set(set) => {
                let mut i = 0;

                args.into_iter().for_each(|value| {
                    if set.remove(&value) {
                        i += 1;
                    }
                });

                Ok(i.into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))?;

    conn.db().bump_version(&key);

    Ok(result)
}

/// Returns the members of the set resulting from the union of all the given sets.
pub async fn sunion(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    compare_sets(conn, args, |all_entries, elements| {
        for element in elements.iter() {
            all_entries.insert(element.clone());
        }

        true
    })
    .await
}

/// This command is equal to SUNION, but instead of returning the resulting set, it is stored in
/// destination.
///
/// If destination already exists, it is overwritten.
pub async fn sunionstore(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key_name = args.pop_front().ok_or(Error::Syntax)?;
    if let Value::Array(values) = sunion(conn, args).await? {
        if !values.is_empty() {
            Ok(store_key_values(conn, key_name, values).into())
        } else {
            let _ = conn.db().del(&[key_name]);
            Ok(0.into())
        }
    } else {
        Ok(0.into())
    }
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

    #[tokio::test]
    async fn smismember() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "foo", "1", "2", "3", "4", "5", "5"]).await,
            run_command(&c, &["scard", "foo"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(1),
            ])),
            run_command(&c, &["smismember", "foo", "5", "6", "3"]).await
        );
    }

    #[tokio::test]
    async fn smove() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["smove", "1", "2", "d"]).await
        );

        assert_eq!(
            Ok(Value::Integer(0)),
            run_command(&c, &["smove", "1", "2", "f"]).await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["scard", "2"]).await
        );
    }

    #[tokio::test]
    async fn spop() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        let _ = run_command(&c, &["spop", "1"]).await;

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["scard", "1"]).await
        );

        if let Ok(Value::Array(x)) = run_command(&c, &["spop", "1", "2"]).await {
            assert_eq!(2, x.len());
        }

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["scard", "1"]).await
        );
    }

    #[tokio::test]
    async fn srem() {
        let c = create_connection();

        assert_eq!(
            run_command(&c, &["sadd", "1", "a", "b", "c", "d"]).await,
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["srem", "1", "b"]).await
        );

        assert_eq!(
            Ok(Value::Integer(3)),
            run_command(&c, &["scard", "1"]).await
        );

        assert_eq!(
            Ok(Value::Integer(2)),
            run_command(&c, &["srem", "1", "a", "b", "c"]).await
        );

        assert_eq!(
            Ok(Value::Integer(1)),
            run_command(&c, &["scard", "1"]).await
        );
    }

    #[tokio::test]
    async fn sunion() {
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
            6,
            if let Ok(Value::Array(x)) = run_command(&c, &["sunion", "1", "2", "3"]).await {
                x.len()
            } else {
                0
            }
        );
    }

    #[tokio::test]
    async fn sunion_first_key_do_not_exists() {
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
            6,
            if let Ok(Value::Array(x)) = run_command(&c, &["sunion", "0", "1", "2", "3"]).await {
                x.len()
            } else {
                0
            }
        );
    }
}
