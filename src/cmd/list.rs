//! # List command handlers
use crate::{
    check_arg,
    connection::{Connection, UnblockReason},
    db::utils::far_future,
    error::Error,
    try_get_arg, try_get_arg_str,
    value::bytes_to_number,
    value::checksum,
    value::Value,
};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use std::{collections::VecDeque, sync::Arc};
use tokio::{
    sync::broadcast::{self, Receiver},
    time::{sleep, Duration, Instant},
};

#[allow(clippy::needless_range_loop)]
/// Removes an element from a list
#[inline]
fn remove_element(
    conn: &Connection,
    key: &Bytes,
    limit: Option<usize>,
    front: bool,
) -> Result<Value, Error> {
    let db = conn.db();
    let mut new_len = 0;
    let result = db
        .get(key)
        .map_mut(|v| match v {
            Value::List(x) => {
                let limit = if let Some(limit) = limit {
                    limit
                } else {
                    // Return a single element
                    let ret = Ok((if front { x.pop_front() } else { x.pop_back() })
                        .map_or(Value::Null, |x| x.clone_value()));
                    new_len = x.len();
                    return ret;
                };

                let mut ret = vec![None; limit];

                for i in 0..limit {
                    if front {
                        ret[i] = x.pop_front();
                    } else {
                        ret[i] = x.pop_back();
                    }
                }
                new_len = x.len();

                Ok(ret
                    .iter()
                    .flatten()
                    .map(|m| m.clone_value())
                    .collect::<Vec<Value>>()
                    .into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Null))?;

    if new_len == 0 {
        let _ = db.del(&[key.clone()]);
    } else {
        db.bump_version(key);
    }

    Ok(result)
}

#[inline]
async fn wait_for_event(receiver: &mut Receiver<()>) {
    let _ = receiver.recv().await;
}

#[inline]
async fn schedule_blocking_task<F, T>(
    conn: Arc<Connection>,
    keys_to_watch: Vec<Bytes>,
    worker: F,
    args: VecDeque<Bytes>,
    timeout: Option<Instant>,
) where
    F: Fn(Arc<Connection>, VecDeque<Bytes>, usize) -> T + Send + Sync + 'static,
    T: Future<Output = Result<Value, Error>> + Send + Sync + 'static,
{
    conn.block();

    let mut timeout_rx = if let Some(timeout) = timeout {
        let (timeout_sx, timeout_rx) = broadcast::channel::<()>(1);
        // setup timeout triggering event
        let conn_for_timeout = conn.clone();
        let _keys_to_watch_for_timeout = keys_to_watch.clone();
        let block_id = conn.get_block_id();
        tokio::spawn(async move {
            sleep(timeout - Instant::now()).await;
            if conn_for_timeout.get_block_id() != block_id {
                // Timeout trigger event is not longer relevant
                return;
            }
            conn_for_timeout.unblock(UnblockReason::Timeout);
            conn_for_timeout.append_response(Value::Null);
            // Notify timeout event to the worker thread
            let _ = timeout_sx.send(());
        });
        Some(timeout_rx)
    } else {
        None
    };

    tokio::spawn(async move {
        let db = conn.db();

        let mut changes_watchers = db.subscribe_to_key_changes(&keys_to_watch);
        let mut externally_unblock_watcher = conn.get_unblocked_subscription();

        let mut attempt = 1;

        loop {
            // Run task
            match worker(conn.clone(), args.clone(), attempt).await {
                Ok(Value::Ignore | Value::Null) => {}
                Ok(result) => {
                    conn.append_response(result);
                    conn.unblock(UnblockReason::Finished);
                }
                Err(x) => {
                    conn.append_response(x.into());
                    conn.unblock(UnblockReason::Finished);
                }
            }

            attempt += 1;

            if !conn.is_blocked() {
                break;
            }

            let mut futures = changes_watchers
                .iter_mut()
                .map(wait_for_event)
                .collect::<FuturesUnordered<_>>();

            if let Some(ref mut timeout_rx) = &mut timeout_rx {
                futures.push(wait_for_event(timeout_rx));
            }
            if let Some(ref mut externally) = &mut externally_unblock_watcher {
                futures.push(wait_for_event(externally));
            }

            // wait until a key changes or a timeout event occurs
            let _ = futures.next().await;
        }
    });
}

/// Parses timeout and returns an instant or none if it should wait forever.
#[inline]
fn parse_timeout(arg: &Bytes) -> Result<Option<Instant>, Error> {
    let raw_timeout = bytes_to_number::<f64>(arg)?;
    if raw_timeout < 0f64 {
        return Err(Error::NegativeNumber("timeout".to_owned()));
    }

    if raw_timeout == 0.0 {
        return Ok(None);
    }

    Ok(Some(
        Instant::now()
            .checked_add(Duration::from_millis(
                (raw_timeout * 1_000f64).round() as u64
            ))
            .unwrap_or_else(far_future),
    ))
}

/// BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks
/// the connection when there are no elements to pop from any of the given lists. An element is
/// popped from the head of the first list that is non-empty, with the given keys being checked in
/// the order that they are given.
pub async fn blpop(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let blpop_task = |conn: Arc<Connection>, args: VecDeque<Bytes>, attempt| async move {
        for key in args.iter() {
            match remove_element(&conn, key, None, true) {
                Ok(Value::Null) => (),
                Ok(n) => return Ok(vec![Value::Blob(key.clone()), n].into()),
                Err(x) => {
                    if attempt == 1 {
                        return Err(x);
                    }
                }
            };
        }
        Ok(Value::Null)
    };

    if conn.is_executing_tx() {
        return blpop_task(conn.get_connection(), args, 1).await;
    }

    let timeout = parse_timeout(&args.pop_back().ok_or(Error::Syntax)?)?;
    let conn = conn.get_connection();
    let keys_to_watch = args.iter().cloned().collect::<Vec<_>>();

    conn.block();

    schedule_blocking_task(conn.clone(), keys_to_watch, blpop_task, args, timeout).await;

    Ok(Value::Ignore)
}

/// BLMOVE is the blocking variant of LMOVE. When source contains elements, this
/// command behaves exactly like LMOVE. When used inside a MULTI/EXEC block,
/// this command behaves exactly like LMOVE. When source is empty, Redis will
/// block the connection until another client pushes to it or until timeout (a
/// double value specifying the maximum number of seconds to block) is reached.
/// A timeout of zero can be used to block indefinitely.
///
/// This command comes in place of the now deprecated BRPOPLPUSH. Doing BLMOVE
/// RIGHT LEFT is equivalent.
///
/// See LMOVE for more information.
pub async fn blmove(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    if conn.is_executing_tx() {
        return lmove(conn, args).await;
    }

    let timeout = parse_timeout(&args.pop_back().ok_or(Error::Syntax)?)?;
    let keys_to_watch = vec![args[0].clone(), args[1].clone()];

    schedule_blocking_task(
        conn.get_connection(),
        keys_to_watch,
        |conn, args, _| async move { lmove(&conn, args).await },
        args,
        timeout,
    )
    .await;

    Ok(Value::Ignore)
}

/// BRPOPLPUSH is the blocking variant of RPOPLPUSH. When source contains
/// elements, this command behaves exactly like RPOPLPUSH. When used inside a
/// MULTI/EXEC block, this command behaves exactly like RPOPLPUSH. When source
/// is empty, Redis will block the connection until another client pushes to it
/// or until timeout is reached. A timeout of zero can be used to block
/// indefinitely.
pub async fn brpoplpush(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    blmove(
        conn,
        VecDeque::from([
            args.pop_front().ok_or(Error::Syntax)?,
            args.pop_front().ok_or(Error::Syntax)?,
            "RIGHT".into(),
            "LEFT".into(),
            args.pop_front().ok_or(Error::Syntax)?,
        ]),
    )
    .await
}

/// BRPOP is a blocking list pop primitive. It is the blocking version of RPOP because it blocks
/// the connection when there are no elements to pop from any of the given lists. An element is
/// popped from the tail of the first list that is non-empty, with the given keys being checked in
/// the order that they are given.
pub async fn brpop(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let brpop_task = |conn: Arc<Connection>, args: VecDeque<Bytes>, attempt| async move {
        for key in args.iter() {
            match remove_element(&conn, key, None, false) {
                Ok(Value::Null) => (),
                Ok(n) => return Ok(vec![Value::Blob(key.clone()), n].into()),
                Err(x) => {
                    if attempt == 1 {
                        return Err(x);
                    }
                }
            };
        }
        Ok(Value::Null)
    };

    if conn.is_executing_tx() {
        return brpop_task(conn.get_connection(), args, 1).await;
    }

    let timeout = parse_timeout(&args.pop_back().ok_or(Error::Syntax)?)?;
    let keys_to_watch = args.iter().cloned().collect();

    schedule_blocking_task(
        conn.get_connection(),
        keys_to_watch,
        brpop_task,
        args,
        timeout,
    )
    .await;

    Ok(Value::Ignore)
}

/// Returns the element at index index in the list stored at key. The index is zero-based, so 0
/// means the first element, 1 the second element and so on. Negative indices can be used to
/// designate elements starting at the tail of the list. Here, -1 means the last element, -2 means
/// the penultimate and so forth.
pub async fn lindex(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::List(x) => {
                let index: i64 = bytes_to_number(&args[1])?;

                let index = if index < 0 {
                    x.len().checked_sub(-index as usize).unwrap_or(x.len())
                } else {
                    index as usize
                };

                Ok(x.get(index).map_or(Value::Null, |x| x.clone_value()))
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Null))
}

/// Inserts element in the list stored at key either before or after the reference value pivot.
///
/// When key does not exist, it is considered an empty list and no operation is performed.
pub async fn linsert(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let direction = args.pop_front().ok_or(Error::Syntax)?;
    let pivot = args.pop_front().ok_or(Error::Syntax)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;

    let is_before = match String::from_utf8_lossy(&direction)
        .to_ascii_uppercase()
        .as_str()
    {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Err(Error::Syntax),
    };

    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                let pivot = checksum::Ref::new(&pivot);
                let mut found = false;

                for (key, val) in x.iter().enumerate() {
                    if *val == pivot {
                        let id = if is_before { key } else { key + 1 };

                        let value = checksum::Value::new(value);

                        if id > x.len() {
                            x.push_back(value);
                        } else {
                            x.insert(id, value);
                        }

                        found = true;
                        break;
                    }
                }

                if found {
                    Ok(x.len().into())
                } else {
                    Ok((-1).into())
                }
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))?;

    conn.db().bump_version(&key);

    Ok(result)
}

/// Returns the length of the list stored at key. If key does not exist, it is interpreted as an
/// empty list and 0 is returned. An error is returned when the value stored at key is not a list.
pub async fn llen(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::List(x) => Ok(x.len().into()),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))
}

/// Atomically returns and removes the first/last element (head/tail depending on the wherefrom
/// argument) of the list stored at source, and pushes the element at the first/last element
/// (head/tail depending on the whereto argument) of the list stored at destination.
pub async fn lmove(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let source_is_left = if check_arg!(args, 2, "LEFT") {
        true
    } else if check_arg!(args, 2, "RIGHT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let target_is_left = if check_arg!(args, 3, "LEFT") {
        true
    } else if check_arg!(args, 3, "RIGHT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let source = args.pop_front().ok_or(Error::Syntax)?;
    let destination = args.pop_front().ok_or(Error::Syntax)?;
    let to_lock = vec![source.clone(), destination.clone()];

    let db = conn.db();

    // Lock keys to alter exclusively
    db.lock_keys(&to_lock);

    let mut to_create = None;

    let result = db
        .get(&source)
        .map_mut(|v| match v {
            Value::List(source) => conn
                .db()
                .get(&destination)
                .map_mut(|v| match v {
                    Value::List(target) => {
                        let element = if source_is_left {
                            source.pop_front()
                        } else {
                            source.pop_back()
                        };

                        if let Some(element) = element {
                            let ret = element.clone_value();
                            if target_is_left {
                                target.push_front(element);
                            } else {
                                target.push_back(element);
                            }
                            Ok(ret)
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Err(Error::WrongType),
                })
                .unwrap_or_else(|| {
                    let element = if source_is_left {
                        source.pop_front()
                    } else {
                        source.pop_back()
                    };

                    if let Some(element) = element {
                        let ret = element.clone_value();
                        let mut h = VecDeque::new();
                        h.push_front(element);
                        to_create = Some(h);
                        Ok(ret)
                    } else {
                        Ok(Value::Null)
                    }
                }),
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Null));

    if let Some(to_create) = to_create {
        conn.db().set(destination.clone(), to_create.into(), None);
    }

    // release the lock on keys
    db.unlock_keys(&to_lock);

    if result != Ok(Value::Null) {
        conn.db().bump_version(&source);
        conn.db().bump_version(&destination);
    }

    result
}

/// Removes and returns the first elements of the list stored at key.
///
/// By default, the command pops a single element from the beginning of the list. When provided
/// with the optional count argument, the reply will consist of up to count elements, depending on
/// the list's length.
pub async fn lpop(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let count = match args.get(1) {
        Some(v) => Some(bytes_to_number(v)?),
        None => None,
    };

    remove_element(conn, &args[0], count, true)
}

/// The command returns the index of matching elements inside a Redis list. By default, when no
/// options are given, it will scan the list from head to tail, looking for the first match of
/// "element". If the element is found, its index (the zero-based position in the list) is
/// returned. Otherwise, if no match is found, nil is returned.
pub async fn lpos(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let element = checksum::Ref::new(&args[1]);
    let mut rank = None;
    let mut count = None;
    let mut max_len = None;

    let mut index = 2;
    loop {
        if args.len() <= index {
            break;
        }

        let next = try_get_arg!(args, index + 1);
        match try_get_arg_str!(args, index).to_uppercase().as_str() {
            "RANK" => rank = Some(bytes_to_number::<i64>(next)?),
            "COUNT" => count = Some(bytes_to_number::<usize>(next)?),
            "MAXLEN" => max_len = Some(bytes_to_number::<usize>(next)?),
            _ => return Err(Error::Syntax),
        }

        index += 2;
    }

    let (must_reverse, rank) = if let Some(rank) = rank {
        if rank == 0 {
            return Err(Error::InvalidRank("RANK".to_owned()));
        }
        if rank < 0 {
            (true, Some(-rank as usize))
        } else {
            (false, Some(rank as usize))
        }
    } else {
        (false, None)
    };

    let max_len = max_len.unwrap_or_default();

    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::List(x) => {
                let mut result: Vec<Value> = vec![];

                let mut values = x
                    .iter()
                    .enumerate()
                    .collect::<Vec<(usize, &checksum::Value)>>();

                if must_reverse {
                    values.reverse();
                }

                let mut checks = 1;

                for (id, val) in values.iter() {
                    if **val == element {
                        // Match!
                        if let Some(count) = count {
                            result.push((*id).into());
                            if result.len() == count && count != 0 && rank.is_none() {
                                // There is no point in keep looping. No RANK provided, COUNT is not 0
                                // therefore we can return the vector of result as IS
                                return Ok(result.into());
                            }
                        } else if let Some(rank) = rank {
                            result.push((*id).into());
                            if result.len() == rank {
                                return Ok((*id).into());
                            }
                        } else {
                            // return first match!
                            return Ok((*id).into());
                        }
                    }
                    if checks == max_len {
                        break;
                    }
                    checks += 1;
                }

                if let Some(rank) = rank {
                    let rank = rank - 1;

                    let result = if rank < result.len() {
                        result[rank..].to_vec()
                    } else {
                        vec![]
                    };

                    return Ok(if let Some(count) = count {
                        if count > 0 && count < result.len() {
                            result[0..count].to_vec().into()
                        } else {
                            result.to_vec().into()
                        }
                    } else {
                        result.to_vec().first().cloned().unwrap_or_default()
                    });
                }

                if count.is_some() {
                    Ok(result.into())
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(if count.is_some() {
            Value::Array(vec![])
        } else {
            Value::Null
        }))
}

/// Insert all the specified values at the head of the list stored at key. If key does not exist,
/// it is created as empty list before performing the push operations. When key holds a value that
/// is not a list, an error is returned.
///
/// It is possible to push multiple elements using a single command call just specifying multiple
/// arguments at the end of the command. Elements are inserted one after the other to the head of
/// the list, from the leftmost element to the rightmost element. So for instance the command LPUSH
/// mylist a b c will result into a list containing c as first element, b as second element and a
/// as third element.
pub async fn lpush(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                for val in args.clone().into_iter() {
                    x.push_front(checksum::Value::new(val.clone()));
                }
                Ok(x.len().into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            let mut h = VecDeque::new();

            for val in args.into_iter() {
                h.push_front(checksum::Value::new(val));
            }

            let len = h.len();
            conn.db().set(key.clone(), h.into(), None);
            Ok(len.into())
        })?;

    conn.db().bump_version(&key);
    Ok(result)
}

/// LPUSHX key element
pub async fn lpushx(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                for val in args.into_iter() {
                    x.push_front(checksum::Value::new(val));
                }
                Ok(x.len().into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))?;

    conn.db().bump_version(&key);
    Ok(result)
}

/// Returns the specified elements of the list stored at key. The offsets start and stop are
/// zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being
/// the next element and so on.
///
/// These offsets can also be negative numbers indicating offsets starting at the end of the list.
/// For example, -1 is the last element of the list, -2 the penultimate, and so on.
pub async fn lrange(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    conn.db()
        .get(&args[0])
        .map(|v| match v {
            Value::List(x) => {
                let start: i64 = bytes_to_number(&args[1])?;
                let end: i64 = bytes_to_number(&args[2])?;
                let mut ret = vec![];

                let start = if start < 0 {
                    x.len().checked_sub(-start as usize).unwrap_or_default()
                } else {
                    start as usize
                };

                let end = if end < 0 {
                    if let Some(x) = x.len().checked_sub(-end as usize) {
                        x
                    } else {
                        return Ok(Value::Array(vec![]));
                    }
                } else {
                    end as usize
                };

                for (i, val) in x.iter().enumerate().skip(start) {
                    if i > end {
                        break;
                    }
                    ret.push(val.clone_value());
                }
                Ok(ret.into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Array(vec![])))
}

/// Removes the first count occurrences of elements equal to element from the list stored at key
pub async fn lrem(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let result = conn
        .db()
        .get(&args[0])
        .map_mut(|v| match v {
            Value::List(x) => {
                let element = checksum::Ref::new(&args[2]);
                let limit: i64 = bytes_to_number(&args[1])?;

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
        })
        .unwrap_or(Ok(0.into()))?;

    conn.db().bump_version(&args[0]);

    Ok(result)
}

/// Sets the list element at index to element. For more information on the index argument, see
/// LINDEX.
///
/// An error is returned for out of range indexes.
pub async fn lset(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let index = args.pop_front().ok_or(Error::Syntax)?;
    let value = args.pop_front().ok_or(Error::Syntax)?;

    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                let mut index: i64 = bytes_to_number(&index)?;

                if index < 0 {
                    index += x.len() as i64;
                }

                if let Some(x) = x.get_mut(index as usize) {
                    *x = checksum::Value::new(value);
                    Ok(Value::Ok)
                } else {
                    Err(Error::OutOfRange)
                }
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Err(Error::NotFound))?;

    conn.db().bump_version(&key);

    Ok(result)
}

/// Trim an existing list so that it will contain only the specified range of elements specified.
/// Both start and stop are zero-based indexes, where 0 is the first element of the list (the
/// head), 1 the next element and so on.
pub async fn ltrim(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let result = conn
        .db()
        .get(&args[0])
        .map_mut(|v| match v {
            Value::List(x) => {
                let mut start: i64 = bytes_to_number(&args[1])?;
                let mut end: i64 = bytes_to_number(&args[2])?;

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

                Ok(Value::Ok)
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(Value::Ok))?;

    conn.db().bump_version(&args[1]);

    Ok(result)
}

/// Removes and returns the last elements of the list stored at key.
///
/// By default, the command pops a single element from the end of the list. When provided with the
/// optional count argument, the reply will consist of up to count elements, depending on the
/// list's length.
pub async fn rpop(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let count = match args.get(1) {
        Some(v) => Some(bytes_to_number(v)?),
        None => None,
    };

    remove_element(conn, &args[0], count, false)
}

/// Atomically returns and removes the last element (tail) of the list stored at source, and pushes
/// the element at the first element (head) of the list stored at destination.
pub async fn rpoplpush(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    lmove(
        conn,
        VecDeque::from([
            args.pop_front().ok_or(Error::Syntax)?,
            args.pop_front().ok_or(Error::Syntax)?,
            "RIGHT".into(),
            "LEFT".into(),
        ]),
    )
    .await
}

/// Insert all the specified values at the tail of the list stored at key. If key does not exist,
/// it is created as empty list before performing the push operation. When key holds a value that
/// is not a list, an error is returned.
pub async fn rpushx(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                for val in args.into_iter() {
                    x.push_back(checksum::Value::new(val));
                }
                Ok(x.len().into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or(Ok(0.into()))?;

    conn.db().bump_version(&key);
    Ok(result)
}

/// Insert all the specified values at the tail of the list stored at key. If key does not exist,
/// it is created as empty list before performing the push operation. When key holds a value that
/// is not a list, an error is returned.
pub async fn rpush(conn: &Connection, mut args: VecDeque<Bytes>) -> Result<Value, Error> {
    let key = args.pop_front().ok_or(Error::Syntax)?;
    let result = conn
        .db()
        .get(&key)
        .map_mut(|v| match v {
            Value::List(x) => {
                for val in args.clone().into_iter() {
                    x.push_back(checksum::Value::new(val));
                }
                Ok(x.len().into())
            }
            _ => Err(Error::WrongType),
        })
        .unwrap_or_else(|| {
            let mut h = VecDeque::new();

            for val in args.into_iter() {
                h.push_back(checksum::Value::new(val));
            }

            let len = h.len();
            conn.db().set(key.clone(), h.into(), None);
            Ok(len.into())
        })?;

    conn.db().bump_version(&key);
    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::{
        cmd::test::{create_connection, create_connection_and_pubsub, run_command},
        error::Error,
        value::Value,
    };
    use tokio::time::{sleep, Duration, Instant};

    #[tokio::test]
    async fn blpop_no_waiting() {
        let (mut recv, c) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["blpop", "foo", "1"]).await
        );

        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            recv.recv().await
        );
    }

    #[tokio::test]
    async fn blpop_timeout() {
        let (mut recv, c) = create_connection_and_pubsub();
        let x = Instant::now();

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["blpop", "foobar", "1"]).await
        );

        assert_eq!(Some(Value::Null), recv.recv().await,);

        assert!(Instant::now() - x >= Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn blpop_wait_insert() {
        let (mut recv, c) = create_connection_and_pubsub();
        let x = Instant::now();

        // Query command that will block connection until some data is inserted
        // to foobar, foo, bar or the 5 seconds timeout happens.
        //
        // We are issuing the command, sleeping a little bit then adding data to
        // foobar, before actually waiting on the result.
        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["blpop", "foobar", "foo", "bar", "5"]).await
        );

        // Sleep 1 second before inserting new data
        sleep(Duration::from_millis(1000)).await;

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["lpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        // Read the output of the first blpop command now.
        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            recv.recv().await,
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
        let (mut recv, c) = create_connection_and_pubsub();

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["brpop", "foo", "1"]).await
        );

        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            recv.recv().await,
        );
    }

    #[tokio::test]
    async fn brpop_timeout() {
        let (mut recv, c) = create_connection_and_pubsub();
        let x = Instant::now();

        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["brpop", "foobar", "1"]).await
        );

        assert_eq!(Some(Value::Null), recv.recv().await,);

        assert!(Instant::now() - x >= Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn brpop_wait_insert() {
        let (mut recv, c) = create_connection_and_pubsub();
        let x = Instant::now();

        // Query command that will block connection until some data is inserted
        // to foobar, foo, bar or the 5 seconds timeout happens.
        //
        // We are issuing the command, sleeping a little bit then adding data to
        // foobar, before actually waiting on the result.
        assert_eq!(
            Ok(Value::Ignore),
            run_command(&c, &["brpop", "foobar", "foo", "bar", "5"]).await
        );

        // Sleep 1 second before inserting new data
        sleep(Duration::from_millis(1000)).await;

        assert_eq!(
            Ok(Value::Integer(5)),
            run_command(&c, &["rpush", "foo", "1", "2", "3", "4", "5"]).await,
        );

        // Read the output of the first blpop command now.
        assert_eq!(
            Some(Value::Array(vec![
                Value::Blob("foo".into()),
                Value::Blob("5".into()),
            ])),
            recv.recv().await,
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
            run_command(&c, &["lmove", "foo", "foo-668", "left", "left"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Blob("1".into()),])),
            run_command(&c, &["lrange", "foo-668", "0", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Blob("5".into())),
            run_command(&c, &["lmove", "foo", "foo-668", "right", "left"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::Blob("5".into()),
                Value::Blob("1".into()),
            ])),
            run_command(&c, &["lrange", "foo-668", "0", "-1"]).await
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
    async fn lpos_with_negative_rank_with_count() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Integer(7), Value::Integer(6)])),
            run_command(&c, &["lpos", "mylist", "c", "count", "2", "rank", "-1"]).await
        );
    }

    #[tokio::test]
    async fn lpos_with_negative_rank_with_count_max_len() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Integer(7), Value::Integer(6)])),
            run_command(
                &c,
                &["lpos", "mylist", "c", "count", "0", "maxlen", "3", "rank", "-1"]
            )
            .await
        );
    }

    #[tokio::test]
    async fn lpos_rank_with_count() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Integer(6), Value::Integer(7)])),
            run_command(&c, &["lpos", "mylist", "c", "count", "0", "rank", "2"]).await
        );
    }

    #[tokio::test]
    async fn lpos_all_settings() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![Value::Integer(6)])),
            run_command(
                &c,
                &["lpos", "mylist", "c", "count", "0", "rank", "2", "maxlen", "7"]
            )
            .await
        );
    }

    #[tokio::test]
    async fn lpos_negative_rank() {
        let c = create_connection();
        assert_eq!(
            Ok(Value::Integer(8)),
            run_command(
                &c,
                &["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Integer(7)),
            run_command(&c, &["lpos", "mylist", "c", "rank", "-1"]).await
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
            Ok(Value::Array(vec![Value::Integer(6), Value::Integer(8),])),
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
            Ok(Value::Ok),
            run_command(&c, &["lset", "foo", "-1", "6"]).await,
        );

        assert_eq!(
            Ok(Value::Ok),
            run_command(&c, &["lset", "foo", "-2", "7"]).await,
        );

        assert_eq!(
            Ok(Value::Ok),
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
            Ok(Value::Ok),
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

    #[tokio::test]
    async fn lrange_test_1() {
        let c = create_connection();

        assert_eq!(
            Ok(Value::Integer(10)),
            run_command(
                &c,
                &[
                    "rpush",
                    "mylist",
                    "largevalue",
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "6",
                    "7",
                    "8",
                    "9"
                ]
            )
            .await
        );

        assert_eq!(
            Ok(Value::Array(vec![
                "1".into(),
                "2".into(),
                "3".into(),
                "4".into(),
                "5".into(),
                "6".into(),
                "7".into(),
                "8".into()
            ])),
            run_command(&c, &["lrange", "mylist", "1", "-2"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec!["7".into(), "8".into(), "9".into()])),
            run_command(&c, &["lrange", "mylist", "-3", "-1"]).await
        );

        assert_eq!(
            Ok(Value::Array(vec!["4".into()])),
            run_command(&c, &["lrange", "mylist", "4", "4"]).await
        );
    }
}
