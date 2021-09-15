use crate::{cmd, connection::Connection, dispatcher, error::Error, value::Value};
use bytes::Bytes;
use std::convert::TryInto;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

fn do_time(_conn: &Connection, _args: &[Bytes]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = format!("{}", since_the_epoch.as_secs());
    let millis = format!("{}", since_the_epoch.subsec_millis());

    Ok(vec![seconds.as_str(), millis.as_str()].into())
}

fn do_command(_conn: &Connection, _args: &[Bytes]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let in_ms: i128 =
        since_the_epoch.as_secs() as i128 * 1000 + since_the_epoch.subsec_millis() as i128;
    Ok(format!("{}", in_ms).as_str().into())
}

dispatcher! {
    command  {
        do_command,
        ["random" "loading" "stale"],
        1,
    },
    client {
        cmd::client::client,
        ["random" "loading" "stale"],
        -2,
    },
    decr {
        cmd::string::decr,
        ["write" "denyoom" "fast"],
        2,
    },
    echo {
        cmd::client::echo,
        ["random" "loading" "stale"],
        2,
    },
    expire {
        cmd::key::expire,
        ["read" "write" "fast"],
        3,
    },
    expireat {
        cmd::key::expire_at,
        ["read" "write" "fast"],
        3,
    },
    expiretime {
        cmd::key::expire_time,
        ["read" "write" "fast"],
        2,
    },
    del {
        cmd::key::del,
        ["random" "loading" "stale"],
        -2,
    },
    get {
        cmd::string::get,
        ["random" "loading" "stale"],
        2,
    },
    incr {
        cmd::string::incr,
        ["write" "denyoom" "fast"],
        2,
    },
    incrby {
        cmd::string::incr_by,
        ["write" "denyoom" "fast"],
        3,
    },
    persist {
        cmd::key::persist,
        ["write" "fast"],
        2,
    },
    ttl {
        cmd::key::ttl,
        ["read" "read"],
        2,
    },
    pexpire {
        cmd::key::expire,
        ["read" "write" "fast"],
        3,
    },
    pexpireat {
        cmd::key::expire_at,
        ["read" "write" "fast"],
        3,
    },
    pexpiretime {
        cmd::key::expire_time,
        ["read" "write" "fast"],
        2,
    },
    pttl {
        cmd::key::ttl,
        ["read" "read"],
        2,
    },
    set {
        cmd::string::set,
        ["random" "loading" "stale"],
        -3,
    },
    getset {
        cmd::string::getset,
        ["random" "loading" "stale"],
        -3,
    },
    ping {
        cmd::client::ping,
        ["random" "loading" "stale"],
        -1,
    },
    time {
        do_time,
        ["random" "loading" "stale"],
        1,
    },
}
