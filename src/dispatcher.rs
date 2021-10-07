use crate::{cmd, connection::Connection, dispatcher, error::Error, value::Value};
use bytes::Bytes;
use std::convert::TryInto;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

async fn do_time(_conn: &Connection, _args: &[Bytes]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = format!("{}", since_the_epoch.as_secs());
    let millis = format!("{}", since_the_epoch.subsec_millis());

    Ok(vec![seconds.as_str(), millis.as_str()].into())
}

async fn do_command(_conn: &Connection, _args: &[Bytes]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let in_ms: i128 =
        since_the_epoch.as_secs() as i128 * 1000 + since_the_epoch.subsec_millis() as i128;
    Ok(format!("{}", in_ms).as_str().into())
}

dispatcher! {
    list {
        blpop {
            cmd::list::blpop,
            [""],
            -3,
        },
        brpop {
            cmd::list::brpop,
            [""],
            -3,
        },
        lindex {
            cmd::list::lindex,
            [""],
            3,
        },
        linsert {
            cmd::list::linsert,
            [""],
            5,
        },
        llen {
            cmd::list::llen,
            [""],
            2,
        },
        lpop {
            cmd::list::lpop,
            [""],
            -2,
        },
        lpos {
            cmd::list::lpos,
            [""],
            -2,
        },
        lpush {
            cmd::list::lpush,
            [""],
            -3,
        },
        lpushx {
            cmd::list::lpush,
            [""],
            -3,
        },
        lrange {
            cmd::list::lrange,
            [""],
            4,
        },
        lrem {
            cmd::list::lrem,
            [""],
            4,
        },
        lset {
            cmd::list::lset,
            [""],
            4,
        },
        ltrim {
            cmd::list::ltrim,
            [""],
            4,
        },
        rpop {
            cmd::list::rpop,
            [""],
            -2,
        },
        rpush {
            cmd::list::rpush,
            [""],
            -3,
        },
        rpushx {
            cmd::list::rpush,
            [""],
            -3,
        },
    },
    hash {
        hdel {
            cmd::hash::hdel,
            [""],
            -2,
        },
        hexists {
            cmd::hash::hexists,
            [""],
            3,
        },
        hget {
            cmd::hash::hget,
            [""],
            3,
        },
        hgetall {
            cmd::hash::hgetall,
            [""],
            2,
        },
        hincrby {
            cmd::hash::hincrby::<i64>,
            [""],
            4,
        },
        hincrbyfloat {
            cmd::hash::hincrby::<f64>,
            [""],
            4,
        },
        hkeys {
            cmd::hash::hkeys,
            [""],
            2,
        },
        hlen {
            cmd::hash::hlen,
            [""],
            2,
        },
        hmget {
            cmd::hash::hmget,
            [""],
            -3,
        },
        hmset {
            cmd::hash::hset,
            [""],
            -3,
        },
        hrandfield {
            cmd::hash::hrandfield,
            [""],
            -2,
        },
        hset {
            cmd::hash::hset,
            [""],
            -4,
        },
        hsetnx {
            cmd::hash::hsetnx,
            [""],
            -4,
        },
        hstrlen {
            cmd::hash::hstrlen,
            [""],
            3,
        },
        hvals {
            cmd::hash::hvals,
            [""],
            2,
        },
    },
    keys {
        del {
            cmd::key::del,
            ["random" "loading" "stale"],
            -2,
        },
        exists {
            cmd::key::exists,
            ["read" "fast"],
            -2,
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
        persist {
            cmd::key::persist,
            ["write" "fast"],
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
        ttl {
            cmd::key::ttl,
            ["read" "read"],
            2,
        },
    },
    string {
        decr {
            cmd::string::decr,
            ["write" "denyoom" "fast"],
            2,
        },
        decrby {
            cmd::string::decr_by,
            ["write" "denyoom" "fast"],
            3,
        },
        get {
            cmd::string::get,
            ["random" "loading" "stale"],
            2,
        },
        getdel {
            cmd::string::getdel,
            ["random" "loading" "stale"],
            2,
        },
        getset {
            cmd::string::getset,
            ["random" "loading" "stale"],
            -3,
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
        incrbyfloat {
            cmd::string::incr_by_float,
            ["write" "denyoom" "fast"],
            3,
        },
        mget {
            cmd::string::mget,
            ["random" "loading" "stale"],
            -2,
        },
        set {
            cmd::string::set,
            ["random" "loading" "stale"],
            -3,
        },
        setex {
            cmd::string::setex,
            ["random" "loading" "stale"],
            4,
        },
        psetex {
            cmd::string::setex,
            ["random" "loading" "stale"],
            4,
        },
        strlen {
            cmd::string::strlen,
            ["random" "fast"],
            2,
        }
    },
    connection {
        client {
            cmd::client::client,
            ["random" "loading" "stale"],
            -2,
        },
        echo {
            cmd::client::echo,
            ["random" "loading" "stale"],
            2,
        },
        ping {
            cmd::client::ping,
            ["random" "loading" "stale"],
            -1,
        },
    },
    server {
        command  {
            do_command,
            ["random" "loading" "stale"],
            1,
        },
        time {
            do_time,
            ["random" "loading" "stale"],
            1,
        },
    }
}
