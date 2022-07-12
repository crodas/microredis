//! # Dispatcher
//!
//! Here is where every command is defined. Each command has some definition and a handler. Their
//! handler are rust functions.
//!
//! Each command is defined with the dispatcher macro, which generates efficient and developer
//! friendly code.
use crate::{
    cmd,
    connection::{Connection, ConnectionStatus},
    dispatcher,
    error::Error,
    value::Value,
};
use bytes::Bytes;
use command::Flag;
use std::convert::TryInto;

pub mod command;

/// Returns the server time
dispatcher! {
    set {
        SADD {
            cmd::set::sadd,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        SCARD {
            cmd::set::scard,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        SDIFF {
            cmd::set::sdiff,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        SDIFFSTORE {
            cmd::set::sdiffstore,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            -1,
            1,
            true,
        },
        SINTER {
            cmd::set::sinter,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        SINTERCARD {
            cmd::set::sintercard,
            [Flag::ReadOnly],
            -2,
            1,
            -1,
            1,
            true,
        },
        SINTERSTORE {
            cmd::set::sinterstore,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            -1,
            1,
            true,
        },
        SISMEMBER {
            cmd::set::sismember,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        SMEMBERS {
            cmd::set::smembers,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            1,
            1,
            1,
            true,
        },
        SMISMEMBER {
            cmd::set::smismember,
            [Flag::ReadOnly Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        SMOVE {
            cmd::set::smove,
            [Flag::Write Flag::Fast],
            4,
            1,
            2,
            1,
            true,
        },
        SPOP {
            cmd::set::spop,
            [Flag::Write Flag::Random Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        SRANDMEMBER {
            cmd::set::srandmember,
            [Flag::ReadOnly Flag::Random],
            -2,
            1,
            1,
            1,
            true,
        },
        SREM {
            cmd::set::srem,
            [Flag::Write Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        SUNION {
            cmd::set::sunion,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        SUNIONSTORE {
            cmd::set::sunionstore,
            [Flag::Write Flag::DenyOom],
            -2,
            1,
            -1,
            1,
            true,
        },
    },
    metrics {
        METRICS {
            cmd::metrics::metrics,
            [Flag::ReadOnly Flag::Fast],
            -1,
            0,
            0,
            0,
            false,
        },
    },
    list {
        BLPOP {
            cmd::list::blpop,
            [Flag::Write Flag::NoScript],
            -3,
            1,
            -2,
            1,
            true,
        },
        BRPOP {
            cmd::list::brpop,
            [Flag::Write Flag::NoScript],
            -3,
            1,
            -2,
            1,
            true,
        },
        LINDEX {
            cmd::list::lindex,
            [Flag::ReadOnly],
            3,
            1,
            1,
            1,
            true,
        },
        LINSERT {
            cmd::list::linsert,
            [Flag::Write Flag::DenyOom],
            5,
            1,
            1,
            1,
            true,
        },
        LLEN {
            cmd::list::llen,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        LMOVE {
            cmd::list::lmove,
            [Flag::Write Flag::DenyOom],
            5,
            1,
            2,
            1,
            true,
        },
        LPOP {
            cmd::list::lpop,
            [Flag::Write Flag::DenyOom],
            -2,
            1,
            -2,
            1,
            true,
        },
        LPOS {
            cmd::list::lpos,
            [Flag::ReadOnly],
            -2,
            1,
            1,
            1,
            true,
        },
        LPUSH {
            cmd::list::lpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        LPUSHX {
            cmd::list::lpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        LRANGE {
            cmd::list::lrange,
            [Flag::ReadOnly],
            4,
            1,
            1,
            1,
            true,
        },
        LREM {
            cmd::list::lrem,
            [Flag::Write],
            4,
            1,
            1,
            1,
            true,
        },
        LSET {
            cmd::list::lset,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        LTRIM {
            cmd::list::ltrim,
            [Flag::Write],
            4,
            1,
            1,
            1,
            true,
        },
        RPOP {
            cmd::list::rpop,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        RPOPLPUSH {
            cmd::list::rpoplpush,
            [Flag::Write Flag::DenyOom],
            3,
            1,
            2,
            1,
            true,
        },
        RPUSH {
            cmd::list::rpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        RPUSHX {
            cmd::list::rpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
    },
    hash {
        HDEL {
            cmd::hash::hdel,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        HEXISTS {
            cmd::hash::hexists,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        HGET {
            cmd::hash::hget,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        HGETALL {
            cmd::hash::hgetall,
            [Flag::ReadOnly Flag::Random],
            2,
            1,
            1,
            1,
            true,
        },
        HINCRBY {
            cmd::hash::hincrby::<i64>,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        HINCRBYFLOAT {
            cmd::hash::hincrby::<f64>,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        HKEYS {
            cmd::hash::hkeys,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            1,
            1,
            1,
            true,
        },
        HLEN {
            cmd::hash::hlen,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        HMGET {
            cmd::hash::hmget,
            [Flag::ReadOnly Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        HMSET {
            cmd::hash::hset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        HRANDFIELD {
            cmd::hash::hrandfield,
            [Flag::ReadOnly Flag::ReadOnly],
            -2,
            1,
            1,
            1,
            true,
        },
        HSET {
            cmd::hash::hset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -4,
            1,
            1,
            1,
            true,
        },
        HSETNX {
            cmd::hash::hsetnx,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        HSTRLEN {
            cmd::hash::hstrlen,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        HVALS {
            cmd::hash::hvals,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            1,
            1,
            1,
            true,
        },
    },
    keys {
        COPY {
            cmd::key::copy,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            2,
            1,
            true,
        },
        DEL {
            cmd::key::del,
            [Flag::Write],
            -2,
            1,
            -1,
            1,
            true,
        },
        EXISTS {
            cmd::key::exists,
            [Flag::ReadOnly Flag::Fast],
            -2,
            1,
            -1,
            1,
            true,
        },
        EXPIRE {
            cmd::key::expire,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        EXPIREAT {
            cmd::key::expire_at,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        EXPIRETIME {
            cmd::key::expire_time,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        KEYS {
            cmd::key::keys,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            0,
            0,
            0,
            true,
        },
        MOVE {
            cmd::key::move_key,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        OBJECT {
            cmd::key::object,
            [Flag::ReadOnly Flag::Random],
            -2,
            2,
            2,
            1,
            true,
        },
        PERSIST {
            cmd::key::persist,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        PEXPIRE {
            cmd::key::expire,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        PEXPIREAT {
            cmd::key::expire_at,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        PEXPIRETIME {
            cmd::key::expire_time,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        PTTL {
            cmd::key::ttl,
            [Flag::ReadOnly Flag::Random Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        RENAME {
            cmd::key::rename,
            [Flag::Write],
            3,
            1,
            2,
            1,
            true,
        },
        RENAMENX {
            cmd::key::rename,
            [Flag::Write Flag::Write],
            3,
            1,
            2,
            1,
            true,
        },
        SCAN {
            cmd::key::scan,
            [Flag::ReadOnly Flag::Random],
            -2,
            0,
            0,
            0,
            true,
        },
        TTL {
            cmd::key::ttl,
            [Flag::ReadOnly Flag::Random Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        TYPE {
            cmd::key::data_type,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        UNLINK {
            cmd::key::del,
            [Flag::Write Flag::Fast],
            -2,
            1,
            -1,
            1,
            true,
        },
    },
    string {
        APPEND {
            cmd::string::append,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        DECR {
            cmd::string::decr,
            [Flag::Write Flag::DenyOom Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        DECRBY {
            cmd::string::decr_by,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        GET {
            cmd::string::get,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        GETEX {
            cmd::string::getex,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        GETRANGE {
            cmd::string::getrange,
            [Flag::ReadOnly],
            4,
            1,
            1,
            1,
            true,
        },
        GETDEL {
            cmd::string::getdel,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        GETSET {
            cmd::string::getset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        INCR {
            cmd::string::incr,
            [Flag::Write Flag::DenyOom Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        INCRBY {
            cmd::string::incr_by,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        INCRBYFLOAT {
            cmd::string::incr_by_float,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        MGET {
            cmd::string::mget,
            [Flag::ReadOnly Flag::Fast],
            -2,
            1,
            -1,
            1,
            true,
        },
        MSET {
            cmd::string::mset,
            [Flag::Write Flag::DenyOom],
            -2,
            1,
            -1,
            1,
            true,
        },
        MSETNX {
            cmd::string::msetnx,
            [Flag::Write Flag::DenyOom],
            -2,
            1,
            -1,
            1,
            true,
        },
        SET {
            cmd::string::set,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            1,
            1,
            true,
        },
        SETEX {
            cmd::string::setex,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        SETNX {
            cmd::string::setnx,
            [Flag::Write Flag::DenyOom],
            3,
            1,
            1,
            1,
            true,
        },
        PSETEX {
            cmd::string::setex,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        STRLEN {
            cmd::string::strlen,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        SUBSTR {
            cmd::string::getrange,
            [Flag::ReadOnly],
            2,
            1,
            1,
            1,
            true,
        },
        SETRANGE {
            cmd::string::setrange,
            [Flag::Write],
            4,
            1,
            1,
            1,
            true,
        }
    },
    connection {
        CLIENT {
            cmd::client::client,
            [Flag::Admin Flag::NoScript Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        ECHO {
            cmd::client::echo,
            [Flag::Fast],
            2,
            0,
            0,
            0,
            true,
        },
        PING {
            cmd::client::ping,
            [Flag::Stale Flag::Fast],
            -1,
            0,
            0,
            0,
            true,
        },
        RESET {
            cmd::client::reset,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
        SELECT {
            cmd::client::select,
            [Flag::Fast Flag::Stale Flag::Loading],
            2,
            0,
            0,
            0,
            false,
        }
    },
    transaction {
        DISCARD {
            cmd::transaction::discard,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
        EXEC {
            cmd::transaction::exec,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::SkipMonitor Flag::SkipSlowlog],
            1,
            0,
            0,
            0,
            false,
        },
        MULTI {
            cmd::transaction::multi,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
        WATCH {
            cmd::transaction::watch,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            -2,
            1,
            -1,
            1,
            false,
        },
        UNWATCH {
            cmd::transaction::unwatch,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            true,
        },
    },
    pubsub {
        PUBLISH {
            cmd::pubsub::publish,
            [Flag::PubSub Flag::Loading Flag::Stale Flag::Fast Flag::MayReplicate],
            3,
            0,
            0,
            0,
            true,
        },
        PUBSUB {
            cmd::pubsub::pubsub,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        PSUBSCRIBE {
            cmd::pubsub::subscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        PUNSUBSCRIBE {
            cmd::pubsub::punsubscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
        SUBSCRIBE {
            cmd::pubsub::subscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        UNSUBSCRIBE {
            cmd::pubsub::unsubscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
    },
    server {
        COMMAND {
            cmd::server::command,
            [Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
        DBSIZE {
            cmd::server::dbsize,
            [Flag::ReadOnly Flag::Fast],
            1,
            0,
            0,
            0,
            true,
        },
        DEBUG {
            cmd::server::debug,
            [Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        INFO {
            cmd::server::info,
            [Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
        FLUSHDB {
            cmd::server::flushdb,
            [Flag::Write],
            -1,
            0,
            0,
            0,
            true,
        },
        TIME {
            cmd::server::time,
            [Flag::Random Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            true,
        },
    }
}
