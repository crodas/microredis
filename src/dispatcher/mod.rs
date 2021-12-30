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
        sadd {
            cmd::set::sadd,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        scard {
            cmd::set::scard,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        sdiff {
            cmd::set::sdiff,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        sdiffstore {
            cmd::set::sdiffstore,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            -1,
            1,
            true,
        },
        sinter {
            cmd::set::sinter,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        sintercard {
            cmd::set::sintercard,
            [Flag::ReadOnly],
            -2,
            1,
            -1,
            1,
            true,
        },
        sinterstore {
            cmd::set::sinterstore,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            -1,
            1,
            true,
        },
        sismember {
            cmd::set::sismember,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        smembers {
            cmd::set::smembers,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            1,
            1,
            1,
            true,
        },
        smismember {
            cmd::set::smismember,
            [Flag::ReadOnly Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        smove {
            cmd::set::smove,
            [Flag::Write Flag::Fast],
            4,
            1,
            2,
            1,
            true,
        },
        spop {
            cmd::set::spop,
            [Flag::Write Flag::Random Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        srandmember {
            cmd::set::srandmember,
            [Flag::ReadOnly Flag::Random],
            -2,
            1,
            1,
            1,
            true,
        },
        srem {
            cmd::set::srem,
            [Flag::Write Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        sunion {
            cmd::set::sunion,
            [Flag::ReadOnly Flag::SortForScript],
            -2,
            1,
            -1,
            1,
            true,
        },
        sunionstore {
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
        metrics {
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
        blpop {
            cmd::list::blpop,
            [Flag::Write Flag::NoScript],
            -3,
            1,
            -2,
            1,
            true,
        },
        brpop {
            cmd::list::brpop,
            [Flag::Write Flag::NoScript],
            -3,
            1,
            -2,
            1,
            true,
        },
        lindex {
            cmd::list::lindex,
            [Flag::ReadOnly],
            3,
            1,
            1,
            1,
            true,
        },
        linsert {
            cmd::list::linsert,
            [Flag::Write Flag::DenyOom],
            5,
            1,
            1,
            1,
            true,
        },
        llen {
            cmd::list::llen,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        lmove {
            cmd::list::lmove,
            [Flag::Write Flag::DenyOom],
            5,
            1,
            2,
            1,
            true,
        },
        lpop {
            cmd::list::lpop,
            [Flag::Write Flag::DenyOom],
            -2,
            1,
            -2,
            1,
            true,
        },
        lpos {
            cmd::list::lpos,
            [Flag::ReadOnly],
            -2,
            1,
            1,
            1,
            true,
        },
        lpush {
            cmd::list::lpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        lpushx {
            cmd::list::lpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        lrange {
            cmd::list::lrange,
            [Flag::ReadOnly],
            4,
            1,
            1,
            1,
            true,
        },
        lrem {
            cmd::list::lrem,
            [Flag::Write],
            4,
            1,
            1,
            1,
            true,
        },
        lset {
            cmd::list::lset,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        ltrim {
            cmd::list::ltrim,
            [Flag::Write],
            4,
            1,
            1,
            1,
            true,
        },
        rpop {
            cmd::list::rpop,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        rpoplpush {
            cmd::list::rpoplpush,
            [Flag::Write Flag::DenyOom],
            3,
            1,
            2,
            1,
            true,
        },
        rpush {
            cmd::list::rpush,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        rpushx {
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
        hdel {
            cmd::hash::hdel,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        hexists {
            cmd::hash::hexists,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        hget {
            cmd::hash::hget,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        hgetall {
            cmd::hash::hgetall,
            [Flag::ReadOnly Flag::Random],
            2,
            1,
            1,
            1,
            true,
        },
        hincrby {
            cmd::hash::hincrby::<i64>,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        hincrbyfloat {
            cmd::hash::hincrby::<f64>,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        hkeys {
            cmd::hash::hkeys,
            [Flag::ReadOnly Flag::SortForScript],
            2,
            1,
            1,
            1,
            true,
        },
        hlen {
            cmd::hash::hlen,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        hmget {
            cmd::hash::hmget,
            [Flag::ReadOnly Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        hmset {
            cmd::hash::hset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -3,
            1,
            1,
            1,
            true,
        },
        hrandfield {
            cmd::hash::hrandfield,
            [Flag::ReadOnly Flag::ReadOnly],
            -2,
            1,
            1,
            1,
            true,
        },
        hset {
            cmd::hash::hset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            -4,
            1,
            1,
            1,
            true,
        },
        hsetnx {
            cmd::hash::hsetnx,
            [Flag::Write Flag::DenyOom Flag::Fast],
            4,
            1,
            1,
            1,
            true,
        },
        hstrlen {
            cmd::hash::hstrlen,
            [Flag::ReadOnly Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        hvals {
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
        del {
            cmd::key::del,
            [Flag::Write],
            -2,
            1,
            -1,
            1,
            true,
        },
        exists {
            cmd::key::exists,
            [Flag::ReadOnly Flag::Fast],
            -2,
            1,
            -1,
            1,
            true,
        },
        expire {
            cmd::key::expire,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        expireat {
            cmd::key::expire_at,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        expiretime {
            cmd::key::expire_time,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        persist {
            cmd::key::persist,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        pexpire {
            cmd::key::expire,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        pexpireat {
            cmd::key::expire_at,
            [Flag::Write Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        pexpiretime {
            cmd::key::expire_time,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        pttl {
            cmd::key::ttl,
            [Flag::ReadOnly Flag::Random Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        ttl {
            cmd::key::ttl,
            [Flag::ReadOnly Flag::Random Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
    },
    string {
        append {
            cmd::string::append,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        decr {
            cmd::string::decr,
            [Flag::Write Flag::DenyOom Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        decrby {
            cmd::string::decr_by,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        get {
            cmd::string::get,
            [Flag::ReadOnly Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        getex {
            cmd::string::getex,
            [Flag::Write Flag::Fast],
            -2,
            1,
            1,
            1,
            true,
        },
        getrange {
            cmd::string::getrange,
            [Flag::ReadOnly],
            4,
            1,
            1,
            1,
            true,
        },
        getdel {
            cmd::string::getdel,
            [Flag::Write Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        getset {
            cmd::string::getset,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        incr {
            cmd::string::incr,
            [Flag::Write Flag::DenyOom Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        },
        incrby {
            cmd::string::incr_by,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        incrbyfloat {
            cmd::string::incr_by_float,
            [Flag::Write Flag::DenyOom Flag::Fast],
            3,
            1,
            1,
            1,
            true,
        },
        mget {
            cmd::string::mget,
            [Flag::ReadOnly Flag::Fast],
            -2,
            1,
            -1,
            1,
            true,
        },
        set {
            cmd::string::set,
            [Flag::Write Flag::DenyOom],
            -3,
            1,
            1,
            1,
            true,
        },
        setex {
            cmd::string::setex,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        psetex {
            cmd::string::setex,
            [Flag::Write Flag::DenyOom],
            4,
            1,
            1,
            1,
            true,
        },
        strlen {
            cmd::string::strlen,
            [Flag::Random Flag::Fast],
            2,
            1,
            1,
            1,
            true,
        }
    },
    connection {
        client {
            cmd::client::client,
            [Flag::Admin Flag::NoScript Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        echo {
            cmd::client::echo,
            [Flag::Fast],
            2,
            0,
            0,
            0,
            true,
        },
        ping {
            cmd::client::ping,
            [Flag::Stale Flag::Fast],
            -1,
            0,
            0,
            0,
            true,
        },
        reset {
            cmd::client::reset,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
    },
    transaction {
        discard {
            cmd::transaction::discard,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
        exec {
            cmd::transaction::exec,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::SkipMonitor Flag::SkipSlowlog],
            1,
            0,
            0,
            0,
            false,
        },
        multi {
            cmd::transaction::multi,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            1,
            0,
            0,
            0,
            false,
        },
        watch {
            cmd::transaction::watch,
            [Flag::NoScript Flag::Loading Flag::Stale Flag::Fast],
            -2,
            1,
            -1,
            1,
            false,
        },
        unwatch {
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
        publish {
            cmd::pubsub::publish,
            [Flag::PubSub Flag::Loading Flag::Stale Flag::Fast Flag::MayReplicate],
            3,
            0,
            0,
            0,
            true,
        },
        pubsub {
            cmd::pubsub::pubsub,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        psubscribe {
            cmd::pubsub::subscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        punsubscribe {
            cmd::pubsub::punsubscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
        subscribe {
            cmd::pubsub::subscribe,
            [Flag::PubSub Flag::Random Flag::Loading Flag::Stale],
            -2,
            0,
            0,
            0,
            true,
        },
        unsubscribe {
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
        command {
            cmd::server::command,
            [Flag::Random Flag::Loading Flag::Stale],
            -1,
            0,
            0,
            0,
            true,
        },
        time {
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
