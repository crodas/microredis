//! # Dispatcher
//!
//! Here is where every command is defined. Each command has some definition and a handler. Their
//! handler are rust functions.
//!
//! Each command is defined with the dispatcher macro, which generates efficient and developer
//! friendly code.
use crate::{
    connection::{Connection, ConnectionStatus},
    dispatcher,
    error::Error,
    value::Value,
};
use bytes::Bytes;
use metered::{ErrorCount, HitCount, InFlight, ResponseTime, Throughput};
use std::convert::TryInto;

/// Command Flags
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Flag {
    /// May result in database modification
    Write,
    /// Will never modify database
    ReadOnly,
    /// Can fail if the server runs out of memory
    DenyOom,
    /// Server Admin command
    Admin,
    /// Pubsub related command
    PubSub,
    /// Not used, added to be compatible
    NoScript,
    /// Random result
    Random,
    /// Not used, added to be compatible
    SortForScript,
    /// Allow command while database is loading
    Loading,
    /// Allow command while replica has stale data
    Stale,
    /// Do not show this command in MONITOR
    SkipMonitor,
    /// Do not gather stats about slow log
    SkipSlowlog,
    /// The command is fast (Close to log(N) time)
    Fast,
    /// Command may be replicated to other nodes
    MayReplicate,
}

impl ToString for Flag {
    fn to_string(&self) -> String {
        match self {
            Self::Write => "write",
            Self::DenyOom => "denyoom",
            Self::ReadOnly => "readonly",
            Self::Admin => "admin",
            Self::PubSub => "pubsub",
            Self::NoScript => "noscript",
            Self::Random => "random",
            Self::SortForScript => "sort_for_script",
            Self::Loading => "loading",
            Self::Stale => "stale",
            Self::SkipMonitor => "skip_monitor",
            Self::SkipSlowlog => "skip_slowlog",
            Self::Fast => "fast",
            Self::MayReplicate => "may_replicate",
        }
        .to_owned()
    }
}

/// Command definition
#[derive(Debug)]
pub struct Command {
    name: &'static str,
    group: &'static str,
    flags: &'static [Flag],
    min_args: i32,
    key_start: i32,
    key_stop: i32,
    key_step: usize,
    is_queueable: bool,
    metrics: Metrics,
}

/// Metric struct for all command
#[derive(Debug, Default, serde::Serialize)]
pub struct Metrics {
    /// Command hits
    pub hit_count: HitCount,
    /// Error count
    pub error_count: ErrorCount,
    /// How many concurrent executions are happening right now
    pub in_flight: InFlight,
    /// Response time
    pub response_time: ResponseTime,
    /// Throughput
    pub throughput: Throughput,
}

impl Command {
    /// Creates a new comamnd
    pub fn new(
        name: &'static str,
        group: &'static str,
        flags: &'static [Flag],
        min_args: i32,
        key_start: i32,
        key_stop: i32,
        key_step: usize,
        is_queueable: bool,
    ) -> Self {
        Self {
            name,
            group,
            flags,
            min_args,
            key_start,
            key_stop,
            key_step,
            is_queueable,
            metrics: Metrics::default(),
        }
    }

    /// Returns a reference to the metrics
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Can this command be executed in a pub-sub only mode?
    pub fn is_pubsub_executable(&self) -> bool {
        self.group == "pubsub" || self.name == "PING" || self.name == "RESET"
    }

    /// Can this command be queued in a transaction or should it be executed right away?
    pub fn is_queueable(&self) -> bool {
        self.is_queueable
    }

    /// Returns all database keys from the command arguments
    pub fn get_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a Bytes> {
        let start = self.key_start;
        let stop = if self.key_stop > 0 {
            self.key_stop
        } else {
            (args.len() as i32) + self.key_stop
        };

        if start == 0 {
            return vec![];
        }

        let mut result = vec![];

        for i in (start..stop + 1).step_by(self.key_step) {
            result.push(&args[i as usize]);
        }

        result
    }

    /// Checks if a given number of args is expected by this command
    pub fn check_number_args(&self, n: usize) -> bool {
        if (self.min_args >= 0) {
            n == (self.min_args as i32).try_into().unwrap_or(0)
        } else {
            let s: usize = (self.min_args as i32).abs().try_into().unwrap_or(0);
            n >= s
        }
    }

    /// Returns information about this command. The response is encoded as a
    /// Value, following the output of the COMMAND command in redis
    pub fn get_command_info(&self) -> Value {
        Value::Array(vec![
            self.name().into(),
            self.get_min_args().into(),
            Value::Array(
                self.get_flags()
                    .iter()
                    .map(|m| m.to_string().into())
                    .collect(),
            ),
            self.get_key_start().into(),
            self.get_key_stop().into(),
            self.get_key_step().into(),
        ])
    }

    /// Returns the command's flags
    pub fn get_flags(&self) -> Vec<Flag> {
        self.flags.to_vec()
    }

    /// Returns the minimum arguments (including the command name itself) that
    /// this command takes. This is also known as the arity of a command.
    pub fn get_min_args(&self) -> i32 {
        self.min_args
    }

    /// Where does the first database 'key' start in the arguments list
    pub fn get_key_start(&self) -> i32 {
        self.key_start
    }

    /// Where does the last database 'key' start in the arguments list
    pub fn get_key_stop(&self) -> i32 {
        self.key_stop
    }

    /// Useful to extract 'keys' from the arguments alongside with key_stop and
    /// key_start
    pub fn get_key_step(&self) -> usize {
        self.key_step
    }

    /// Command group
    pub fn group(&self) -> &'static str {
        &self.group
    }

    /// Command name
    pub fn name(&self) -> &'static str {
        &self.name
    }
}
