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

/// Command definition
#[derive(Debug)]
pub struct Command {
    name: &'static str,
    group: &'static str,
    tags: &'static [&'static str],
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
        tags: &'static [&'static str],
        min_args: i32,
        key_start: i32,
        key_stop: i32,
        key_step: usize,
        is_queueable: bool,
    ) -> Self {
        Self {
            name,
            group,
            tags,
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
        self.group == "pubsub" || self.name == "ping" || self.name == "reset"
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

    /// Command group
    pub fn group(&self) -> &'static str {
        &self.group
    }

    /// Command name
    pub fn name(&self) -> &'static str {
        &self.name
    }
}
