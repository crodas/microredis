//! # Redis Config parsing
//!
//! This module loads and parses the config, compatible with Redis format, to run the service
use crate::error::Error;
use redis_config_parser::de::from_slice;
use serde::Deserialize;
use serde_enum_str::Deserialize_enum_str;
use strum_macros::Display;

/// Config
///
/// Holds the parsed configuration to start the service
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Run the server as a deamon
    pub daemonize: bool,
    /// Port to listen
    pub port: u32,
    /// List of addresses to bind
    pub bind: Vec<String>,
    /// Logging settings
    #[serde(flatten)]
    pub log: Log,
    /// Number of databases
    pub databases: u8,
    /// Unix socket
    pub unixsocket: Option<String>,
}

impl Config {
    /// Returns all addresses to bind
    pub fn get_tcp_hostnames(&self) -> Vec<String> {
        self.bind
            .iter()
            .map(|host| format!("{}:{}", host, self.port))
            .collect::<Vec<String>>()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            daemonize: false,
            port: 6379,
            bind: vec!["127.0.0.1".to_owned()],
            log: Log::default(),
            databases: 16,
            unixsocket: None,
        }
    }
}

/// Log levels
#[derive(Deserialize_enum_str, Debug, PartialEq, Clone, Display)]
pub enum LogLevel {
    /// Trace
    #[serde(rename = "trace")]
    Trace,
    /// Debug
    #[serde(rename = "debug")]
    Debug,
    /// Verbose
    #[serde(rename = "verbose")]
    Verbose,
    /// Notice
    #[serde(rename = "notice")]
    Notice,
    /// Warning
    #[serde(rename = "warning")]
    Warning,
}

impl Default for LogLevel {
    fn default() -> Self {
        Self::Debug
    }
}

/// Logging settings
#[derive(Deserialize, Debug, Default, Clone)]
pub struct Log {
    /// Log level
    #[serde(rename = "loglevel")]
    pub level: LogLevel,
    /// File where to store the log
    #[serde(rename = "logfile")]
    pub file: Option<String>,
}

/// Loads and parses the config from a file path
pub async fn parse(path: String) -> Result<Config, Error> {
    let content = tokio::fs::read(path).await?;
    Ok(from_slice(&content)?)
}
