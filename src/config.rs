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
    #[serde(rename = "verbose")]
    Debug,
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

#[cfg(test)]
mod test {
    use super::*;
    use redis_config_parser::de::from_str;

    #[test]
    fn parse() {
        let config = "always-show-logo yes
notify-keyspace-events KEA
daemonize no
pidfile /var/run/redis.pid
port 21111
timeout 0
bind 127.0.0.1
loglevel verbose
logfile ''
databases 16
latency-monitor-threshold 1
save 60 10000
rdbcompression yes
dbfilename dump.rdb
dir ./tests/tmp/server.43948.1
slave-serve-stale-data yes
appendonly no
appendfsync everysec
no-appendfsync-on-rewrite no
activerehashing yes
unixsocket /Users/crodas/projects/rust/microredis/tests/tmp/server.43948.1/socket
";

        let config: Config = from_str(config).unwrap();
        assert!(!config.daemonize);
        assert_eq!(21111, config.port);
        assert_eq!(vec!["127.0.0.1"], config.bind);
        assert_eq!(vec!["127.0.0.1:21111"], config.get_tcp_hostnames());
        assert_eq!(LogLevel::Debug, config.log.level);
        assert_eq!(Some("".to_owned()), config.log.file);
        assert_eq!(16, config.databases);
        assert_eq!(
            Some(
                "/Users/crodas/projects/rust/microredis/tests/tmp/server.43948.1/socket".to_owned()
            ),
            config.unixsocket
        );
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(!config.daemonize);
        assert_eq!(6379, config.port);
        assert_eq!(vec!["127.0.0.1"], config.bind);
        assert_eq!(vec!["127.0.0.1:6379"], config.get_tcp_hostnames());
        assert_eq!(LogLevel::Debug, config.log.level);
        assert_eq!(None, config.log.file);
        assert_eq!(16, config.databases);
        assert_eq!(None, config.unixsocket);
    }
}
