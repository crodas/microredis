use crate::parser::{parse, ConfigValue, Error as ParsingError};
use args::ArgsDeserializer;
use serde::de::{self, IntoDeserializer};
use std::str;
use thiserror::Error as ThisError;

mod args;
mod value;

/// Errors that can occur when deserializing a type.
#[derive(Debug, PartialEq, Eq, Clone, ThisError)]
pub enum Error {
    /// EOF was reached when looking for a value
    #[error("Unexpected end of file")]
    UnexpectedEof(ErrorInfo),

    #[error("End of stream")]
    EndOfStream,

    /// Custom errors
    #[error("Custom error")]
    Custom(ErrorInfo),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ErrorInfo {
    line: Option<usize>,
    col: usize,
    at: Option<usize>,
    message: String,
}

pub fn from_str<'de, T>(s: &'de str) -> Result<T, Error>
where
    T: de::Deserialize<'de>,
{
    from_slice(s.as_bytes())
}

pub fn from_slice<'de, T>(bytes: &'de [u8]) -> Result<T, Error>
where
    T: de::Deserialize<'de>,
{
    let mut d = Deserializer::new(bytes);
    let ret = T::deserialize(&mut d)?;
    d.end()?;
    Ok(ret)
}

/// Deserialization implementation for Config protocol
pub struct Deserializer<'a> {
    input: &'a [u8],
}

impl<'a> Deserializer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    pub fn end(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Return the next value
    #[inline]
    pub fn parse_next(&mut self) -> Result<ConfigValue<'a>, Error> {
        match parse(self.input) {
            Ok((new_stream, value)) => {
                self.input = new_stream;
                Ok(value)
            }
            Err(ParsingError::Partial) => Err(Error::EndOfStream),
        }
    }
}

impl<'de, 'b> de::Deserializer<'de> for &'b mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_map(MapVisitor {
            de: self,
            last_value: None,
        })
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string seq
        bytes byte_buf map unit newtype_struct
        ignored_any unit_struct tuple_struct tuple option identifier
        enum struct
    }
}

struct MapVisitor<'de, 'b> {
    de: &'b mut Deserializer<'de>,
    last_value: Option<ConfigValue<'de>>,
}

impl<'de, 'b> de::MapAccess<'de> for MapVisitor<'de, 'b> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        match self.de.parse_next() {
            Ok(v) => {
                let name = v.name.clone();
                self.last_value = Some(v);
                seed.deserialize(name.into_deserializer()).map(Some)
            }
            _ => Ok(None),
        }
    }

    #[inline]
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        seed.deserialize(ArgsDeserializer {
            input: self.last_value.as_ref().unwrap().args.clone(),
        })
    }
}

impl Error {
    pub fn custom(at: Option<usize>, s: String) -> Self {
        Self::Custom(ErrorInfo {
            line: None,
            col: 0,
            at,
            message: s,
        })
    }
}

impl de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Error {
        Error::custom(None, msg.to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;
    use serde_enum_str::Deserialize_enum_str;

    #[derive(Deserialize, Debug)]
    pub struct Foo {
        foo: Vec<i32>,
        bar: u8,
        xxx: Option<String>,
    }

    #[derive(Deserialize, Debug, Default)]
    pub struct SaveInfo(pub u64, pub u64);

    #[derive(Deserialize_enum_str, Debug, PartialEq)]
    pub enum AppendFsync {
        #[serde(rename = "always")]
        Always,
        #[serde(rename = "everysec")]
        EverySecond,
        #[serde(other, rename = "no")]
        No,
    }

    #[derive(Deserialize_enum_str, Debug, PartialEq)]
    pub enum LogLevel {
        #[serde(rename = "debug")]
        Debug,
        #[serde(rename = "verbose")]
        Verbose,
        #[serde(rename = "notice")]
        Notice,
        #[serde(rename = "warning")]
        Warning,
    }

    impl Default for LogLevel {
        fn default() -> Self {
            Self::Warning
        }
    }

    impl Default for AppendFsync {
        fn default() -> Self {
            Self::No
        }
    }

    #[derive(Deserialize, Debug, Default)]
    pub struct Config {
        #[serde(rename = "always-show-logo")]
        always_show_logo: bool,
        #[serde(rename = "notify-keyspace-events")]
        notify_keyspace_events: String,
        daemonize: bool,
        port: u32,
        save: SaveInfo,
        #[serde(rename = "appendfsync")]
        append_fsync: AppendFsync,
        #[serde(flatten)]
        log: Log,
        databases: u8,
        bind: Vec<String>,
    }

    #[derive(Deserialize, Debug, Default)]
    pub struct Log {
        #[serde(rename = "loglevel")]
        level: LogLevel,
        #[serde(rename = "logfile")]
        file: String,
    }

    #[test]
    fn de() {
        let x: Foo = from_str("foo 32 44 12\r\nbar 32\r\n").unwrap();
        assert_eq!(32, x.bar);
        assert_eq!(None, x.xxx);
        assert_eq!(3, x.foo.len());
    }

    #[test]
    fn real_config() {
        let x: Config = from_str(
            "always-show-logo yes # this is a comment
        notify-keyspace-events KEA
        daemonize no
        pidfile /var/run/redis.pid
        port 24611
        timeout 0
        bind 127.0.0.1
        loglevel verbose
        logfile '' 
        databases 16
        latency-monitor-threshold 1
        save 60 10000
        rdbcompression yes
        dbfilename dump.rdb
        dir ./tests/tmp/server.64463.1
        slave-serve-stale-data yes
        #this should be ignored
        appendonly no
        appendfsync everysec 
        no-appendfsync-on-rewrite no
        activerehashing yes
        unixsocket /home/crodas/redis/tests/tmp/server.64463.1/socket
        ",
        )
        .unwrap();
        assert!(x.always_show_logo);
        assert_eq!(60, x.save.0);
        assert_eq!(10_000, x.save.1);
        assert_eq!(24_611, x.port);
        assert_eq!("KEA", x.notify_keyspace_events);
        assert_eq!(AppendFsync::EverySecond, x.append_fsync);
        assert!(!x.daemonize);
        assert_eq!(LogLevel::Verbose, x.log.level);
        assert_eq!("", x.log.file);
        assert_eq!(16, x.databases);
        assert_eq!(vec!["127.0.0.1".to_owned()], x.bind);
    }
}
