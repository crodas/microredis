use std::borrow::Cow;

#[derive(Debug, PartialEq, Clone)]
pub enum Error {
    /// The data is incomplete. This it not an error per-se, but rather a
    /// mechanism to let the caller know they should keep buffering data before
    /// calling the parser again.
    Partial,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Args<'a> {
    None,
    Single(Cow<'a, str>),
    Multiple(Vec<Cow<'a, str>>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Config<'a> {
    pub name: Cow<'a, str>,
    pub args: Args<'a>,
}

macro_rules! skip {
    ($bytes:ident, $to_skip:expr) => {{
        let len = $bytes.len();
        let mut ret = &$bytes[len..];

        for i in 0..len {
            if $to_skip.iter().find(|x| **x == $bytes[i]).is_none() {
                ret = &$bytes[i..];
                break;
            }
        }
        ret
    }};
}

macro_rules! read_until {
    ($bytes:ident, $stop:expr) => {{
        let len = $bytes.len();
        let mut result = None;
        for i in 0..len {
            if $stop.iter().find(|x| **x == $bytes[i]).is_some() {
                result = Some((&$bytes[i..], String::from_utf8_lossy(&$bytes[0..i])));
                break;
            }
        }

        if let Some(result) = result {
            result
        } else {
            return Err(Error::Partial);
        }
    }};
}

pub fn parse(bytes: &'_ [u8]) -> Result<(&'_ [u8], Config<'_>), Error> {
    let bytes = skip!(bytes, vec![b' ', b'\t', b'\r', b'\n']);
    let (bytes, name) = read_until!(bytes, vec![b' ', b'\t', b'\r']);
    let bytes = skip!(bytes, vec![b' ', b'\t', b'\r']);

    let mut args = vec![];
    let len = bytes.len();
    let mut i = 0;

    #[allow(unused_variables)]
    #[allow(unreachable_patterns)]
    loop {
        if i >= len {
            return Err(Error::Partial);
        }
        match bytes[i] {
            b' ' | b'\t' | b'\r' => {}
            b'\n' => {
                i += 1;
                break;
            }
            b'"' | b'\'' => {
                let e = i;
                let stop_at = bytes[e];
                loop {
                    if i >= len {
                        return Err(Error::Partial);
                    }
                    match bytes[i] {
                        stop_at => {
                            args.push(String::from_utf8_lossy(&bytes[e..i]));
                            break;
                        }
                        b'\\' => i += 1,
                        _ => continue,
                    };
                }
            }
            _ => {
                let e = i;
                loop {
                    if i >= len {
                        return Err(Error::Partial);
                    }
                    if bytes[i] == b' '
                        || bytes[i] == b'\t'
                        || bytes[i] == b'\r'
                        || bytes[i] == b'\n'
                    {
                        args.push(String::from_utf8_lossy(&bytes[e..i]));
                        i -= 1;
                        break;
                    }
                    i += 1;
                }
            }
        };

        i += 1;
    }

    let bytes = &bytes[i..];

    let args = match args.len() {
        0 => Args::None,
        1 => Args::Single(args[0].clone()),
        _ => Args::Multiple(args),
    };

    Ok((bytes, Config { name, args }))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_partial() {
        let data = b"foo bar foo";
        assert_eq!(Err(Error::Partial), parse(data));
    }

    #[test]
    fn test_parse_single_argument() {
        let data = b"foo bar\r\nsomething";
        let (bytes, config) = parse(data).unwrap();
        assert_eq!("foo", config.name);
        assert_eq!(Args::Single("bar".into()), config.args);
        assert_eq!(b"something", bytes);
    }

    #[test]
    fn test_parse_multi_arguemnt() {
        let data = b"foo bar something\r\n";
        let (bytes, config) = parse(data).unwrap();
        assert_eq!("foo", config.name);
        assert_eq!(
            Args::Multiple(vec!["bar".into(), "something".into()]),
            config.args
        );
        assert_eq!(b"", bytes);
    }

    #[test]
    fn test_real_config() {
        let mut data: &[u8] = b"
        always-show-logo yes
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
        appendonly no
        appendfsync everysec
        no-appendfsync-on-rewrite no
        activerehashing yes
        unixsocket /home/crodas/redis/tests/tmp/server.64463.1/socket
        ";
        vec![
            "always-show-logo",
            "notify-keyspace-events",
            "daemonize",
            "pidfile",
            "port",
            "timeout",
            "bind",
            "loglevel",
            "logfile",
            "databases",
            "latency-monitor-threshold",
            "save",
            "rdbcompression",
            "dbfilename",
            "dir",
            "slave-serve-stale-data",
            "appendonly",
            "appendfsync",
            "no-appendfsync-on-rewrite",
            "activerehashing",
            "unixsocket",
        ]
        .iter()
        .map(|command| {
            let (bytes, config) = parse(data).unwrap();
            assert_eq!(*command, config.name);
            data = bytes;
        })
        .for_each(drop);
    }
}
