//! # Server
//!
//! Redis TCP server. This module also includes a simple HTTP server to dump the prometheus
//! metrics.
use crate::{
    config::Config,
    connection::{connections::Connections, Connection},
    db::{pool::Databases, Db},
    dispatcher::Dispatcher,
    error::Error,
    value::Value,
};
use bytes::{Buf, Bytes, BytesMut};
use futures::{future, SinkExt};
use log::{info, trace, warn};
use redis_zero_protocol_parser::{parse_server, Error as RedisError};
use std::{collections::VecDeque, io, sync::Arc};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{sleep, Duration},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Redis Parser Encoder/Decoder
struct RedisParser;

impl Encoder<Value> for RedisParser {
    type Error = io::Error;

    fn encode(&mut self, response: Value, dst: &mut BytesMut) -> io::Result<()> {
        let v: Vec<u8> = response.into();
        dst.extend_from_slice(&v);
        Ok(())
    }
}

impl Decoder for RedisParser {
    type Item = VecDeque<Bytes>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        let (frame, proccesed) = {
            let (unused, val) = match parse_server(src) {
                Ok((buf, val)) => (buf, val),
                Err(RedisError::Partial) => return Ok(None),
                Err(e) => {
                    log::debug!("{:?}", e);

                    return Err(io::Error::new(io::ErrorKind::Other, "something"));
                }
            };
            (
                val.iter().map(|e| Bytes::copy_from_slice(e)).collect(),
                src.len() - unused.len(),
            )
        };

        src.advance(proccesed);

        Ok(Some(frame))
    }
}

/// Spawn a very simple HTTP server to serve metrics.
///
/// The incoming HTTP request is discarded and the response is always the metrics in a prometheus
/// format
async fn server_metrics(all_connections: Arc<Connections>) -> Result<(), Error> {
    info!("Listening on 127.0.0.1:7878 for metrics");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:7878")
        .await
        .expect("Failed to start metrics server");

    let mut globals = std::collections::HashMap::new();
    globals.insert("service", "microredis");

    loop {
        let (mut stream, _) = listener.accept().await.expect("accept client");
        let mut buf = vec![0; 1024];

        let _ = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(_) => continue,
        };

        let serialized = serde_prometheus::to_string(
            &all_connections
                .get_dispatcher()
                .get_service_metric_registry(),
            Some("redis"),
            globals.clone(),
        )
        .unwrap_or_else(|_| "".to_owned());

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            serialized.len(),
            serialized
        );

        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;
    }
}

/// Spawn the TCP/IP micro-redis server.
async fn serve_tcp(
    addr: &str,
    default_db: Arc<Db>,
    all_connections: Arc<Connections>,
) -> Result<(), Error> {
    let listener = TcpListener::bind(addr).await?;
    info!("Starting server {}", addr);
    info!("Ready to accept connections on {}", addr);
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let transport = Framed::new(socket, RedisParser);
                let all_connections = all_connections.clone();
                let default_db = default_db.clone();

                tokio::spawn(async move {
                    handle_new_connection(transport, all_connections, default_db, addr).await;
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

#[cfg(unix)]
async fn serve_unixsocket(
    file: &str,
    default_db: Arc<Db>,
    all_connections: Arc<Connections>,
) -> Result<(), Error> {
    use std::fs::remove_file;

    info!("Ready to accept connections on unix://{}", file);
    let _ = remove_file(file);
    let listener = UnixListener::bind(file)?;
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let transport = Framed::new(socket, RedisParser);
                let all_connections = all_connections.clone();
                let default_db = default_db.clone();

                tokio::spawn(async move {
                    handle_new_connection(
                        transport,
                        all_connections,
                        default_db,
                        addr.as_pathname()
                            .and_then(|p| p.to_str())
                            .unwrap_or_default(),
                    )
                    .await;
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

#[inline]
async fn execute_command(
    conn: &Connection,
    dispatcher: &Dispatcher,
    args: VecDeque<Bytes>,
) -> Option<Value> {
    match dispatcher.execute(conn, args).await {
        Ok(result) => Some(result),
        Err(Error::EmptyLine) => Some(Value::Ignore),
        Err(Error::Quit) => None,
        Err(err) => Some(err.into()),
    }
}

/// Handles a new connection
///
/// The new connection can be created from a new TCP or Unix stream.
#[inline]
async fn handle_new_connection<T: AsyncReadExt + AsyncWriteExt + Unpin, A: ToString>(
    mut transport: Framed<T, RedisParser>,
    all_connections: Arc<Connections>,
    default_db: Arc<Db>,
    addr: A,
) {
    let (mut pubsub, conn) = all_connections.new_connection(default_db, addr);
    let dispatcher = all_connections.get_dispatcher();
    // Commands are being buffered when the client is blocked.
    let mut buffered_commands: Vec<VecDeque<Bytes>> = vec![];
    trace!("New connection {}", conn.id());

    loop {
        tokio::select! {
            Some(msg) = pubsub.recv() => {
                // Pub-sub message
                if transport.send(msg).await.is_err() {
                    break;
                }
                'outer: for args in buffered_commands.iter() {
                    // Client sent commands while the connection was blocked,
                    // now it is time to process them one by one
                    match execute_command(&conn, &dispatcher, args.clone()).await {
                        Some(result) => if result != Value::Ignore && transport.send(result).await.is_err() {
                            break 'outer;
                        },
                        None => {
                            let _ = transport.send(Value::Ok).await;
                            break 'outer;
                        }
                    }
                }
                buffered_commands.clear();
            }
            result = transport.next() => match result {
                Some(Ok(args)) => {
                        if conn.is_blocked() {
                            buffered_commands.push(args);
                            continue;
                        }
                        match execute_command(&conn, &dispatcher, args).await {
                            Some(result) => if result != Value::Ignore && transport.send(result).await.is_err() {
                               break;
                            },
                            None => {
                                let _ = transport.send(Value::Ok).await;
                                break;
                            }
                        };
                },
                Some(Err(e)) => {
                    warn!("error on decoding from socket; error = {:?}", e);
                    break;
                },
                None => break,
            }
        }
    }
    conn.destroy();
}

/// Spawn redis server
///
/// Spawn a redis server. This function will create Connections object, the in-memory database, the
/// purge process and the TCP server.
///
/// This process is also listening for any incoming message through the internal pub-sub.
///
/// This function will block the main thread and will never exit.
pub async fn serve(config: Config) -> Result<(), Error> {
    let (default_db, all_dbs) = Databases::new(16, 1000);
    let all_connections = Arc::new(Connections::new(all_dbs.clone()));
    let all_connections_for_metrics = all_connections.clone();

    all_dbs
        .into_iter()
        .map(|db_for_purging| {
            tokio::spawn(async move {
                loop {
                    db_for_purging.purge();
                    sleep(Duration::from_millis(5000)).await;
                }
            });
        })
        .for_each(drop);

    let mut services = vec![tokio::spawn(async move {
        server_metrics(all_connections_for_metrics).await
    })];

    config
        .get_tcp_hostnames()
        .iter()
        .map(|host| {
            let default_db = default_db.clone();
            let all_connections = all_connections.clone();
            let host = host.clone();
            services.push(tokio::spawn(async move {
                serve_tcp(&host, default_db, all_connections).await
            }));
        })
        .for_each(drop);

    #[cfg(unix)]
    if let Some(file) = config.unixsocket {
        services.push(tokio::spawn(async move {
            serve_unixsocket(&file, default_db, all_connections).await
        }))
    }

    future::join_all(services).await;

    Ok(())
}
