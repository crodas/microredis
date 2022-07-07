//! # Server
//!
//! Redis TCP server. This module also includes a simple HTTP server to dump the prometheus
//! metrics.
use crate::{
    connection::{connections::Connections, Connection, ConnectionStatus},
    db::{pool::Databases, Db},
    error::Error,
    value::Value,
};
use bytes::{Buf, Bytes, BytesMut};
use futures::{channel::mpsc::Receiver, future, SinkExt};
use log::{info, trace, warn};
use redis_zero_protocol_parser::{parse_server, Error as RedisError};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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
    type Item = Vec<Bytes>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        let (frame, proccesed) = {
            let (unused, val) = match parse_server(src) {
                Ok((buf, val)) => (buf, val),
                Err(RedisError::Partial) => return Ok(None),
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "something")),
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

    Ok(())
}

/// Spawn the TCP/IP micro-redis server.
async fn serve_tcp(
    addr: String,
    default_db: Arc<Db>,
    all_connections: Arc<Connections>,
) -> Result<(), Error> {
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let mut transport = Framed::new(socket, RedisParser);
                let all_connections = all_connections.clone();
                let default_db = default_db.clone();

                tokio::spawn(async move {
                    handle_new_connection(transport, all_connections, default_db, addr).await;
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }

    Ok(())
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
    trace!("New connection {}", conn.id());

    loop {
        tokio::select! {
            Some(msg) = pubsub.recv() => {
                if transport.send(msg).await.is_err() {
                    break;
                }
            }
            result = transport.next() => match result {
                Some(Ok(args)) => match all_connections.get_dispatcher().execute(&conn, &args).await {
                    Ok(result) => {
                        if conn.status() == ConnectionStatus::Pubsub {
                            continue;
                        }
                        if transport.send(result).await.is_err() {
                            break;
                        }
                    },
                    Err(err) => {
                        if transport.send(err.into()).await.is_err() {
                            break;
                        }
                    }
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
/// Spawn a redis server. This function will create Conections object, the in-memory database, the
/// purge process and the TCP server.
///
/// This process is also listening for any incoming message through the internal pub-sub.
///
/// This function will block the main thread and will never exit.
pub async fn serve(addr: String) -> Result<(), Error> {
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

    services.push(tokio::spawn(async move {
        serve_tcp(addr, default_db, all_connections).await
    }));

    future::join_all(services).await;

    Ok(())
}
