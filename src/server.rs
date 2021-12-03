use crate::{
    connection::{connections::Connections, ConnectionStatus},
    db::Db,
    dispatcher::Dispatcher,
    value::Value,
};
use bytes::{Buf, Bytes, BytesMut};
use futures::SinkExt;
use log::{info, trace, warn};
use redis_zero_protocol_parser::{parse_server, Error as RedisError};
use std::{error::Error, io, sync::Arc};
use tokio::{
    net::TcpListener,
    time::{sleep, Duration},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

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

pub async fn serve(addr: String) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);

    let db = Arc::new(Db::new(1000));
    let all_connections = Arc::new(Connections::new(db.clone()));

    let db_for_purging = db.clone();
    tokio::spawn(async move {
        loop {
            db_for_purging.purge();
            sleep(Duration::from_millis(5000)).await;
        }
    });

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let (mut pubsub, conn) = all_connections.new_connection(db.clone(), addr);

                tokio::spawn(async move {
                    let mut transport = Framed::new(socket, RedisParser);

                    trace!("New connection {}", conn.id());

                    loop {
                        tokio::select! {
                            Some(msg) = pubsub.recv() => {
                                if transport.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            result = transport.next() => match result {
                            Some(Ok(args)) => match Dispatcher::new(&args) {
                                Ok(handler) => {
                                    match handler
                                        .execute(&conn, &args)
                                        .await {
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
                                        };

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
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}
