use crate::{connection::Connections, db::Db, dispatcher::Dispatcher, value::Value};
use bytes::{Buf, Bytes, BytesMut};
use futures::SinkExt;
use log::{info, trace, warn};
use redis_zero_protocol_parser::{parse_server, Error as RedisError};
use std::{error::Error, io, ops::Deref, sync::Arc};
use tokio::net::TcpListener;
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
    let mut all_connections = Connections::new();

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let conn = all_connections.new_connection(db.clone(), addr);

                tokio::spawn(async move {
                    let mut transport = Framed::new(socket, RedisParser);

                    trace!("New connection {}", conn.lock().unwrap().id());

                    while let Some(result) = transport.next().await {
                        match result {
                            Ok(args) => match Dispatcher::new(&args) {
                                Ok(handler) => {
                                    let r = handler
                                        .deref()
                                        .execute(&mut conn.lock().unwrap(), &args)
                                        .unwrap_or_else(|x| x.into());
                                    if transport.send(r).await.is_err() {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    if transport.send(err.into()).await.is_err() {
                                        break;
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("error on decoding from socket; error = {:?}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}
