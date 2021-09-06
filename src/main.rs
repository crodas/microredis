mod commands;
mod db;
mod dispatcher;
mod error;
mod macros;
mod value;

use bytes::{Buf, Bytes, BytesMut};
use dispatcher::Dispatcher;
use futures::SinkExt;
use redis_zero_parser::{parse_server, Error as RedisError};
use std::env;
use std::error::Error;
use std::ops::Deref;
use std::{io, sync::Arc};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use value::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    let db = Arc::new(db::Db::new(12));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = db.clone();
                tokio::spawn(async move {
                    let mut transport = Framed::new(socket, RedisParser);

                    while let Some(result) = transport.next().await {
                        match result {
                            Ok(args) => match Dispatcher::new(&args) {
                                Ok(handler) => {
                                    let r = handler
                                        .deref()
                                        .execute(&db, &args)
                                        .unwrap_or_else(|x| x.into());
                                    transport.send(r).await;
                                }
                                Err(err) => {
                                    let err: Value = err.into();
                                    transport.send(err).await;
                                }
                            },
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }

    Ok(())
}

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
            let (unused, mut val) = match parse_server(src) {
                Ok((buf, val)) => (buf, val),
                Err(RedisError::Partial) => return Ok(None),
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "something")),
            };
            (
                val.iter_mut().map(|e| Bytes::copy_from_slice(e)).collect(),
                src.len() - unused.len(),
            )
        };

        src.advance(proccesed);

        Ok(Some(frame))
    }
}
