mod dispatcher;
mod db;
mod macros;
mod value;

use bytes::{Buf, BytesMut};
use dispatcher::Dispatcher;
use futures::SinkExt;
use redis_zero_parser::{parse, Error as RedisError};
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::ops::Deref;
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};
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

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    let mut transport = Framed::new(socket, RedisParser);

                    while let Some(result) = transport.next().await {
                        match result {
                            Ok(Value::Array(args)) => match Dispatcher::new(&args[0]) {
                                Ok(handler) => {
                                    let r = handler.deref().execute(&args);
                                    transport.send(r.unwrap()).await;
                                }
                                Err(err) => {
                                    println!("invalid command {:?}", err);
                                }
                            },
                            Ok(x) => {
                                println!("Invalid message {:?}", x);
                            }
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
    type Item = Value;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        let (frame, proccesed) = {
            let (unused, val) = match parse(src) {
                Ok((buf, val)) => (buf, val),
                Err(RedisError::Partial) => return Ok(None),
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "something")),
            };
            (Value::try_from(&val).unwrap(), src.len() - unused.len())
        };

        src.advance(proccesed);

        Ok(Some(frame))
    }
}
