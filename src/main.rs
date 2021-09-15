mod cmd;
mod connection;
mod db;
mod dispatcher;
mod error;
mod macros;
mod server;
mod value;

use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    env_logger::init();

    server::serve(addr).await
}
