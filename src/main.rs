use microredis::{error::Error, server};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    env_logger::init();

    server::serve(addr).await
}
