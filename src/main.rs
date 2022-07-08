use flexi_logger::{FileSpec, Logger};
use microredis::{
    config::{parse, Config},
    error::Error,
    server,
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = if let Some(path) = env::args().nth(1) {
        parse(path).await?
    } else {
        Config::default()
    };

    let logger = Logger::try_with_str(config.log.level.to_string()).unwrap();

    if let Some(log_path) = config.log.file.as_ref() {
        logger
            .log_to_file(FileSpec::try_from(log_path).unwrap())
            .start()
            .unwrap();
    } else {
        logger.start().unwrap();
    }

    server::serve(config).await
}
