//! # Metrics command handlers
use std::collections::VecDeque;

use crate::{connection::Connection, error::Error, value::Value};
use bytes::Bytes;

/// Dumps metrics from commands. If no argument is passed all commands' metrics are dump.
///
/// The metrics are serialized as JSON.
pub async fn metrics(conn: &Connection, args: VecDeque<Bytes>) -> Result<Value, Error> {
    let dispatcher = conn.all_connections().get_dispatcher();
    let mut result: Vec<Value> = vec![];
    let commands = if args.len() == 1 {
        dispatcher.get_all_commands()
    } else {
        let mut commands = vec![];
        for command in args.into_iter() {
            let command = String::from_utf8_lossy(&command);
            commands.push(dispatcher.get_handler_for_command(&command)?);
        }
        commands
    };

    for command in commands.iter() {
        result.push(command.name().into());
        result.push(
            serde_json::to_string(command.metrics())
                .map_err(|_| Error::Internal)?
                .into(),
        );
    }

    Ok(result.into())
}
