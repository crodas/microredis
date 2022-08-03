//! # Expiration timestamp struct

use super::{bytes_to_int, typ};
use crate::{cmd::now, error::Error};
use std::{convert::TryInto, time::Duration};

/// Expiration timestamp struct
pub struct Expiration {
    millis: u64,
    /// Is the expiration negative?
    pub is_negative: bool,
    command: String,
}

impl Expiration {
    /// Creates a new timestamp from a vector of bytes
    pub fn new(
        bytes: &[u8],
        is_milliseconds: bool,
        is_absolute: bool,
        command: &[u8],
    ) -> Result<Self, Error> {
        let command = String::from_utf8_lossy(command).to_lowercase();
        let input = bytes_to_int::<i64>(bytes)?;
        let millis = if is_milliseconds {
            input
        } else {
            input
                .checked_mul(1_000)
                .ok_or_else(|| Error::InvalidExpire(command.to_string()))?
        };

        let base_time = now().as_millis() as i64;

        let millis = if is_absolute {
            if millis.is_negative() {
                millis.checked_add(base_time)
            } else {
                millis.checked_sub(base_time)
            }
            .ok_or_else(|| Error::InvalidExpire(command.to_string()))?
        } else {
            if millis.checked_add(base_time).is_none() {
                return Err(Error::InvalidExpire(command.to_string()));
            }

            millis
        };

        Ok(Expiration {
            millis: millis.abs() as u64,
            is_negative: millis.is_negative(),
            command: command.to_string(),
        })
    }
}

impl TryInto<Duration> for Expiration {
    type Error = Error;

    fn try_into(self) -> Result<Duration, Self::Error> {
        if self.is_negative {
            Err(Error::InvalidExpire(self.command.to_string()))
        } else {
            Ok(Duration::from_millis(self.millis))
        }
    }
}
