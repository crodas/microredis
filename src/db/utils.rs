use crate::error::Error;
use std::convert::TryFrom;
use tokio::time::{Duration, Instant};

pub(crate) fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(86400 * 365 * 30)
}

/// Override database entries
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Override {
    /// Allow override
    Yes,
    /// Do not allow override, only new entries
    No,
    /// Allow only override
    Only,
}

impl From<bool> for Override {
    fn from(v: bool) -> Override {
        if v {
            Override::Yes
        } else {
            Override::No
        }
    }
}

impl Default for Override {
    fn default() -> Self {
        Self::Yes
    }
}

/// Override database entries
#[derive(PartialEq, Debug, Clone, Copy, Default)]
pub struct ExpirationOpts {
    /// Set expiry only when the key has no expiry
    pub NX: bool,
    /// Set expiry only when the key has an existing expiry
    pub XX: bool,
    /// Set expiry only when the new expiry is greater than current one
    pub GT: bool,
    /// Set expiry only when the new expiry is less than current one
    pub LT: bool,
}

impl TryFrom<&[bytes::Bytes]> for ExpirationOpts {
    type Error = Error;

    fn try_from(args: &[bytes::Bytes]) -> Result<Self, Self::Error> {
        let mut expiration_opts = Self::default();
        for arg in args.iter() {
            match String::from_utf8_lossy(arg).to_uppercase().as_str() {
                "NX" => expiration_opts.NX = true,
                "XX" => expiration_opts.XX = true,
                "GT" => expiration_opts.GT = true,
                "LT" => expiration_opts.LT = true,
                invalid => return Err(Error::UnsupportedOption(invalid.to_owned())),
            }
        }
        Ok(expiration_opts)
    }
}
