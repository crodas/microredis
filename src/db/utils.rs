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
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ExpirationOpts {
    /// Set expiry only when the key has no expiry
    NX,
    /// Set expiry only when the key has an existing expiry
    XX,
    /// Set expiry only when the new expiry is greater than current one
    GT,
    /// Set expiry only when the new expiry is less than current one
    LT,
}

impl TryFrom<&bytes::Bytes> for ExpirationOpts {
    type Error = Error;

    fn try_from(bytes: &bytes::Bytes) -> Result<Self, Self::Error> {
        match String::from_utf8_lossy(bytes).to_uppercase().as_str() {
            "NX" => Ok(Self::NX),
            "XX" => Ok(Self::XX),
            "GT" => Ok(Self::GT),
            "LT" => Ok(Self::LT),
            _ => Err(Error::Syntax),
        }
    }
}
