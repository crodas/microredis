use crate::error::Error;
use bytes::Bytes;
use std::convert::{TryFrom, TryInto};
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
    pub if_none: bool,
    /// Set expiry only when the key has an existing expiry
    pub replace_only: bool,
    /// Set expiry only when the new expiry is greater than current one
    pub greater_than: bool,
    /// Set expiry only when the new expiry is less than current one
    pub lower_than: bool,
}

impl TryFrom<Vec<Bytes>> for ExpirationOpts {
    type Error = Error;

    fn try_from(args: Vec<Bytes>) -> Result<Self, Self::Error> {
        args.as_slice().try_into()
    }
}

impl TryFrom<&[Bytes]> for ExpirationOpts {
    type Error = Error;

    fn try_from(args: &[Bytes]) -> Result<Self, Self::Error> {
        let mut expiration_opts = Self::default();
        for arg in args.iter() {
            match String::from_utf8_lossy(arg).to_uppercase().as_str() {
                "NX" => expiration_opts.if_none = true,
                "XX" => expiration_opts.replace_only = true,
                "GT" => expiration_opts.greater_than = true,
                "LT" => expiration_opts.lower_than = true,
                invalid => return Err(Error::UnsupportedOption(invalid.to_owned())),
            }
        }
        Ok(expiration_opts)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn parsing_expiration_1() {
        let opts = vec![
            Bytes::copy_from_slice(b"nx"),
            Bytes::copy_from_slice(b"Xx"),
            Bytes::copy_from_slice(b"GT"),
            Bytes::copy_from_slice(b"lT"),
        ];
        let x: ExpirationOpts = opts.as_slice().try_into().unwrap();
        assert!(x.if_none);
        assert!(x.replace_only);
        assert!(x.greater_than);
        assert!(x.lower_than);
    }

    #[test]
    fn parsing_expiration_2() {
        let opts = vec![Bytes::copy_from_slice(b"nx")];
        let x: ExpirationOpts = opts.as_slice().try_into().unwrap();

        assert!(x.if_none);
        assert!(!x.replace_only);
        assert!(!x.greater_than);
        assert!(!x.lower_than);
    }

    #[test]
    fn parsing_expiration_3() {
        let opts = vec![Bytes::copy_from_slice(b"xxx")];
        let x: Result<ExpirationOpts, _> = opts.as_slice().try_into();

        assert!(x.is_err());
    }
}
