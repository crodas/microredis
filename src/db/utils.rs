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
    pub NX: bool,
    /// Set expiry only when the key has an existing expiry
    pub XX: bool,
    /// Set expiry only when the new expiry is greater than current one
    pub GT: bool,
    /// Set expiry only when the new expiry is less than current one
    pub LT: bool,
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
        assert!(x.NX);
        assert!(x.XX);
        assert!(x.GT);
        assert!(x.LT);
    }

    #[test]
    fn parsing_expiration_2() {
        let opts = vec![Bytes::copy_from_slice(b"nx")];
        let x: ExpirationOpts = opts.as_slice().try_into().unwrap();

        assert!(x.NX);
        assert!(!x.XX);
        assert!(!x.GT);
        assert!(!x.LT);
    }

    #[test]
    fn parsing_expiration_3() {
        let opts = vec![Bytes::copy_from_slice(b"xxx")];
        let x: Result<ExpirationOpts, _> = opts.as_slice().try_into();

        assert!(x.is_err());
    }
}
