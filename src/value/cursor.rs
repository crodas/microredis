//! Cursor implementation

use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;
use std::{
    convert::TryFrom,
    io,
    num::{IntErrorKind, ParseIntError},
    str::FromStr,
};
use thiserror::Error;

/// Error
#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("Parsing Error")]
    /// Parsing Int error
    Int(#[from] ParseIntError),
    #[error("I/O Error")]
    /// I/O error
    Io,
}

/// Cursor.
///
/// Redis cursors are stateless. They serialize into a u128 integer information
/// about the latest processed bucket and the last position with a checksum
/// value to make sure the number is valid.
#[derive(Debug, Eq, PartialEq)]
pub struct Cursor {
    checksum: u32,
    /// Current Bucket ID
    pub bucket: u16,
    /// Last position of the key that was processed
    pub last_position: u64,
}

impl Cursor {
    /// Creates a new cursor
    pub fn new(bucket: u16, last_position: u64) -> Result<Self, Error> {
        let mut hasher = Crc32Hasher::new();
        let mut buf = vec![];
        buf.write_u16::<LittleEndian>(bucket)
            .map_err(|_| Error::Io)?;
        buf.write_u64::<LittleEndian>(last_position)
            .map_err(|_| Error::Io)?;
        hasher.update(&buf);
        Ok(Self {
            checksum: hasher.finalize(),
            bucket,
            last_position,
        })
    }

    /// Serializes the cursor a  single u128 integer
    pub fn serialize(&self) -> u128 {
        let bucket: u128 = self.bucket.into();
        let last_position: u128 = self.last_position as u128;
        if bucket == last_position && bucket == 0 {
            return 0;
        }
        let checksum: u128 = self.checksum.into();
        (checksum << 80) | (bucket << 64) | (last_position)
    }
}

impl FromStr for Cursor {
    type Err = Error;

    /// Deserializes a cursor from a string. The string must be a valid number.
    /// If the number is invalid or the checksum is not valid a new cursor with
    /// position 0,0 is returned.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw_number: u128 = u128::from_str(s)?;
        let checksum: u32 = (raw_number >> 80) as u32;
        let cursor = Self::new((raw_number >> 64) as u16, raw_number as u64)?;
        if cursor.checksum == checksum {
            Ok(cursor)
        } else {
            Ok(Self::new(0, 0)?)
        }
    }
}

impl TryFrom<&Bytes> for Cursor {
    type Error = Error;

    fn try_from(v: &Bytes) -> Result<Self, Self::Error> {
        Cursor::from_str(&String::from_utf8_lossy(v))
    }
}

impl ToString for Cursor {
    fn to_string(&self) -> String {
        self.serialize().to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn serialize_end() {
        let x = Cursor::new(0, 0).unwrap();
        assert_eq!("0", x.to_string());
    }

    #[test]
    fn serialize() {
        for e in 0..255 {
            for i in 1..10000 {
                let x = Cursor::new(e, i).unwrap();
                let y = Cursor::from_str(&x.to_string()).unwrap();
                assert_eq!(x, y);
            }
        }
    }
}
