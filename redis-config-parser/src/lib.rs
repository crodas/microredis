pub enum Error {
    /// The data is incomplete. This it not an error per-se, but rather a
    /// mechanism to let the caller know they should keep buffering data before
    /// calling the parser again.
    Partial
}

pub enum Value<'a> {
    Single(&'a [u8]),
    Multiple(Vec<&'a [u8]>),
}

pub fn parse<'a>(bytes: &'a [u8]) -> Result<(&'a [u8], &'a [u8], Value<'a>), Error> {
    if bytes[0] == b'\n' {
    }
    Err(Error::Partial)
}
