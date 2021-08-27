macro_rules! next {
    ($bytes:ident) => {{
        match $bytes.next() {
            Some(b) => b,
            None => return Ok(Status::Partial),
        }
    }};
}

macro_rules! read_len {
    ($bytes:ident, $len:ident) => {{
        let len: usize = $len.try_into().unwrap();

        if ($bytes.len() - $bytes.pos() < len) {
            return Ok(Status::Partial);
        }

        let start = $bytes.pos();

        $bytes.skip(len);

        $bytes.slice_any(start, start + len)
    }};
}

macro_rules! assert_nl {
    ($bytes:ident) => {{
        if (next!($bytes) != b'\r' || next!($bytes) != b'\n') {
            return Err(Error::NewLine);
        }
    }};
}

macro_rules! read_until {
    ($bytes:ident, $next:expr) => {{
        let start = $bytes.pos();
        loop {
            if (next!($bytes) == $next) {
                break;
            }
        }
        $bytes.slice_any(start, $bytes.pos() - 1)
    }};
}

macro_rules! read_line {
    ($bytes:ident) => {{
        let start = $bytes.pos();

        read_until!($bytes, b'\r');

        if (next!($bytes) != b'\n') {
            return Err(Error::NewLine);
        }

        $bytes.slice_any(start, $bytes.pos() - 2)
    }};
}

macro_rules! read_line_number {
    ($bytes:ident, $type:ident) => {{
        let n = unsafe { std::str::from_utf8_unchecked(read_line!($bytes)) };
        match n.parse::<$type>() {
            Ok(x) => x,
            _ => return Err(Error::InvalidNumber),
        }
    }};
}
