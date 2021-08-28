macro_rules! next {
    ($self:ident, $bytes:ident) => {{
        if $bytes.len() > $self.pos {
            let b = unsafe { *$bytes.get_unchecked($self.pos) };
            $self.pos += 1;
            b
        } else {
            return Err(Error::Partial);
        }
    }};
}

macro_rules! read_len {
    ($self:ident, $bytes:ident, $len:ident) => {{
        let len: usize = $len.try_into().unwrap();

        if ($bytes.len() - $self.pos < len) {
            return Err(Error::Partial);
        }

        let start = $self.pos;

        &$bytes[start..start + len]
    }};
}

macro_rules! assert_nl {
    ($self:ident, $bytes:ident) => {{
        if (next!($self, $bytes) != b'\r' || next!($self, $bytes) != b'\n') {
            return Err(Error::NewLine);
        }
    }};
}

macro_rules! read_until {
    ($self:ident, $bytes:ident, $next:expr) => {{
        let start = $self.pos;
        loop {
            if (next!($self, $bytes) == $next) {
                break;
            }
        }
        &$bytes[start..$self.pos - 1]
    }};
}

macro_rules! read_line {
    ($self:ident, $bytes:ident) => {{
        let start = $self.pos;

        read_until!($self, $bytes, b'\r');

        if (next!($self, $bytes) != b'\n') {
            return Err(Error::NewLine);
        }

        &$bytes[start..$self.pos - 2]
    }};
}

macro_rules! read_line_number {
    ($self:ident, $bytes:ident, $type:ident) => {{
        let n = unsafe { std::str::from_utf8_unchecked(read_line!($self, $bytes)) };
        match n.parse::<$type>() {
            Ok(x) => x,
            _ => return Err(Error::InvalidNumber),
        }
    }};
}
