pub struct Bytes<'a> {
    pos: usize,
    slice: &'a [u8],
}

impl<'a> Bytes<'a> {
    #[inline]
    pub fn new(slice: &'a [u8]) -> Bytes<'a> {
        Self { pos: 0, slice }
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slice.len()
    }

    #[inline]
    pub fn skip(&mut self, pos: usize) {
        self.pos += pos;
    }

    #[inline]
    pub fn slice_any(&self, start: usize, end: usize) -> &[u8] {
        &self.slice[start..end]
    }
}

impl<'a> AsRef<[u8]> for Bytes<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.slice[self.pos..]
    }
}

impl<'a> Iterator for Bytes<'a> {
    type Item = u8;

    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.slice.len() > self.pos {
            let b = unsafe { *self.slice.get_unchecked(self.pos) };
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pos_and_len() {
        let f: &[u8] = b"foo";
        let b = Bytes::new(f);
        assert_eq!(0, b.pos());
        assert_eq!(3, b.len());
    }

    #[test]
    fn test_iterate() {
        let f: &[u8] = b"foo";
        let mut b = Bytes::new(f);
        assert_eq!(Some(b'f'), b.next());
        assert_eq!(Some(b'o'), b.next());
        assert_eq!(Some(b'o'), b.next());
        assert_eq!(None, b.next());
    }
}
