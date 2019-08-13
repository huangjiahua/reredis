use crate::object::{RobjPtr, Robj};

pub struct DecodeIter<'a> {
    raw: &'a [u8],
    idx: usize,
    curr: usize,
    total: usize,
}

impl<'a> Iterator for DecodeIter<'a> {
    type Item = Result<RobjPtr, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr == self.total {
            return None;
        }
        if self.idx + 1 >= self.raw.len() || self.raw[self.idx] != '$' as u8 {
            return Some(Err(()));
        }
        let i = self.raw.iter()
            .skip(self.idx + 1)
            .enumerate()
            .find(|x| *x.1 == '\\' as u8)
            .map(|x| x.0);

        let i = match i {
            None => return Some(Err(())),
            Some(n) => n,
        };

        let len = &self.raw[self.idx..i];
        let len: usize = match std::str::from_utf8(len)
            .unwrap()
            .parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Some(Err(())),
        };

        if self.idx + len + 8 >= self.raw.len() {
            return Some(Err(()));
        }

        let next = self.idx + 1 + len + 8;
        let this = &self.raw[self.idx + 2..next];
        assert_eq!(this.len(), len + 8);
        let s = std::str::from_utf8(&this[4..this.len() - 4]).unwrap();

        self.idx += 1 + len + 8;
        self.curr += 1;

        Some(Ok(Robj::create_string_object(s)))
    }
}

pub fn decode(raw: &[u8]) -> Result<DecodeIter, ()> {
    if 1 >= raw.len() {
        return Err(());
    }
    if raw[0] != '*' as u8 {
        return Err(());
    }
    let i = raw.iter()
        .enumerate()
        .find(|x| *x.1 == '$' as u8)
        .map(|x| x.0);

    let i = match i {
        None => return Err(()),
        Some(n) => n,
    };

    if i == 1 {
        return Err(());
    }

    let total = match std::str::from_utf8(&raw[1..i])
        .unwrap()
        .parse::<usize>() {
        Ok(n) => n,
        Err(_) => return Err(()),
    };

    let iter = DecodeIter {
        raw,
        idx: i,
        curr: 0,
        total,
    };
    Ok(iter)
}