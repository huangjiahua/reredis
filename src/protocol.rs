use crate::object::{Robj, RobjPtr};
use crate::util::bytes_to_usize;

pub struct DecodeIter<'a> {
    raw: &'a [u8],
    idx: usize,
    curr: usize,
    total: usize,
    fail: bool,
}

impl<'a> Iterator for DecodeIter<'a> {
    type Item = Result<RobjPtr, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr == self.total || self.fail {
            return None;
        }

        self.fail = true;

        if self.idx + 1 >= self.raw.len() || self.raw[self.idx] != '$' as u8 {
            return Some(Err(()));
        }
        let i = self
            .raw
            .iter()
            .enumerate()
            .skip(self.idx + 1)
            .find(|x| *x.1 == '\r' as u8)
            .map(|x| x.0);

        let i = match i {
            None => return Some(Err(())),
            Some(n) => n,
        };

        let len = &self.raw[self.idx + 1..i];
        let len: usize = match bytes_to_usize(len) {
            Ok(n) => n,
            Err(_) => return Some(Err(())),
        };

        if i + len + 3 >= self.raw.len() {
            return Some(Err(()));
        }

        let next = i + len + 4;
        let this = &self.raw[i..next];
        assert_eq!(this.len(), len + 4);

        if self.raw[next - 2] != '\r' as u8 || self.raw[next - 1] != '\n' as u8 {
            return Some(Err(()));
        }

        let s = &this[2..this.len() - 2];

        self.idx = next;
        self.curr += 1;

        self.fail = false;
        Some(Ok(Robj::create_bytes_object(s)))
    }
}

pub fn decode(raw: &[u8]) -> Result<DecodeIter, ()> {
    if 1 >= raw.len() {
        return Err(());
    }
    if raw[0] != '*' as u8 {
        return Err(());
    }
    let i = raw
        .iter()
        .enumerate()
        .find(|x| *x.1 == '\r' as u8)
        .map(|x| x.0);

    let i = match i {
        None => return Err(()),
        Some(n) => n,
    };

    // won't parse "*0"
    if i == 1 {
        return Err(());
    }

    let total = match bytes_to_usize(&raw[1..i]) {
        Ok(n) => n,
        Err(_) => return Err(()),
    };

    if i + 1 >= raw.len() {
        return Err(());
    }

    let iter = DecodeIter {
        raw,
        idx: i + 2,
        curr: 0,
        total,
        fail: false,
    };
    Ok(iter)
}

#[cfg(test)]
mod test {
    use super::*;

    fn equal(left: &[RobjPtr], right: &[&str]) {
        for p in left.iter().zip(right) {
            assert_eq!(p.0.borrow().string(), (*p.1).as_bytes());
        }
    }

    fn check_right_decode(raw: &[u8], expected: &[&str]) {
        let argv: Vec<RobjPtr> = decode(raw).unwrap().map(|x| x.unwrap()).collect();
        assert_eq!(argv.len(), expected.len());
        equal(&argv, expected);
    }

    fn fail_to_parse(raw: &[u8]) {
        assert!(decode(raw).is_err());
    }

    fn assert_nth_err(raw: &[u8], n: usize) {
        let argv: Vec<Result<RobjPtr, ()>> = decode(raw).unwrap().collect();
        assert_eq!(argv.len(), n + 1);
        for i in 0..n {
            assert!(argv[i].is_ok());
        }
        assert!(argv[n].is_err());
    }

    #[test]
    fn decode_right_command() {
        let cmd = "*1\r\n$7\r\nCOMMAND\r\n".as_bytes();
        check_right_decode(cmd, &["COMMAND"]);

        let cmd = "*2\r\n$3\r\nGET\r\n$1\r\na\r\n".as_bytes();
        check_right_decode(cmd, &["GET", "a"]);

        let cmd = "*3\r\n$3\r\nset\r\n$3\r\nval\r\n$1\r\na\r\n".as_bytes();
        check_right_decode(cmd, &["set", "val", "a"]);

        let cmd = "*2\r\n$3\r\nset\r\n$3\r\nval\r\n$1\r\na\r\n".as_bytes();
        check_right_decode(cmd, &["set", "val"]);

        let cmd = "*5\r\n$5\r\nLPUSH\r\n\
        $4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n$2\r\nab\r\n"
            .as_bytes();
        check_right_decode(cmd, &["LPUSH", "list", "a", "b", "ab"]);

        let cmd = "*0\r\n".as_bytes();
        check_right_decode(cmd, &[]);

        let cmd = "*1\r\n$0\r\n\r\n".as_bytes();
        check_right_decode(cmd, &[""]);
    }

    #[test]
    fn decode_malformed_command() {
        fail_to_parse("".as_bytes());
        fail_to_parse("*".as_bytes());
        fail_to_parse("*0".as_bytes());
        fail_to_parse("*0\r".as_bytes());
        fail_to_parse("*-1\r\n".as_bytes());

        assert_nth_err("*1\r\n$6\r\nCOMMAND\r\n".as_bytes(), 0);

        assert_nth_err("*2\r\n$2\r\nget\r\n$2\r\nti\r\n".as_bytes(), 0);

        assert_nth_err("*1\r\n$8\r\nCOMMAND\r\n".as_bytes(), 0);

        assert_nth_err("*2\r\n$7\r\nCOMMAND\r\n".as_bytes(), 1);
    }
}
