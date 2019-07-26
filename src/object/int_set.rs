use std::mem;

pub struct IntSet(Vec<i8>);

type Encoding = i8;

const INT_SET_ENC_INT16: Encoding = mem::size_of::<i16>() as Encoding;
const INT_SET_ENC_INT32: Encoding = mem::size_of::<i32>() as Encoding;
const INT_SET_ENC_INT64: Encoding = mem::size_of::<i64>() as Encoding;

impl IntSet {
    pub fn new() -> IntSet {
        let mut int_set = vec![0i8; 4];
        int_set[0] = INT_SET_ENC_INT16;
        IntSet(int_set)
    }

    pub fn add(&mut self, value: i64) -> Result<(), ()> {
        let val_enc = Self::value_encoding(value);
        if val_enc > self.encoding() {
            return self.upgrade_and_add(value);
        }

        let pos = self.search(value);
        if let None = pos {
            return Err(());
        }

        let pos = pos.unwrap();
        let old_len = self.len();

        self.resize(self.len() + 1);

        if pos < old_len {
            self.move_tail(pos, pos + 1);
        }

        self.set(pos, value);

        Ok(())
    }

    fn upgrade_and_add(&mut self, value: i64) -> Result<(), ()> {
        unimplemented!()
    }

    fn resize(&mut self, len: usize) {}

    fn move_tail(&mut self, from: usize, to: usize) {
        assert!(to > from);
        let from = self.true_pos(from);
        let to = self.true_pos(to);
        let diff = to - from;
        self.0.resize(diff + self.0.len(), 0);

        for p in (to..self.0.len()).rev().map(|x| (x, x - diff)) {
            self.0.swap(p.0, p.1);
        }
    }

    fn set(&mut self, pos: usize, value: i64) {
        let pos = self.true_pos(pos);

        for i in 0..(self.encoding() as usize) / mem::size_of::<i8>() {
            let mut v = value >> (i * mem::size_of::<i8>()) as i64;
            v &= 0xff;
            assert!(v < std::i8::MAX as i64);
            self.0[pos + i] = v as i8;
        }
    }

    fn get(&self, pos: usize) -> i64 {
        let pos = self.true_pos(pos);
        let mut v = 0i64;

        for i in (0..(self.encoding() as usize) / mem::size_of::<i8>()).rev() {
            v <<= mem::size_of::<i8>() as i64;
            v |= self.0[pos + i] as i64;
        }

        v
    }

    pub fn search(&self, value: i64) -> Option<usize> {
        unimplemented!()
    }


    pub fn len(&self) -> usize {
        let enc = self.0[0] as usize;
        (self.0.len() - 4) / enc
    }

    fn encoding(&self) -> Encoding {
        self.0[0]
    }

    fn value_encoding(value: i64) -> Encoding {
        if value < std::i32::MIN as i64 || value > std::i32::MAX as i64 {
            return INT_SET_ENC_INT64;
        }
        if value < std::i16::MIN as i64 || value > std::i16::MAX as i64 {
            return INT_SET_ENC_INT32;
        }
        INT_SET_ENC_INT16
    }

    fn true_pos(&self, pos: usize) -> usize {
        pos * (self.encoding() as usize) + 4
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{i16, i32, i64};

    #[test]
    fn new_int_set() {
        let set = IntSet::new();
        assert_eq!(set.len(), 0);
        assert_eq!(set.encoding(), INT_SET_ENC_INT16);
    }

    #[test]
    fn encoding() {
        assert_eq!(IntSet::value_encoding(0), INT_SET_ENC_INT16);
        assert_eq!(IntSet::value_encoding(9999), INT_SET_ENC_INT16);
        assert_eq!(IntSet::value_encoding((i16::MAX as i64) + 1), INT_SET_ENC_INT32);
        assert_eq!(IntSet::value_encoding(i32::MAX as i64), INT_SET_ENC_INT32);
        assert_eq!(IntSet::value_encoding((i32::MAX as i64) + 1), INT_SET_ENC_INT64);
    }

    #[test]
    fn move_tail_test() {
        let mut set = IntSet::new();
        set.move_tail(0, 4);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn simple_add() {
        let mut set = IntSet::new();
        set.add(1);
        set.add(2);
    }

    fn simple_set_get(v: i64) {
        let mut set = IntSet::new();
        set.move_tail(0, 1);
        set.set(0, 1);
        assert_eq!(set.get(0), 1);
    }

    #[test]
    fn set_and_get() {
        for i in 0..i16::MAX {
            simple_set_get(i);
        }
    }
}
