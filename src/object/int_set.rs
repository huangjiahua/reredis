use std::mem;
use std::convert::TryInto;

pub struct IntSet(Vec<u8>);

type Encoding = u8;

const INT_SET_ENC_INT16: Encoding = mem::size_of::<i16>() as Encoding;
const INT_SET_ENC_INT32: Encoding = mem::size_of::<i32>() as Encoding;
const INT_SET_ENC_INT64: Encoding = mem::size_of::<i64>() as Encoding;

pub struct Iter<'a> {
    set: &'a IntSet,
    idx: usize,
}

impl IntSet {
    pub fn new() -> IntSet {
        let mut int_set = vec![0u8; 4];
        int_set[0] = INT_SET_ENC_INT16;
        IntSet(int_set)
    }

    pub fn iter(&self) -> Iter {
        Iter {
            set: self,
            idx: 0,
        }
    }

    pub fn add(&mut self, value: i64) -> Result<(), ()> {
        let val_enc = Self::value_encoding(value);
        if val_enc > self.encoding() {
            return self.upgrade_and_add(value);
        }

        let pos = self.search(value);
        if let Ok(_) = pos {
            return Err(());
        }

        let pos = pos.unwrap_err();

        self.resize(self.len() + 1);

        self.move_tail(pos, pos + 1);

        self.set(pos, value);

        Ok(())
    }

    fn upgrade_and_add(&mut self, value: i64) -> Result<(), ()> {
        let old_len = self.len();
        let val_enc = Self::value_encoding(value);
        let new_vec_len = (self.len() + 1) * val_enc as usize + 4;

        self.0.resize(new_vec_len, 0);

        let (j, k) = if value < self.get(0) {
            (1, 0)
        } else {
            (0, old_len)
        };

        for i in (0..old_len).rev() {
            let v = self.get(i);
            self.set_by_enc(i + j, val_enc, v);
        }

        self.set_by_enc(k, val_enc, value);

        self.0[0] = val_enc;

        Ok(())
    }

    fn resize(&mut self, _len: usize) {}

    fn move_tail(&mut self, from: usize, to: usize) {
        let from = self.true_pos(from);
        let to = self.true_pos(to);
        if to > from {
            let diff = to - from;
            self.0.resize(diff + self.0.len(), 0);
            for p in (to..self.0.len()).rev().map(|x| (x, x - diff)) {
                self.0.swap(p.0, p.1);
            }
        } else if from > to {
            let diff = from - to;
            if from < self.0.len() {
                for p in (from..self.0.len()).map(|x| (x, x - diff)) {
                    self.0.swap(p.0, p.1);
                }
            }
            self.0.resize(self.0.len() - diff, 0);
        }
    }

    fn set(&mut self, pos: usize, value: i64) {
        self.set_by_enc(pos, self.encoding(), value);
    }

    fn set_by_enc(&mut self, pos: usize, enc: Encoding, value: i64) {
        let pos = Self::true_pos_enc(pos, enc);

        for i in 0..(enc as usize) {
            let mut v = value >> (i * 8) as i64;
            v &= 0xff;
            self.0[pos + i] = v as u8;
        }
    }

    pub fn get(&self, pos: usize) -> i64 {
        self.get_by_enc(pos, self.encoding())
    }

    fn get_by_enc(&self, pos: usize, enc: Encoding) -> i64 {
        let pos = Self::true_pos_enc(pos, enc);
        let enc = enc as usize;
        let mut v = if self.0[pos + enc - 1] > 127 {
            -1
        } else {
            0
        };

        for i in (0..enc).rev() {
            v <<= 8;
            let k = self.0[pos + i] as i64;
            v |= k;
        }

        v
    }

    pub fn find(&self, value: i64) -> bool {
        self.search(value).is_ok()
    }

    fn search(&self, value: i64) -> Result<usize, usize> {
        if self.len() == 0 {
            return Err(0);
        } else {
            if value < self.get(0) {
                return Err(0);
            } else if self.get(self.len() - 1) < value {
                return Err(self.len());
            }
        }

        let mut min = 0;
        let mut max = self.len() - 1;
        let mut mid = 0;
        let mut mid_value = 0;

        while min <= max {
            mid = min + (max - min) / 2;
            mid_value = self.get(mid);
            if value < mid_value {
                max = mid - 1;
            } else if mid_value < value {
                min = mid + 1;
            } else {
                break;
            }
        }

        if mid_value == value {
            return Ok(mid);
        }
        Err(min)
    }

    pub fn remove(&mut self, value: i64) -> Result<(), ()> {
        let enc = self.encoding();

        if Self::value_encoding(value) > enc {
            return Err(());
        }

        let pos = self.search(value);

        if let Err(_) = pos {
            return Err(());
        }

        let pos = pos.unwrap();

        self.move_tail(pos + 1, pos);

        Ok(())
    }

    pub fn len(&self) -> usize {
        let enc = self.0[0] as usize;
        (self.0.len() - 4) / enc
    }

    pub fn blob_len(&self) -> usize {
        self.0.len()
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
        Self::true_pos_enc(pos, self.encoding())
    }

    fn true_pos_enc(pos: usize, enc: Encoding) -> usize {
        pos * (enc as usize) + 4
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.set.len() {
            return None;
        }
        let i = self.set.get(self.idx);
        self.idx += 1;
        Some(i)
    }
}

#[cfg(target_endian = "little")]
fn _serialize_i64(i: i64) -> [u8; mem::size_of::<i64>()] {
    i.to_le_bytes()
}

#[cfg(target_endian = "little")]
fn _i64_from(b: &[u8]) -> i64 {
    let (int_bytes, _) = b.split_at(std::mem::size_of::<i64>());
    i64::from_le_bytes(int_bytes.try_into().unwrap())
}

#[cfg(target_endian = "big")]
fn _serialize_i64(i: i64) -> [u8; mem::size_of::<i64>()] {
    i.to_be_bytes()
}

#[cfg(target_endian = "big")]
fn _i64_from(b: &[u8]) -> i64 {
    let (int_bytes, _) = b.split_at(std::mem::size_of::<i64>());
    i64::from_be_bytes(int_bytes.try_into().unwrap())
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
        set.add(1).unwrap();
        set.add(2).unwrap();
        assert_eq!(set.get(0), 1);
        assert_eq!(set.get(1), 2);
        set.add(32767).unwrap();
        set.add(-32768).unwrap();
        assert_eq!(set.get(0), -32768);
        assert_eq!(set.get(3), 32767);

        set.add(32768).unwrap();
        assert_eq!(set.get(0), -32768);
        assert_eq!(set.get(1), 1);
        assert_eq!(set.get(2), 2);
        assert_eq!(set.get(3), 32767);
        assert_eq!(set.get(4), 32768);

        set.add(std::i64::MAX).unwrap();
        assert_eq!(set.get(0), -32768);
        assert_eq!(set.get(1), 1);
        assert_eq!(set.get(2), 2);
        assert_eq!(set.get(3), 32767);
        assert_eq!(set.get(4), 32768);
        assert_eq!(set.get(5), std::i64::MAX);

        set.add(1).unwrap_err();
    }

    fn simple_set_get(v: i64) {
        let mut set = IntSet::new();
        set.move_tail(0, 1);
        set.set(0, v);
        assert_eq!(set.get(0), v);
    }

    #[test]
    fn set_and_get() {
        for i in 0..i16::MAX as i64 {
            simple_set_get(i);
        }
    }

    #[test]
    fn search_test() {
        let mut set = IntSet::new();
        for i in (0..100).rev() {
            set.move_tail(0, 1);
            set.set(0, i);
            assert_eq!(set.get(0), i);
        }

        for i in 0..100 {
            let r = set.search(i).unwrap();
            assert_eq!(set.get(r), i);
        }

        for i in 101..200 {
            let r = set.search(i).unwrap_err();
            assert_eq!(r, set.len());
        }
    }

    #[test]
    fn find_test() {
        let mut set = IntSet::new();
        for i in 0..100 {
            set.add(i).unwrap();
            set.add(i).unwrap_err();
        }

        for i in 0..100 {
            set.add(std::i32::MIN as i64 + i).unwrap();
            set.add(std::i32::MIN as i64 + i).unwrap_err();
        }

        for i in 0..100 {
            set.add(std::i32::MAX as i64 + i).unwrap();
            set.add(std::i32::MAX as i64 + i).unwrap_err();
        }

        for i in 0..100 {
            assert!(set.find(i));
            assert!(set.find(std::i32::MIN as i64 + i));
            assert!(set.find(std::i32::MAX as i64 + i));
            assert!(!set.find(-i - 1));
        }
    }

    #[test]
    fn remove_test() {
        let mut set = IntSet::new();
        for i in 0..100 {
            set.add(i).unwrap();
        }

        for i in 0..100 {
            set.add(std::i32::MIN as i64 + i).unwrap();
        }

        for i in 0..100 {
            set.add(std::i32::MAX as i64 + i).unwrap();
        }

        for i in 0..100 {
            assert!(set.find(i));
            set.remove(i).unwrap();
            assert!(!set.find(i));

            assert!(set.find(std::i32::MIN as i64 + i));
            set.remove(std::i32::MIN as i64 + i).unwrap();
            assert!(!set.find(std::i32::MIN as i64 + i));

            assert!(set.find(std::i32::MAX as i64 + i));
            set.remove(std::i32::MAX as i64 + i).unwrap();
            assert!(!set.find(std::i32::MAX as i64 + i));

            assert!(!set.find(-i - 1));
            set.remove(-i - 1).unwrap_err();
        }
    }
}
