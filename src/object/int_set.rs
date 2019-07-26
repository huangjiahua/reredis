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
}
