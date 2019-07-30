use std::mem;

const ZIP_LIST_I16_ENC: u8 = 0b1100_0000;
const ZIP_LIST_I32_ENC: u8 = 0b1101_0000;
const ZIP_LIST_I64_ENC: u8 = 0b1110_0000;
const ZIP_LIST_I24_ENC: u8 = 0b1111_0000;
const ZIP_LIST_I8_ENC: u8 = 0b1111_1110;

#[derive(Clone)]
enum Encoding {
    Str(usize),
    Int(i64),
}

impl Encoding {
    fn unwrap_str(&self) -> usize {
        match self {
            Encoding::Str(sz) => *sz,
            _ => panic!("this is an int encoding"),
        }
    }

    fn unwrap_int(&self) -> i64 {
        match self {
            Encoding::Int(v) => *v,
            _ => panic!("this is a str encoding"),
        }
    }

    fn is_str(&self) -> bool {
        match self {
            Encoding::Str(_) => true,
            _ => false,
        }
    }

    fn is_int(&self) -> bool {
        !self.is_str()
    }

    fn blob_len(&self) -> usize {
        match self {
            Encoding::Str(sz) => {
                if *sz < 1 << 6 {
                    return 1;
                }
                if *sz < 1 << 14 {
                    return 2;
                }
                assert!(*sz < 1 << 32);
                5
            }
            Encoding::Int(v) => {
                if *v > 0 && *v < 12 {
                    return 1;
                }
                if *v > std::i8::MIN as i64 && *v < std::i8::MAX as i64 {
                    return 1 + mem::size_of::<i8>();
                }
                if *v > std::i16::MIN as i64 && *v < std::i16::MAX as i64 {
                    return 1 + mem::size_of::<i16>();
                }
                if *v > -(1 << 23) && *v < (1 << 23 - 1) {
                    return 1 + 3;
                }
                if *v > std::i32::MIN as i64 && *v < std::i32::MAX as i64 {
                    return 1 + mem::size_of::<i32>();
                }
                1 + mem::size_of::<i64>()
            }
        }
    }

    fn index(&self, idx: usize) -> u8 {
        match self {
            Encoding::Str(_) => self.index_str(idx),
            Encoding::Int(_) => self.index_int(idx),
        }
    }

    fn index_str(&self, mut idx: usize) -> u8 {
        let len = self.blob_len();
        let mut v = 0;
        assert!(idx < len);
        if idx == 0 {
            match len {
                2 => v |= 0b0100_0000,
                5 => {
                    return 0b1000_0000;
                }
                _ => {}
            }
        }
        v |= ((self.unwrap_str() >> ((len - idx - 1) * 8)) & 0xff);
        v as u8
    }

    fn index_int(&self, idx: usize) -> u8 {
        assert!(idx < self.blob_len());
        if let Encoding::Int(v) = self {
            if idx == 0 {
                if *v > 0 && *v < 12 {
                    return *v as u8 | 0b1111_0000;
                }
                if *v > std::i8::MIN as i64 && *v < std::i8::MAX as i64 {
                    return ZIP_LIST_I8_ENC;
                }
                if *v > std::i16::MIN as i64 && *v < std::i16::MAX as i64 {
                    return ZIP_LIST_I16_ENC;
                }
                if *v > -(1 << 23) && *v < (1 << 23 - 1) {
                    return ZIP_LIST_I24_ENC;
                }
                if *v > std::i32::MIN as i64 && *v < std::i32::MAX as i64 {
                    return ZIP_LIST_I32_ENC;
                }
                return ZIP_LIST_I64_ENC;
            }
            return ((*v >> (self.blob_len() - idx - 1) as i64 * 8) & 0xff) as u8;
        }
        panic!("This is not a str encoding")
    }

    fn iter(&self) -> EncodingIter {
        EncodingIter {
            enc: self.clone(),
            curr: 0,
        }
    }

    fn is_str_enc(x: &[u8]) -> bool {
        x[0] & 0b1100_0000 != 0b1100_0000
    }

    fn is_int_enc(x: &[u8]) -> bool {
        !Self::is_str_enc(x)
    }

    fn parse(x: &[u8]) -> Encoding {
        match Self::is_str_enc(x) {
            true => Self::parse_str_enc(x),
            false => Self::parse_int_enc(x),
        }
    }

    fn parse_str_enc(x: &[u8]) -> Encoding {
        let sz = match x[0] & 0b1100_0000 {
            0b0000_0000 => 1usize,
            0b0100_0000 => 2usize,
            0b1000_0000 => 5usize,
            _ => panic!("not possible"),
        };
        let mut v = x[0] as usize & 0b0011_1111;
        for i in 1..sz {
            v <<= 8;
            v |= x[i] as usize;
        }
        Encoding::Str(v)
    }

    fn parse_int_enc(x: &[u8]) -> Encoding {
        let sz = match x[0] {
            ZIP_LIST_I16_ENC => mem::size_of::<i16>(),
            ZIP_LIST_I32_ENC => mem::size_of::<i32>(),
            ZIP_LIST_I64_ENC => mem::size_of::<i64>(),
            ZIP_LIST_I24_ENC => 3,
            ZIP_LIST_I8_ENC => mem::size_of::<i8>(),
            _ => {
                if x[0] >> 4 != 0b1111 {
                    panic!("not int encoding");
                }
                let k = x[0] & 0x0f;
                assert!(k > 0 && k < 12);
                return Encoding::Int(k as i64);
            }
        };
        let mut v = if x[1] >> 7 == 1 {
            -1i64
        } else {
            0i64
        };
        for i in 0..sz {
            v <<= 8;
            v |= x[i + 1] as i64;
        }
        Encoding::Int(v)
    }
}

struct EncodingIter {
    enc: Encoding,
    curr: usize,
}

impl Iterator for EncodingIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr < self.enc.blob_len() {
            self.curr += 1;
            Some(self.enc.index(self.curr - 1))
        } else {
            None
        }
    }
}

struct Node<'a> {
    prev_raw_len: usize,
    prev_raw_len_size: usize,
    len: usize,
    len_size: usize,
    encoding: Encoding,
    content: &'a mut [u8],
}

impl<'a> Node<'a> {
    fn header_size(&self) -> usize {
        self.prev_raw_len_size + self.len_size
    }
}


// ZipList
// | tail offset: sizeof(usize) | number of nodes: sizeof(u16) | node 1 | node 2 | ... | node N |
struct ZipList(Vec<u8>);

const ZIP_LIST_TAIL_OFF_SIZE: usize = mem::size_of::<usize>();
const ZIP_LIST_LEN_SIZE: usize = mem::size_of::<u16>();
const ZIP_LIST_HEADER_SIZE: usize = mem::size_of::<usize>() + mem::size_of::<u16>();

impl ZipList {
    pub fn new() -> ZipList {
        let mut zl = ZipList(vec![0; ZIP_LIST_HEADER_SIZE]);
        zl.set_tail_offset(zl.blob_len());
        zl.set_len(0);
        zl
    }

    pub fn blob_len(&self) -> usize {
        self.0.len()
    }

    pub fn len(&self) -> usize {
        let mut l = self.get_usize_value(ZIP_LIST_TAIL_OFF_SIZE, ZIP_LIST_LEN_SIZE);
        assert!(l < std::u16::MAX as usize);
        l
    }

    fn set_usize_value(&mut self, value: usize, off: usize, n: usize) {
        assert!(n <= mem::size_of::<usize>());
        for i in 0..n {
            let mut v = value >> (i * 8);
            v &= 0xff;
            self.0[off + i] = v as u8;
        }
    }

    fn set_tail_offset(&mut self, off: usize) {
        self.set_usize_value(off, 0, ZIP_LIST_TAIL_OFF_SIZE);
    }

    fn set_len(&mut self, mut len: usize) {
        if len > std::u16::MAX as usize {
            len = std::u16::MAX as usize;
        }
        self.set_usize_value(len, ZIP_LIST_TAIL_OFF_SIZE, ZIP_LIST_LEN_SIZE);
    }

    fn get_usize_value(&self, off: usize, n: usize) -> usize {
        let mut v = 0usize;
        for i in (0..n).rev() {
            v <<= 8;
            v |= self.0[off + i] as usize;
        }
        v
    }

    fn get_tail_offset(&self) -> usize {
        self.get_usize_value(0, ZIP_LIST_TAIL_OFF_SIZE)
    }

    fn insert(&mut self, off: usize, s: &[u8]) {
        unimplemented!()
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_zip_list() {
        let zl = ZipList::new();
        assert_eq!(zl.blob_len(), ZIP_LIST_HEADER_SIZE);
        assert_eq!(zl.len(), 0);
        assert_eq!(zl.get_tail_offset(), zl.blob_len());
    }

    #[test]
    fn int_encoding() {
        for i in 1..12 {
            let enc = Encoding::Int(i);
            assert_eq!(enc.iter().next().unwrap(), (0xf0 | i) as u8);
        }

        let enc = Encoding::Int(0);
        assert_eq!(enc.index(0), ZIP_LIST_I8_ENC);

        let enc = Encoding::Int(128);
        assert_eq!(enc.index(0), ZIP_LIST_I16_ENC);

        let enc = Encoding::Int(32768);
        assert_eq!(enc.index(0), ZIP_LIST_I24_ENC);

        let enc = Encoding::Int(8388608);
        assert_eq!(enc.index(0), ZIP_LIST_I32_ENC);

        let enc = Encoding::Int(1 << 31);
        assert_eq!(enc.index(0), ZIP_LIST_I64_ENC);
    }

    #[test]
    fn str_encoding() {
        for i in 0..64 {
            let enc = Encoding::Str(i);
            assert_eq!(enc.iter().next().unwrap(), i as u8);
        }

        for i in 64..16383 {
            let enc = Encoding::Str(i);
            let mut arr = [0, 0];
            arr[1] = (i & 0xff) as u8;
            arr[0] = (i >> 8) as u8 | 0b0100_0000;
            for p in enc.iter().zip(arr.iter()) {
                assert_eq!(p.0, *p.1);
            }
        }

        let enc = Encoding::Str(0xffff_ffff);
        let arr = [0x80u8, 0xff, 0xff, 0xff, 0xff];
        for p in enc.iter().zip(arr.iter()) {
            assert_eq!(p.0, *p.1);
        }
    }

    fn single_int_parsing_test(i: i64) {
        let v: Vec<u8> = Encoding::Int(i).iter().collect();
        let value = Encoding::parse(&v).unwrap_int();
        assert_eq!(value, i);
    }

    #[test]
    fn int_enc_parsing() {
        for i in std::i16::MIN as i64..std::i16::MAX as i64 + 1 {
            single_int_parsing_test(i);
        }
        for i in (1 << 23) as i64 - 10000..(1 << 23) as i64 + 10000 {
            single_int_parsing_test(i);
            single_int_parsing_test(-i);
        }
        for i in std::i32::MAX as i64 - 10000..std::i32::MAX as i64 + 10000 {
            single_int_parsing_test(i);
            single_int_parsing_test(-i);
        }
        for i in std::i64::MAX - 10000..=std::i64::MAX {
            single_int_parsing_test(i);
            single_int_parsing_test(-i);
        }
        single_int_parsing_test(std::i64::MIN);
    }

    fn single_str_parsing_test(i: usize) {
        let v: Vec<u8> = Encoding::Str(i).iter().collect();
        let value = Encoding::parse(&v).unwrap_str();
        assert_eq!(value, i);
    }

    #[test]
    fn str_enc_parsing() {
        for i in 0..50000 {
            single_str_parsing_test(i);
        }
        for i in (1 << 32) - 50000..(1 << 32) {
            single_str_parsing_test(i);
        }
    }
}