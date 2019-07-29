use std::mem;

enum EncodingType {
    Str(usize),
    Int(usize),
}

struct Node<'a> {
    prev_entry_len: usize,
    encoding: EncodingType,
    content: &'a str,
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
        zl.set_tail_offset(zl.byte_len());
        zl.set_len(0);
        zl
    }

    pub fn byte_len(&self) -> usize {
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

    fn insert(&mut self, off: usize, s: &str) {}
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_zip_list() {
        let zl = ZipList::new();
        assert_eq!(zl.byte_len(), ZIP_LIST_HEADER_SIZE);
        assert_eq!(zl.len(), 0);
        assert_eq!(zl.get_tail_offset(), zl.byte_len());
    }
}