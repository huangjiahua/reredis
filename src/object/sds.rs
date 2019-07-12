use crate::object::ObjectData;

pub type Sds = String;

impl ObjectData for Sds {
    fn sds_ref(&self) -> &str {
        self
    }
}

trait SdsString {
    fn sds_empty() -> Sds;
    fn sds_new_len(len: usize) -> Sds;
    fn sds_len(&self) -> usize;
    fn sds_avail(&self) -> usize;
    fn sds_dup(&self) -> Sds;
    fn sds_clear(&mut self);
    fn sds_cat(&mut self, string: &str) -> &str;
    fn sds_cat_len(&mut self, string: &str, len: usize) -> &str;
    fn sds_cat_sds(&mut self, string: &Sds);
    fn sds_cpy_from(&mut self, string: &str) -> &str;
    fn sds_cpy_len_from(&mut self, string: &str, len: usize) -> &str;
    fn sds_trim(&mut self, string: &str) -> &str;
    fn sds_cmp(&self, string: &str) -> bool;
    fn sds_grow_zero(&mut self, len: usize) -> &str;
    fn sds_range(&mut self, start: usize, end: usize);
}

impl SdsString for Sds {
    fn sds_empty() -> Sds {
        Sds::from("")
    }
    fn sds_new_len(len: usize) -> Sds {
        let mut s = Sds::new();
        s.sds_grow_zero(len);
        s
    }
    fn sds_len(&self) -> usize {
        self.len()
    }
    fn sds_avail(&self) -> usize {
        self.capacity() - self.len()
    }
    fn sds_dup(&self) -> String {
        self.clone()
    }

    fn sds_clear(&mut self) {
        self.clear();
    }

    fn sds_cat(&mut self, string: &str) -> &str {
        self.push_str(string);
        self
    }

    fn sds_cat_len(&mut self, string: &str, len: usize) -> &str {
        self.push_str(&string[0..len]);
        self
    }

    fn sds_cat_sds(&mut self, string: &String) {
        self.push_str(string);
    }

    fn sds_cpy_from(&mut self, string: &str) -> &str {
        self.clear();
        self.push_str(string);
        self
    }

    fn sds_cpy_len_from(&mut self, string: &str, len: usize) -> &str {
        self.clear();
        self.push_str(&string[0..len]);
        self
    }

    fn sds_trim(&mut self, string: &str) -> &str {
        let s = self.trim_matches(|c| { string.contains(c) }).to_string();
        self.sds_cpy_from(&s)
    }

    fn sds_cmp(&self, string: &str) -> bool {
        self == string
    }

    fn sds_grow_zero(&mut self, len: usize) -> &str {
        if self.len() < len {
            let zeroes = vec![0; len - self.len()];
            self.push_str(&String::from_utf8(zeroes).unwrap());
        }
        self
    }

    fn sds_range(&mut self, start: usize, end: usize) {
        let s = self[start..end].to_string();
        self.sds_cpy_from(&s);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_empty_sds() {
        let s = Sds::sds_empty();
        assert_eq!(s, "");
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn create_new_sds_with_len() {
        let s = Sds::sds_new_len(5);
        assert_eq!(s.sds_len(), 5);
    }

    #[test]
    fn get_len_of_sds() {
        let s = Sds::from("hello");
        assert_eq!(s.sds_len(), 5);
        let s = Sds::sds_empty();
        assert_eq!(s.sds_len(), 0);
    }

    #[test]
    fn get_available_space() {
        let s = Sds::sds_empty();
        assert!(s.sds_avail() >= 0);
        let mut s = Sds::from("hello");
        s.sds_clear();
        assert_eq!(s.sds_avail(), 5);
    }

    #[test]
    fn dup_sds() {
        let s = Sds::from("hello");
        let s_dup = s.sds_dup();
        assert_eq!(s, s_dup);
    }

    #[test]
    fn clear_sds() {
        let mut s = Sds::from("hello");
        s.sds_clear();
        assert_eq!(s, Sds::sds_empty());
    }

    #[test]
    fn cat_string() {
        let mut s = Sds::sds_empty();
        s.sds_cat("foo");
        s.sds_cat("bar");
        assert_eq!(s, "foobar");
        let s2 = Sds::from("foo");
        s.sds_cat(&s2);
        assert_eq!(s, "foobarfoo");
        s.sds_cat(&Sds::from("bar"));
        assert_eq!(s, "foobarfoobar");
        s.sds_cat_len("foobarfoobar", 6);
        assert_eq!(s, "foobarfoobarfoobar");
    }

    #[test]
    fn copy_and_replace() {
        let mut s = Sds::from("foo");
        s.sds_cpy_from("bar");
        assert_eq!(s, "bar");
        s.sds_cpy_len_from("foobar", 3);
        assert_eq!(s, "foo");
    }

    #[test]
    fn trim_from_set() {
        let mut s = Sds::from("11foo1");
        s.sds_trim("1");
        assert_eq!(s, "foo");
    }

    #[test]
    fn compare_sds() {
        let s = Sds::from("foo");
        assert!(s.sds_cmp("foo"));
        assert!(s.sds_cmp(&Sds::from("foo")));
        assert!(s.sds_cmp(&s));
        assert!(!s.sds_cmp("bar"));
    }

    #[test]
    fn sds_grow_zero() {
        let mut s = Sds::from("foo");
        s.sds_grow_zero(5);
        assert_eq!(s, "foo\0\0");
    }

    #[test]
    fn sds_keep_range() {
        let mut s = Sds::from("hello, world");
        s.sds_range(0, 5);
        assert_eq!(s, "hello");
    }
}