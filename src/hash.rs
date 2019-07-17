use crate::object::{Sds, RobjPtr};
use murmurhash64::murmur_hash64a;

fn sds_hash(data: &str, seed: u64) -> usize {
    murmur_hash64a(data.as_bytes(), seed) as usize
}

fn string_object_hash(object: &RobjPtr, seed: u64) -> usize {
    let object = object.borrow();
    let string = object.string();
    murmur_hash64a(string.as_bytes(), seed) as usize
}

#[cfg(test)]
mod test {
    use crate::object::{Sds, Robj};
    use super::*;

    #[test]
    fn sds_test() {
        let h1 = sds_hash(&Sds::from("hello"), 77);
        let h2 = sds_hash(&Sds::from("hello!"), 77);
        assert_ne!(h1, h2);
    }

    #[test]
    fn string_object_test() {
        let obj1 = Robj::create_string_object("hello");
        let obj2 = Robj::create_string_object("hello!");
        let h1 = string_object_hash(&obj1, 77);
        let h2 = string_object_hash(&obj2, 77);
        assert_ne!(h1, h2);
    }
}