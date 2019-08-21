use crate::object::RobjPtr;
use murmurhash64::murmur_hash64a;

pub fn string_object_hash(object: &RobjPtr, seed: u64) -> usize {
    let object = object.borrow();
    let string = object.string();
    murmur_hash64a(string, seed) as usize
}

#[cfg(test)]
mod test {
    use crate::object::Robj;
    use super::*;

    #[test]
    fn string_object_test() {
        let obj1 = Robj::create_bytes_object(b"hello");
        let obj2 = Robj::create_bytes_object(b"hello!");
        let h1 = string_object_hash(&obj1, 77);
        let h2 = string_object_hash(&obj2, 77);
        assert_ne!(h1, h2);
    }
}