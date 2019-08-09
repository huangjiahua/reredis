use crate::object::{Robj, RobjPtr};
use crate::object::dict::Dict;
use crate::hash::string_object_hash;
use rand::Rng;

struct DB {
    pub dict: Dict<RobjPtr, RobjPtr>,
    pub expires: Dict<RobjPtr, RobjPtr>,
}

impl DB {
    pub fn new() -> DB {
        let mut rng = rand::thread_rng();
        DB {
            dict: Dict::new(string_object_hash, rng.gen()),
            expires: Dict::new(string_object_hash, rng.gen()),
        }
    }

    pub fn add(&mut self, key: RobjPtr, value: RobjPtr) {
        self.dict.add(key, value).unwrap();
    }

    pub fn look_up(&mut self, key: &RobjPtr) -> Option<&RobjPtr> {
        let r = self.dict.find(key);
        match r {
            None => None,
            Some((_, obj)) => Some(obj),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_db() {
        let db = DB::new();
    }

    #[test]
    fn add_and_look_up() {
        let mut db = DB::new();
        for i in 0..100 {
            db.add(Robj::create_string_object_from_long(i),
                   Robj::create_string_object_from_long(i));
        }
        for i in 0..100 {
            let r: &RobjPtr =
                db.look_up(&Robj::create_string_object_from_long(i)).unwrap();
            let k = r.borrow().object_to_long().unwrap();
            assert_eq!(k, i);
        }
    }
}