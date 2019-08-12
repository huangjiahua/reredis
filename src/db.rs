use crate::object::{Robj, RobjPtr};
use crate::object::dict::Dict;
use crate::hash::string_object_hash;
use rand::Rng;
use std::time::SystemTime;
use mio::Ready;

struct DB {
    pub id: usize,
    pub dict: Dict<RobjPtr, RobjPtr>,
    pub expires: Dict<RobjPtr, SystemTime>,
}

impl DB {
    pub fn new(id: usize) -> DB {
        let mut rng = rand::thread_rng();
        DB {
            id,
            dict: Dict::new(string_object_hash, rng.gen()),
            expires: Dict::new(string_object_hash, rng.gen()),
        }
    }

    pub fn remove_expire(&mut self, key: &RobjPtr) -> Result<(), ()> {
        let _ = self.expires.delete(key)?;
        Ok(())
    }

    pub fn set_expire(&mut self, key: RobjPtr, when: SystemTime) -> Result<(), ()> {
        self.expires.add(key, when)
    }

    pub fn get_expire(&mut self, key: &RobjPtr) -> Option<&SystemTime> {
        self.expires.find_by_mut(key).map(|p| p.1)
    }

    pub fn expire_if_needed(&mut self, key: &RobjPtr) -> Result<bool, ()> {
        if self.expires.len() == 0 {
            return Err(());
        }

        let r = self.expires.find(key);
        if r.is_none() {
            return Err(());
        }

        let when = r.unwrap().1;

        if SystemTime::now() < *when {
            return Ok(false);
        }

        self.expires.delete(key).unwrap();

        let r = self.dict.delete(key)?;

        Ok(true)
    }

    pub fn delete(&mut self, key: &RobjPtr) -> Result<(), ()> {
        if self.expires.len() == 0 {
            return Err(())
        }

        let _ = self.expires.delete(key)?;
        let _ = self.dict.delete(key)?;
        Ok(())
    }

//    pub fn add(&mut self, key: RobjPtr, value: RobjPtr) {
//        self.dict.add(key, value).unwrap();
//    }
//
//    pub fn look_up(&mut self, key: &RobjPtr) -> Option<&RobjPtr> {
//        let r = self.dict.find(key);
//        match r {
//            None => None,
//            Some((_, obj)) => Some(obj),
//        }
//    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_db() {
        let db = DB::new(0);
    }

//    #[test]
//    fn add_and_look_up() {
//        let mut db = DB::new(0);
//        for i in 0..100 {
//            db.add(Robj::create_string_object_from_long(i),
//                   Robj::create_string_object_from_long(i));
//        }
//        for i in 0..100 {
//            let r: &RobjPtr =
//                db.look_up(&Robj::create_string_object_from_long(i)).unwrap();
//            let k = r.borrow().object_to_long().unwrap();
//            assert_eq!(k, i);
//        }
//    }
}