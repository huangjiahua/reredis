pub mod sds;
pub mod list;
pub mod dict;
pub mod skip_list;
pub mod int_set;
pub mod zip_list;

pub use sds::Sds;

use std::time::SystemTime;
use std::cell::RefCell;
use std::rc::Rc;
use std::error::Error;
use crate::object::list::List;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum RobjType {
    String,
    List,
    Set,
    Zset,
    Hash,
}

#[derive(Copy, Clone)]
pub enum RobjEncoding {
    Raw,
    Int,
    Ht,
    ZipMap,
    LinkedList,
    ZipList,
    IntSet,
    SkipList,
    EmbStr,
}

pub trait ObjectData {
    fn sds_ref(&self) -> &str {
        panic!("This is not an Sds string");
    }
    fn list_ref(&self) -> &List {
        panic!("This is not a List");
    }
}

type Pointer = Box<dyn ObjectData>;
pub type RobjPtr = Rc<RefCell<Robj>>;

pub struct Robj {
    obj_type: RobjType,
    encoding: RobjEncoding,
    lru: SystemTime,
    ptr: Pointer,
}

impl Robj {
    pub fn string(&self) -> &str {
        self.ptr.sds_ref()
    }

    pub fn dup_string_object(&self) -> RobjPtr {
        let string = self.string();
        Self::create_string_object(string)
    }

    pub fn object_to_long(&self) -> Result<i64, Box<dyn Error>> {
        let string = self.string();
        let i: i64 = string.parse()?;
        Ok(i)
    }

    pub fn try_object_encoding(&self) -> RobjPtr {
        unimplemented!()
    }

    pub fn get_decoded_object(&self) -> Rc<RefCell<Robj>> {
        unimplemented!()
    }

    pub fn string_object_len(&self) -> usize {
        self.string().len()
    }

    pub fn create_object(obj_type: RobjType, ptr: Pointer) -> RobjPtr {
        Rc::new(RefCell::new(
            Robj {
                obj_type,
                encoding: RobjEncoding::Raw,
                lru: SystemTime::now(),
                ptr,
            }
        ))
    }

    pub fn create_string_object(string: &str) -> RobjPtr {
        Self::create_object(RobjType::String, Box::new(string.to_string()))
    }

    pub fn create_raw_string_object(string: &str) -> RobjPtr {
        let ret = Self::create_string_object(string);
        ret.borrow_mut().encoding = RobjEncoding::Raw;
        ret
    }

    pub fn create_embedded_string_object(string: &str) -> RobjPtr {
        let ret = Self::create_string_object(string);
        ret.borrow_mut().encoding = RobjEncoding::EmbStr;
        ret
    }

    pub fn create_string_object_from_long(value: i64) -> Rc<RefCell<Robj>> {
        let ptr = Box::new(value.to_string());
        Self::create_object(RobjType::String, ptr)
    }

    pub fn create_string_object_from_double(value: f64) -> Rc<RefCell<Robj>> {
        let ptr = Box::new(value.to_string());
        Self::create_object(RobjType::String, ptr)
    }

    pub fn string_obj_eq(lhs: &RobjPtr, rhs: &RobjPtr) -> bool {
        lhs.borrow().string() == rhs.borrow().string()
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_new_object() {
        let o: RobjPtr = Robj::create_object(RobjType::String, Box::new(Sds::from("hi")));
    }

    #[test]
    fn create_new_string_object() {
        let o: RobjPtr = Robj::create_string_object("foo");
        let o2: RobjPtr = Robj::create_raw_string_object("bar");
        let o3: RobjPtr = Robj::create_embedded_string_object("hey");
    }

    #[test]
    fn object_to_long() {
        let objp = Robj::create_string_object("135");
        let obj = objp.borrow();
        if let Err(_) = obj.object_to_long() {
            panic!("fail converting");
        }

        let objp = Robj::create_string_object("kmp");
        let obj = objp.borrow();
        if let Ok(_) = obj.object_to_long() {
            panic!("not number");
        }
    }

    #[test]
    fn get_string_object_len() {
        let objp = Robj::create_string_object("foobar");
        assert_eq!(objp.borrow().string_object_len(), 6);
    }

    #[test]
    fn create_from_number() {
        let objp = Robj::create_string_object_from_long(56);
        assert_eq!(objp.borrow().string(), "56");
        let objp = Robj::create_string_object_from_double(3.14);
        assert_eq!(objp.borrow().string(), "3.14");
        let objp = Robj::create_string_object_from_double(0.0);
        assert_eq!(objp.borrow().string(), "0");
    }
}