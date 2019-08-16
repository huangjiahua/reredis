pub mod sds;
pub mod list;
pub mod dict;
pub mod skip_list;
pub mod int_set;
pub mod zip_list;
pub mod zset;


use std::time::SystemTime;
use std::cell::RefCell;
use std::rc::Rc;
use std::error::Error;

pub use sds::Sds;
use list::List;
use zip_list::ZipList;
use dict::{Dict, DictPartialEq};
use int_set::IntSet;
use zset::Zset;

use crate::hash;
use rand::prelude::*;
use std::borrow::{BorrowMut, Borrow};
use crate::object::zip_list::ZipListValue;
use crate::object::list::ListWhere;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum RobjType {
    String,
    List,
    Set,
    Zset,
    Hash,
}

#[derive(Copy, Clone, PartialEq, Debug)]
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
    fn linked_list_ref(&self) -> &List {
        panic!("This is not a List");
    }
    fn linked_list_mut(&mut self) -> &mut List {
        panic!("This is not a List");
    }
    fn set_ref(&self) -> &Set { panic!("This is not a Set"); }
    fn zip_list_ref(&self) -> &ZipList { panic!("This is not a ZipList"); }
    fn zip_list_mut(&mut self) -> &mut ZipList {
        panic!("This is not a ZipList");
    }
    fn hash_table_ref(&self) -> &Dict<RobjPtr, RobjPtr> { panic!("This is not a hash table"); }
    fn int_set_ref(&self) -> &IntSet { panic!("This is not an IntSet"); }
    fn zset_ref(&self) -> &Zset { panic!("This is not a Zset"); }
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

    pub fn is_object_can_be_long(&self) -> bool {
        self.obj_type == RobjType::String &&
            self.string().parse::<i64>().is_ok()
    }

    pub fn try_object_encoding(&self) -> RobjPtr {
        unimplemented!()
    }

    pub fn get_decoded_object(&self) -> RobjPtr {
        unimplemented!()
    }

    pub fn string_object_len(&self) -> usize {
        self.string().len()
    }

    pub fn create_object(obj_type: RobjType, encoding: RobjEncoding, ptr: Pointer) -> RobjPtr {
        Rc::new(RefCell::new(
            Robj {
                obj_type,
                encoding,
                lru: SystemTime::now(),
                ptr,
            }
        ))
    }

    pub fn create_string_object(string: &str) -> RobjPtr {
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(string.to_string()),
        )
    }

    pub fn create_raw_string_object(string: &str) -> RobjPtr {
        let ret = Self::create_string_object(string);
        ret.as_ref().borrow_mut().encoding = RobjEncoding::Raw;
        ret
    }

    pub fn create_embedded_string_object(string: &str) -> RobjPtr {
        // TODO: add embedded string support
        let ret = Self::create_string_object(string);
        ret.as_ref().borrow_mut().encoding = RobjEncoding::EmbStr;
        ret
    }

    pub fn create_string_object_from_long(value: i64) -> Rc<RefCell<Robj>> {
        let ptr = Box::new(value.to_string());
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            ptr,
        )
    }

    pub fn create_string_object_from_double(value: f64) -> Rc<RefCell<Robj>> {
        let ptr = Box::new(value.to_string());
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            ptr,
        )
    }

    pub fn string_obj_eq(lhs: &RobjPtr, rhs: &RobjPtr) -> bool {
        lhs.as_ref().borrow().string() == rhs.as_ref().borrow().string()
    }

    pub fn create_list_object() -> RobjPtr {
        Self::create_object(
            RobjType::List,
            RobjEncoding::LinkedList,
            Box::new(List::new()),
        )
    }

    pub fn create_zip_list_object() -> RobjPtr {
        Self::create_object(
            RobjType::List,
            RobjEncoding::ZipList,
            Box::new(ZipList::new()),
        )
    }

    pub fn create_set_object() -> RobjPtr {
        let mut rng = rand::thread_rng();
        let mut num: u64 = rng.gen();
        let s: Set = Dict::new(hash::string_object_hash, num);
        Self::create_object(
            RobjType::Set,
            RobjEncoding::Ht,
            Box::new(s),
        )
    }

    pub fn create_int_set_object() -> RobjPtr {
        Self::create_object(
            RobjType::Set,
            RobjEncoding::IntSet,
            Box::new(IntSet::new()),
        )
    }

    pub fn create_hash_object() -> RobjPtr {
        let mut num: u64 = rand::thread_rng().gen();
        let ht: Dict<RobjPtr, RobjPtr> = Dict::new(hash::string_object_hash, num);
        Self::create_object(
            RobjType::Hash,
            RobjEncoding::Ht,
            Box::new(ht),
        )
    }

    pub fn create_zset_object() -> RobjPtr {
        Self::create_object(
            RobjType::Zset,
            RobjEncoding::SkipList,
            Box::new(Zset::new()),
        )
    }

    pub fn create_zset_zip_list_object() -> RobjPtr {
        Self::create_object(
            RobjType::Zset,
            RobjEncoding::ZipList,
            Box::new(ZipList::new()),
        )
    }

    pub fn is_string(&self) -> bool {
        match self.obj_type {
            RobjType::String => true,
            _ => false,
        }
    }

    pub fn is_list(&self) -> bool {
        match self.obj_type {
            RobjType::List => true,
            _ => false,
        }
    }

    pub fn encoding(&self) -> RobjEncoding {
        self.encoding
    }

    pub fn list_push_front(&mut self, o: RobjPtr) {
        if self.encoding == RobjEncoding::ZipList {
            let mut l = self.ptr.as_mut().zip_list_mut();
            let mut node = l.front_mut();
            node.insert(o.as_ref().borrow().string().as_bytes());
        } else if self.encoding == RobjEncoding::LinkedList {
            let mut l = self.ptr.as_mut().linked_list_mut();
            l.push_front(o);
        } else {
            unreachable!();
        }
    }

    pub fn list_push_back(&mut self, o: RobjPtr) {
        if self.encoding == RobjEncoding::ZipList {
            let mut l = self.ptr.as_mut().zip_list_mut();
            l.push(o.as_ref().borrow().string().as_bytes());
        } else if self.encoding == RobjEncoding::LinkedList {
            let mut l = self.ptr.as_mut().linked_list_mut();
            l.push_back(o);
        } else {
            unreachable!();
        }
    }

    pub fn list_pop(&mut self, w: ListWhere) -> Option<RobjPtr> {
        if self.encoding == RobjEncoding::ZipList {
            let mut l = self.ptr.as_mut().zip_list_mut();
            let node = match w {
                ListWhere::Head => l.front_mut(),
                ListWhere::Tail => l.tail_mut(),
            };
            if node.at_end() {
                return None;
            }
            let ret = match node.value() {
                ZipListValue::Int(i) => Robj::create_string_object_from_long(i),
                ZipListValue::Bytes(b) =>
                    Robj::create_string_object(std::str::from_utf8(b).unwrap()),
            };
            node.delete();
            Some(ret)
        } else if self.encoding == RobjEncoding::LinkedList {
            let mut l = self.ptr.as_mut().linked_list_mut();
            match w {
                ListWhere::Head => l.pop_front(),
                ListWhere::Tail => l.pop_back(),
            }
        } else {
            unreachable!()
        }
    }
}

impl DictPartialEq for RobjPtr {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().borrow().string() == other.as_ref().borrow().string()
    }
}


impl ObjectData for Sds {
    fn sds_ref(&self) -> &str {
        self
    }
}

impl ObjectData for List {
    fn linked_list_ref(&self) -> &List {
        self
    }
    fn linked_list_mut(&mut self) -> &mut List {
        self
    }
}

impl ObjectData for ZipList {
    fn zip_list_ref(&self) -> &ZipList {
        self
    }
    fn zip_list_mut(&mut self) -> &mut ZipList {
        self
    }
}

type Set = Dict<RobjPtr, ()>;

impl ObjectData for Set {
    fn set_ref(&self) -> &Set {
        self
    }
}

impl ObjectData for IntSet {
    fn int_set_ref(&self) -> &IntSet {
        self
    }
}

impl ObjectData for Dict<RobjPtr, RobjPtr> {
    fn hash_table_ref(&self) -> &Dict<RobjPtr, RobjPtr> {
        self
    }
}

impl ObjectData for Zset {
    fn zset_ref(&self) -> &Zset {
        self
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_new_object() {
        let o: RobjPtr = Robj::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(Sds::from("hi")),
        );
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
        let obj = objp.as_ref().borrow();
        if let Err(_) = obj.object_to_long() {
            panic!("fail converting");
        }

        let objp = Robj::create_string_object("kmp");
        let obj = objp.as_ref().borrow();
        if let Ok(_) = obj.object_to_long() {
            panic!("not number");
        }
    }

    #[test]
    fn get_string_object_len() {
        let objp = Robj::create_string_object("foobar");
        assert_eq!(objp.as_ref().borrow().string_object_len(), 6);
    }

    #[test]
    fn create_from_number() {
        let objp = Robj::create_string_object_from_long(56);
        assert_eq!(objp.as_ref().borrow().string(), "56");
        let objp = Robj::create_string_object_from_double(3.14);
        assert_eq!(objp.as_ref().borrow().string(), "3.14");
        let objp = Robj::create_string_object_from_double(0.0);
        assert_eq!(objp.as_ref().borrow().string(), "0");
    }
}