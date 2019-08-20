pub mod sds;
pub mod list;
pub mod dict;
pub mod skip_list;
pub mod int_set;
pub mod zip_list;
pub mod zset;
pub mod linked_list;


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
use crate::object::zip_list::ZipListValue;
use crate::object::list::ListWhere;
use std::hint::unreachable_unchecked;

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
    fn integer(&self) -> i64 {
        panic!("This is not an integer");
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

    pub fn integer(&self) -> i64 {
        self.ptr.integer()
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

    pub fn create_int_object(i: i64) -> RobjPtr {
        Self::create_object(
            RobjType::String,
            RobjEncoding::Int,
            Box::new(i),
        )
    }

    pub fn gen_string(&self) -> RobjPtr {
        Self::create_string_object_from_long(
            self.ptr.integer(),
        )
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
        ret.borrow_mut().encoding = RobjEncoding::Raw;
        ret
    }

    pub fn create_embedded_string_object(string: &str) -> RobjPtr {
        // TODO: add embedded string support
        let ret = Self::create_string_object(string);
        ret.borrow_mut().encoding = RobjEncoding::EmbStr;
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
        lhs.borrow().string() == rhs.borrow().string()
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

    pub fn object_type(&self) -> RobjType {
        self.obj_type
    }

    pub fn list_push(&mut self, o: RobjPtr, w: ListWhere) {
        match self.encoding {
            RobjEncoding::ZipList => {
                if self.list_can_update(&o) {
                    self.list_update_push(o, w);
                    return;
                }
                let mut l = self.ptr.zip_list_mut();
                match w {
                    ListWhere::Tail => {
                        l.push(o.borrow().string().as_bytes());
                    }
                    ListWhere::Head => {
                        let mut node = l.front_mut();
                        node.insert(o.borrow().string().as_bytes());
                    }
                }
            }
            RobjEncoding::LinkedList => {
                let mut l = self.ptr.linked_list_mut();
                match w {
                    ListWhere::Tail => l.push_back(o),
                    ListWhere::Head => l.push_front(o),
                }
            }
            _ => unreachable!(),
        }
    }

    fn list_can_update(&self, o: &RobjPtr) -> bool {
        if (o.borrow().string().len() > (1 << 16)) ||
            (self.list_len() == 7) {
            return true;
        }
        false
    }

    fn list_update_push(&mut self, o: RobjPtr, w: ListWhere) {
        assert_eq!(self.encoding, RobjEncoding::ZipList);
        let old_list = self.ptr.zip_list_ref();
        let mut new_list = Box::new(List::new());
        for v in old_list.iter_rev() {
            let obj = match v {
                ZipListValue::Int(n) => Robj::create_string_object_from_long(n),
                ZipListValue::Bytes(b) =>
                    Robj::create_string_object(std::str::from_utf8(b).unwrap()),
            };
            new_list.push_front(obj);
        }
        match w {
            ListWhere::Head => new_list.push_front(o),
            ListWhere::Tail => new_list.push_back(o),
        }
        self.ptr = new_list;
        self.encoding = RobjEncoding::LinkedList;
    }

    pub fn list_pop(&mut self, w: ListWhere) -> Option<RobjPtr> {
        if self.encoding == RobjEncoding::ZipList {
            let mut l = self.ptr.zip_list_mut();
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
            let mut l = self.ptr.linked_list_mut();
            match w {
                ListWhere::Head => l.pop_front(),
                ListWhere::Tail => l.pop_back(),
            }
        } else {
            unreachable!()
        }
    }

    pub fn list_len(&self) -> usize {
        match self.encoding {
            RobjEncoding::ZipList => self.ptr.zip_list_ref().len(),
            RobjEncoding::LinkedList => self.ptr.linked_list_ref().len(),
            _ => unreachable!(),
        }
    }

    pub fn list_index(&self, idx: usize) -> Option<RobjPtr> {
        match self.encoding {
            RobjEncoding::LinkedList => {
                let l = self.ptr.linked_list_ref();
                if l.len() <= idx {
                    return None;
                }
                let r = l.iter().skip(idx).next().unwrap();
                Some(Rc::clone(&r))
            }
            RobjEncoding::ZipList => {
                let l = self.ptr.zip_list_ref();
                if l.len() <= idx {
                    return None;
                }
                let r = match l.iter().skip(idx).next().unwrap() {
                    ZipListValue::Int(i) =>
                        Robj::create_string_object_from_long(i),
                    ZipListValue::Bytes(b) =>
                        Robj::create_string_object(std::str::from_utf8(b).unwrap()),
                };
                Some(r)
            }
            _ => unreachable!()
        }
    }

    pub fn list_set(&mut self, idx: usize, o: RobjPtr) -> Result<(), ()> {
        match self.encoding {
            RobjEncoding::LinkedList => self.linked_list_set(idx, o),
            RobjEncoding::ZipList => self.zip_list_set(idx, o),
            _ => unreachable!()
        }
    }

    fn linked_list_set(&mut self, idx: usize, o: RobjPtr) -> Result<(), ()> {
        let mut l = self.ptr.linked_list_mut();

        if l.len() <= idx {
            return Err(());
        }

        l.set_off(idx, o);

        Ok(())
    }

    fn zip_list_set(&mut self, idx: usize, o: RobjPtr) -> Result<(), ()> {
        let mut l = self.ptr.zip_list_mut();

        if l.len() <= idx {
            return Err(());
        }

        let mut node = l.front_mut();
        for i in 0..idx {
            node = node.move_next();
        }

        node = node.delete();
        node.insert(o.borrow().string().as_bytes());
        Ok(())
    }

    pub fn list_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a> {
        match self.encoding {
            RobjEncoding::ZipList => {
                let l = self.ptr.zip_list_ref();
                Box::new(l.iter()
                    .map(|x| match x {
                        ZipListValue::Int(i) =>
                            Robj::create_string_object_from_long(i),
                        ZipListValue::Bytes(b) =>
                            Robj::create_string_object(std::str::from_utf8(b).unwrap())
                    }))
            }
            RobjEncoding::LinkedList => {
                let l = self.ptr.linked_list_ref();
                Box::new(l.iter()
                    .map(|x| Rc::clone(x)))
            }
            _ => unreachable!()
        }
    }

    pub fn list_trim(&mut self, start: usize, end: usize) {
        match self.encoding {
            RobjEncoding::ZipList => self.zip_list_trim(start, end),
            RobjEncoding::LinkedList => self.linked_list_trim(start, end),
            _ => unreachable!()
        }
    }

    pub fn zip_list_trim(&mut self, start: usize, end: usize) {
        if start > end {
            self.ptr = Box::new(ZipList::new());
            return;
        }
        let l = self.ptr.zip_list_mut();
        let mut real_end = l.len();
        for _ in end + 1..real_end {
            l.tail_mut().delete();
        }
        l.front_mut().delete_range(start);
        assert_eq!(l.len(), end - start + 1);
    }

    pub fn linked_list_trim(&mut self, start: usize, end: usize) {
        if start > end {
            self.ptr.linked_list_mut().clear();
            return;
        }
        let l = self.ptr.linked_list_mut();
        l.split_off(end + 1);
        let tmp = l.split_off(start);
        self.ptr = Box::new(tmp);
    }

    pub fn list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) {
        match self.encoding {
            RobjEncoding::ZipList => self.zip_list_del_n(w, n, o),
            RobjEncoding::LinkedList => self.linked_list_del_n(w, n, o),
            _ => unreachable!()
        }
    }

    fn zip_list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) {
        let l = self.ptr.zip_list_mut();
        if l.len() == 0 {
            return;
        }

        let tmp = o.borrow();
        let s = tmp.string();

        let f = |val: &ZipListValue| -> bool {
            match val {
                ZipListValue::Int(i) => {
                    s == format!("{}", *i)
                }
                ZipListValue::Bytes(b) => {
                    s == std::str::from_utf8(*b).unwrap()
                }
            }
        };

        match w {
            ListWhere::Head => l.front_mut().delete_first_n_filter(n, f),
            ListWhere::Tail => l.tail_mut().delete_last_n_filter(n, f),
        };
    }

    fn linked_list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) {
        let l = self.ptr.linked_list_mut();
        if l.len() == 0 {
            return;
        }

        let tmp = o.borrow();
        let s = tmp.string();

        let f = |obj: &RobjPtr| -> bool {
            return obj.borrow().string() == s;
        };

        match w {
            ListWhere::Head => l.delete_first_n_filter(n, f),
            ListWhere::Tail => l.delete_last_n_filter(n, f),
        }
    }
}

impl DictPartialEq for RobjPtr {
    fn eq(&self, other: &Self) -> bool {
        self.borrow().string() == other.borrow().string()
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

impl ObjectData for i64 {
    fn integer(&self) -> i64 {
        *self
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