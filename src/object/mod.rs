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

use list::List;
use zip_list::ZipList;
use dict::{Dict, DictPartialEq};
use int_set::IntSet;
use zset::Zset;

use crate::hash;
use rand::prelude::*;
use crate::object::zip_list::ZipListValue;
use crate::object::list::ListWhere;
use crate::util::{bytes_vec, bytes_to_i64, bytes_to_f64};

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
    fn bytes_ref(&self) -> &[u8] { panic!("This is not a byte slice"); }
    fn sds_ref(&self) -> &str { panic!("This is not an Sds string"); }
    fn integer(&self) -> i64 { panic!("This is not an integer"); }
    fn linked_list_ref(&self) -> &List { panic!("This is not a List"); }
    fn linked_list_mut(&mut self) -> &mut List { panic!("This is not a List"); }
    fn set_ref(&self) -> &Set { panic!("This is not a Set"); }
    fn set_mut(&mut self) -> &mut Set { panic!("This is not a Set"); }
    fn zip_list_ref(&self) -> &ZipList { panic!("This is not a ZipList"); }
    fn zip_list_mut(&mut self) -> &mut ZipList { panic!("This is not a ZipList"); }
    fn hash_table_ref(&self) -> &Dict<RobjPtr, RobjPtr> { panic!("This is not a hash table"); }
    fn int_set_ref(&self) -> &IntSet { panic!("This is not an IntSet"); }
    fn int_set_mut(&mut self) -> &mut IntSet { panic!("This is not an IntSet"); }
    fn set_wrapper_ref(&self) -> &dyn SetWrapper { panic!("This is not as SetWrapper") }
    fn set_wrapper_mut(&mut self) -> &mut dyn SetWrapper { panic!("This is not as SetWrapper") }
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

pub trait SetWrapper {
    fn sw_len(&self) -> usize;
    fn sw_delete(&mut self, o: &RobjPtr) -> Result<(), ()>;
    fn sw_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a>;
    fn sw_exists(&self, o: &RobjPtr) -> bool;
    fn sw_pop_random(&mut self) -> RobjPtr;
}

pub struct SWInterIter<'a> {
    main: Box<dyn Iterator<Item=RobjPtr> + 'a>,
    others: &'a [RobjPtr],
}

impl Robj {
    pub fn string(&self) -> &[u8] {
        self.ptr.bytes_ref()
    }

    pub fn string_len(&self) -> usize {
        match self.encoding {
            RobjEncoding::Int => {
                self.integer().to_string().len()
            }
            _ => self.string().len()
        }
    }

    pub fn integer(&self) -> i64 {
        self.ptr.integer()
    }

    pub fn float(&self) -> f64 {
        self.parse_to_float().unwrap()
    }

    pub fn parse_to_float(&self) -> Result<f64, ()> {
        if self.obj_type != RobjType::String {
            panic!("This type cannot be converted to float")
        }
        match self.encoding {
            RobjEncoding::Int => Ok(self.integer() as f64),
            _ => {
                match bytes_to_f64(self.string()) {
                    Ok(n) => Ok(n),
                    Err(_) => Err(()),
                }
            },
        }
    }

    pub fn change_to_str(&mut self, s: &str) {
        let bytes = bytes_vec(s.as_bytes());
        self.ptr = Box::new(bytes);
    }

    pub fn dup_string_object(&self) -> RobjPtr {
        let string = self.string();
        Self::create_bytes_object(string)
    }

    pub fn object_to_long(&self) -> Result<i64, Box<dyn Error>> {
        let string = self.string();
        bytes_to_i64(string)
    }

    pub fn is_object_can_be_long(&self) -> bool {
        if self.obj_type != RobjType::String {
            return false;
        }
        match bytes_to_i64(self.string()) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub fn try_object_encoding(&self) -> RobjPtr {
        unimplemented!()
    }

    pub fn get_decoded_object(&self) -> RobjPtr {
        unimplemented!()
    }

    pub fn string_object_len(&self) -> usize {
        self.string_len()
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
        let bytes: Vec<u8> = bytes_vec(string.as_bytes());
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(bytes),
        )
    }

    pub fn create_bytes_object(bytes: &[u8]) -> RobjPtr {
        let bytes: Vec<u8> = bytes_vec(bytes);
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(bytes),
        )
    }

    pub fn from_bytes(bytes: Vec<u8>) -> RobjPtr {
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(bytes),
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
        let bytes: Vec<u8> = bytes_vec(&value.to_string().as_bytes());
        let ptr = Box::new(bytes);
        Self::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            ptr,
        )
    }

    pub fn create_string_object_from_double(value: f64) -> Rc<RefCell<Robj>> {
        let bytes: Vec<u8> = bytes_vec(&value.to_string().as_bytes());
        let ptr = Box::new(bytes);
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
        let num: u64 = rng.gen();
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
        let num: u64 = rand::thread_rng().gen();
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

    pub fn linear_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a> {
        match self.obj_type {
            RobjType::Set => self.set_iter(),
            RobjType::List => self.list_iter(),
            _ => unreachable!()
        }
    }

    pub fn list_push(&mut self, o: RobjPtr, w: ListWhere) {
        match self.encoding {
            RobjEncoding::ZipList => {
                if self.list_can_update(&o) {
                    self.list_update_push(o, w);
                    return;
                }
                let l = self.ptr.zip_list_mut();
                match w {
                    ListWhere::Tail => {
                        l.push(o.borrow().string());
                    }
                    ListWhere::Head => {
                        let node = l.front_mut();
                        node.insert(o.borrow().string());
                    }
                }
            }
            RobjEncoding::LinkedList => {
                let l = self.ptr.linked_list_mut();
                match w {
                    ListWhere::Tail => l.push_back(o),
                    ListWhere::Head => l.push_front(o),
                }
            }
            _ => unreachable!(),
        }
    }

    fn list_can_update(&self, o: &RobjPtr) -> bool {
        if (o.borrow().string_len() > (1 << 16)) ||
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
                    Robj::create_bytes_object(b),
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
            let l = self.ptr.zip_list_mut();
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
                    Robj::create_bytes_object(b),
            };
            node.delete();
            Some(ret)
        } else if self.encoding == RobjEncoding::LinkedList {
            let l = self.ptr.linked_list_mut();
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
                        Robj::create_bytes_object(b),
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
        let l = self.ptr.linked_list_mut();

        if l.len() <= idx {
            return Err(());
        }

        l.set_off(idx, o);

        Ok(())
    }

    fn zip_list_set(&mut self, idx: usize, o: RobjPtr) -> Result<(), ()> {
        let l = self.ptr.zip_list_mut();

        if l.len() <= idx {
            return Err(());
        }

        let mut node = l.front_mut();
        for _ in 0..idx {
            node = node.move_next();
        }

        node = node.delete();
        node.insert(o.borrow().string());
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
                            Robj::create_bytes_object(b)
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
        let real_end = l.len();
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

    pub fn list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) -> usize {
        match self.encoding {
            RobjEncoding::ZipList => self.zip_list_del_n(w, n, o),
            RobjEncoding::LinkedList => self.linked_list_del_n(w, n, o),
            _ => unreachable!()
        }
    }

    fn zip_list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) -> usize {
        let l = self.ptr.zip_list_mut();
        let len = l.len();
        if len == 0 {
            return 0;
        }

        let tmp = o.borrow();
        let s = tmp.string();

        let f = |val: &ZipListValue| -> bool {
            match val {
                ZipListValue::Int(i) => {
                    s == format!("{}", *i).as_bytes()
                }
                ZipListValue::Bytes(b) => {
                    s == *b
                }
            }
        };

        match w {
            ListWhere::Head => l.front_mut().delete_first_n_filter(n, f),
            ListWhere::Tail => l.tail_mut().delete_last_n_filter(n, f),
        };
        len - l.len()
    }

    fn linked_list_del_n(&mut self, w: ListWhere, n: usize, o: &RobjPtr) -> usize {
        let l = self.ptr.linked_list_mut();
        let len = l.len();
        if len == 0 {
            return 0;
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
        len - l.len()
    }

    pub fn is_set(&self) -> bool {
        match self.obj_type {
            RobjType::Set => true,
            _ => false,
        }
    }

    pub fn set_len(&self) -> usize {
        self.ptr.set_wrapper_ref().sw_len()
    }

    pub fn set_add(&mut self, o: RobjPtr) -> Result<(), ()> {
        match self.encoding {
            RobjEncoding::Ht => {
                let set = self.ptr.set_mut();
                let o = if o.borrow().encoding == RobjEncoding::Int {
                    o.borrow().gen_string()
                } else {
                    o
                };
                set.add(o, ())
            }
            RobjEncoding::IntSet => {
                let i: i64;
                if o.borrow().encoding != RobjEncoding::Int {
                    let r = o.borrow().object_to_long();
                    match r {
                        Ok(n) => i = n,
                        Err(_) => return self.set_update_add(o),
                    }
                } else {
                    i = o.borrow().integer();
                }
                let set = self.ptr.int_set_mut();
                set.add(i)
            }
            _ => unreachable!()
        }
    }

    fn set_update_add(&mut self, o: RobjPtr) -> Result<(), ()> {
        let num: u64 = rand::thread_rng().gen();
        let old: &IntSet = self.ptr.int_set_ref();
        let mut s: Set = Dict::new(hash::string_object_hash, num);
        for i in old.iter() {
            let _ = s.add(Robj::create_string_object_from_long(i), ());
        }
        let ret = s.add(o, ());

        self.ptr = Box::new(s);
        self.encoding = RobjEncoding::Ht;

        ret
    }

    pub fn set_delete(&mut self, o: &RobjPtr) -> Result<(), ()> {
        self.ptr.set_wrapper_mut().sw_delete(o)
    }

    pub fn set_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a> {
        self.ptr.set_wrapper_ref().sw_iter()
    }

    pub fn set_exists(&self, o: &RobjPtr) -> bool {
        self.ptr.set_wrapper_ref().sw_exists(o)
    }

    pub fn set_pop_random(&mut self) -> RobjPtr {
        self.ptr.set_wrapper_mut().sw_pop_random()
    }

    pub fn set_inter_iter<'a>(&'a self, others: &'a [RobjPtr]) -> SWInterIter<'a> {
        SWInterIter {
            main: self.set_iter(),
            others,
        }
    }
}

impl DictPartialEq for RobjPtr {
    fn eq(&self, other: &Self) -> bool {
        self.borrow().string() == other.borrow().string()
    }
}

impl ObjectData for Vec<u8> {
    fn bytes_ref(&self) -> &[u8] {
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
    fn set_mut(&mut self) -> &mut Set {
        self
    }
    fn set_wrapper_ref(&self) -> &dyn SetWrapper {
        self
    }
    fn set_wrapper_mut(&mut self) -> &mut dyn SetWrapper {
        self
    }
}

impl ObjectData for IntSet {
    fn int_set_ref(&self) -> &IntSet {
        self
    }
    fn int_set_mut(&mut self) -> &mut IntSet {
        self
    }
    fn set_wrapper_ref(&self) -> &dyn SetWrapper {
        self
    }
    fn set_wrapper_mut(&mut self) -> &mut dyn SetWrapper {
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

impl SetWrapper for Set {
    fn sw_len(&self) -> usize {
        self.len()
    }

    fn sw_delete(&mut self, o: &Rc<RefCell<Robj>>) -> Result<(), ()> {
        self.delete(o).map(|_| ())
    }

    fn sw_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a> {
        Box::new(self.iter()
            .map(|x| Rc::clone(x.0)))
    }

    fn sw_exists(&self, o: &RobjPtr) -> bool {
        match self.find(o) {
            Some(_) => true,
            None => false,
        }
    }

    fn sw_pop_random(&mut self) -> RobjPtr {
        let (o, _) = self.random_key_value();
        let o = Rc::clone(o);
        let _ = self.delete(&o);
        Rc::clone(&o)
    }
}

impl SetWrapper for IntSet {
    fn sw_len(&self) -> usize {
        self.len()
    }

    fn sw_delete(&mut self, o: &Rc<RefCell<Robj>>) -> Result<(), ()> {
        if let Ok(i) = o.borrow().object_to_long() {
            self.remove(i)
        } else {
            Err(())
        }
    }

    fn sw_iter<'a>(&'a self) -> Box<dyn Iterator<Item=RobjPtr> + 'a> {
        Box::new(self.iter()
            .map(|x| Robj::create_string_object_from_long(x)))
    }

    fn sw_exists(&self, o: &RobjPtr) -> bool {
        if let Ok(i) = o.borrow().object_to_long() {
            self.find(i)
        } else {
            false
        }
    }

    fn sw_pop_random(&mut self) -> RobjPtr {
        let which: usize = rand::thread_rng().gen_range(0, self.len());
        let i = self.get(which);
        let _ = self.remove(i);
        Robj::create_string_object_from_long(i)
    }
}

impl<'a> Iterator for SWInterIter<'a> {
    type Item = RobjPtr;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(o) = self.main.next() {
            let mut i: usize = 0;
            for other in self.others.iter() {
                if !other.borrow().set_exists(&o) {
                    break;
                }
                i += 1;
            }
            if i == self.others.len() {
                return Some(o);
            }
        }
        None
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_new_object() {
        let _o: RobjPtr = Robj::create_object(
            RobjType::String,
            RobjEncoding::Raw,
            Box::new(bytes_vec(b"hello")),
        );
    }

    #[test]
    fn create_new_string_object() {
        let _o: RobjPtr = Robj::create_string_object("foo");
        let _o2: RobjPtr = Robj::create_raw_string_object("bar");
        let _o3: RobjPtr = Robj::create_embedded_string_object("hey");
        let _o4: RobjPtr = Robj::create_bytes_object(b"foo");
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
        assert_eq!(objp.borrow().string(), b"56");
        let objp = Robj::create_string_object_from_double(3.14);
        assert_eq!(objp.borrow().string(), b"3.14");
        let objp = Robj::create_string_object_from_double(0.0);
        assert_eq!(objp.borrow().string(), b"0");
    }
}