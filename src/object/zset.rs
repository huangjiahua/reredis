use crate::object::{Robj, RobjPtr};
use crate::object::skip_list::SkipList;
use crate::object::dict::Dict;
use crate::hash;
use std::iter::Skip;
use rand::prelude::*;

pub struct Zset {
    dict: Dict<RobjPtr, RobjPtr>,
    list: SkipList,
}

impl Zset {
    pub fn new() -> Zset {
        Zset {
            dict: Dict::new(hash::string_object_hash, rand::thread_rng().gen()),
            list: SkipList::new(),
        }
    }
}