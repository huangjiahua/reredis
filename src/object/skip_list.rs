use std::rc::Rc;
use crate::object::RobjPtr;
use std::alloc::handle_alloc_error;
use rand::prelude::*;
use std::iter::Skip;

const SKIP_LIST_MAX_LEVEL: usize = 32;

pub struct SkipListLevel {
    forward: Option<Rc<SkipListNode>>,
    span: usize,
}

pub struct SkipListNode {
    obj: Option<RobjPtr>,
    score: f64,
    backward: Option<Rc<SkipListNode>>,
    level: Vec<SkipListLevel>,
}

impl SkipListNode {
    fn new(level: usize, score: f64, obj: Option<RobjPtr>) -> SkipListNode {
        let mut level_vec: Vec<SkipListLevel>
            = Vec::with_capacity(level);

        for _ in 0..level {
            level_vec.push(SkipListLevel {
                forward: None,
                span: 0,
            });
        }

        let mut node = SkipListNode {
            obj: None,
            score,
            backward: None,
            level: level_vec,
        };

        if let Some(p) = obj {
            node.obj = Some(p);
        }

        node
    }
}

pub struct SkipList {
    header: Rc<SkipListNode>,
    tail: Option<Rc<SkipListNode>>,
    length: usize,
    level: usize,
}

impl SkipList {
    fn new() -> SkipList {
        let mut header =
            SkipListNode::new(SKIP_LIST_MAX_LEVEL, 0.0, None);

        header.backward = None;

        SkipList {
            header: Rc::new(header),
            tail: None,
            length: 0,
            level: 1,
        }
    }

    fn random_level() -> usize {
        let mut level = 1usize;
        let mut rng = rand::thread_rng();
        let mut num: usize = rng.gen();

        while ((num & 0xFFFFusize) as f64) < (0.25 * (0xFFFF as f64)) {
            level += 1;
            num = rng.gen();
        }

        if level < SKIP_LIST_MAX_LEVEL {
            return level;
        }
        SKIP_LIST_MAX_LEVEL
    }

    fn insert(&mut self, score: f64, obj: RobjPtr) {
        let mut update: Vec<Option<&mut Rc<SkipListNode>>> =
            Vec::with_capacity(SKIP_LIST_MAX_LEVEL);

        let mut rank = [0usize; SKIP_LIST_MAX_LEVEL];

        let mut x = Some(&self.header);
        for i in (0..self.level).rev() {
            rank[i] = if i == self.level - 1 {
                0
            } else {
                rank[i + 1]
            };
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::object::{Robj, RobjPtr};

    #[test]
    fn create_new_skip_list() {
        let list = SkipList::new();
        assert_eq!(list.length, 0);
        assert_eq!(list.level, 1);
    }

    #[test]
    fn generate_rand_level() {
        let mut levels = vec![0usize; 33];
        for i in 0..100000 {
            let l = SkipList::random_level();
            levels[l] += 1;
        }

        let q = levels.iter().skip(2);
        for p in levels.iter().skip(1).zip(q) {
            assert!(p.0 >= p.1);
        }
    }

    #[test]
    fn simple_insert() {
        let mut list = SkipList::new();
        let o1 = Robj::create_string_object("foo");
        let o2 = Robj::create_string_object("bar");

        list.insert(3.2, o1);
        list.insert(0.2, o2);
    }
}


