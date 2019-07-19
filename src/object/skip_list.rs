use std::rc::Rc;
use crate::object::RobjPtr;
use std::alloc::handle_alloc_error;

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
            level_vec.push(SkipListLevel{
                forward: None,
                span: 0
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
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_new_skip_list() {
        let list = SkipList::new();
        assert_eq!(list.length, 0);
        assert_eq!(list.level, 1);
    }
}


