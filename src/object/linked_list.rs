use std::mem;
use std::marker::PhantomData;
use std::iter::FromIterator;

pub struct LinkedList<T> {
    head: Option<*mut Node<T>>,
    tail: Option<*mut Node<T>>,
    len: usize,
    marker: PhantomData<Box<Node<T>>>,
}

struct Node<T> {
    next: Option<*mut Node<T>>,
    prev: Option<*mut Node<T>>,
    element: T,
}

pub struct Iter<'a, T: 'a> {
    head: Option<*mut Node<T>>,
    tail: Option<*mut Node<T>>,
    len: usize,
    marker: PhantomData<&'a Node<T>>,
}

impl<T> Node<T> {
    fn new(element: T) -> Self {
        Node {
            next: None,
            prev: None,
            element,
        }
    }

    fn into_element(self: Box<Self>) -> T {
        self.element
    }
}

impl<T> LinkedList<T> {
    fn push_front_node(&mut self, mut node: Box<Node<T>>) {
        unsafe {
            node.next = self.head;
            node.prev = None;
            let node = Some(Box::into_raw(node));

            match self.head {
                None => self.tail = node,
                Some(head) => (*head).prev = node,
            }

            self.head = node;
        }
        self.len += 1;
    }

    fn pop_front_node(&mut self) -> Option<Box<Node<T>>> {
        self.head.map(|node| unsafe {
            let node = Box::from_raw(node);
            self.head = node.next;

            match self.head {
                None => self.tail = None,
                Some(head) => (*head).prev = None,
            }

            self.len -= 1;
            node
        })
    }

    fn push_back_node(&mut self, mut node: Box<Node<T>>) {
        unsafe {
            node.next = None;
            node.prev = self.tail;
            let node = Some(Box::into_raw(node));

            match self.tail {
                None => self.head = node,
                Some(tail) => (*tail).next = node,
            }

            self.tail = node;
        }
        self.len += 1;
    }

    fn pop_back_node(&mut self) -> Option<Box<Node<T>>> {
        self.tail.map(|node| unsafe {
            let node = Box::from_raw(node);
            self.tail = node.prev;

            match self.tail {
                None => self.head = None,
                Some(tail) => (*tail).next = None,
            }

            self.len -= 1;
            node
        })
    }

    unsafe fn unlink_node(&mut self, node: *mut Node<T>) -> Option<*mut Node<T>> {
        match (*node).prev {
            Some(prev) => (*prev).next = (*node).next.clone(),
            None => self.head = (*node).next.clone(),
        }

        let ret = match (*node).next {
            Some(next) => {
                (*next).prev = (*node).prev.clone();
                Some(next)
            }
            None => {
                self.tail = (*node).prev.clone();
                None
            }
        };

        self.len -= 1;
        ret
    }

    fn index_off(&mut self, idx: usize) -> &mut Node<T> {
        let target = if idx < self.len() - idx {
            let mut node = self.head.unwrap();
            for _ in 0..idx {
                unsafe {
                    node = (*node).next.unwrap();
                }
            }
            node
        } else {
            let mut node = self.tail.unwrap();
            unsafe {
                for _ in 0..self.len() - 1 - idx {
                    node = (*node).prev.unwrap();
                }
            }
            node
        };
        unsafe {
            &mut (*target)
        }
    }
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        LinkedList {
            head: None,
            tail: None,
            len: 0,
            marker: PhantomData,
        }
    }

    pub fn append(&mut self, other: &mut Self) {
        match self.tail {
            None => mem::swap(self, other),
            Some(mut tail) => {
                if let Some(other_head) = other.head.take() {
                    unsafe {
                        (*tail).next = Some(other_head);
                        (*other_head).prev = Some(tail);
                    }

                    self.tail = other.tail.take();
                    self.len += mem::replace(&mut other.len, 0);
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            head: self.head,
            tail: self.tail,
            len: self.len,
            marker: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        *self = Self::new()
    }

    pub fn front(&self) -> Option<&T> {
        unsafe {
            self.head.as_ref().map(|node| &(**node).element)
        }
    }

    pub fn back(&self) -> Option<&T> {
        unsafe {
            self.tail.as_ref().map(|node| &(**node).element)
        }
    }

    pub fn push_front(&mut self, elt: T) {
        self.push_front_node(Box::new(Node::new(elt)));
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.pop_front_node().map(Node::into_element)
    }

    pub fn push_back(&mut self, elt: T) {
        self.push_back_node(Box::new(Node::new(elt)));
    }

    pub fn pop_back(&mut self) -> Option<T> {
        self.pop_back_node().map(Node::into_element)
    }

    pub fn split_off(&mut self, at: usize) -> LinkedList<T> {
        let len = self.len();
        assert!(at <= len, "Cannot split off at a nonexistent index");
        if at == 0 {
            return mem::replace(self, Self::new());
        } else if at == len {
            return Self::new();
        }

        let split_node = if at - 1 < len - 1 - (at - 1) {
            let mut node = self.head.unwrap();
            unsafe {
                for _ in 0..at - 1 {
                    node = (*node).next.unwrap()
                }
            }
            node
        } else {
            let mut node = self.tail.unwrap();
            unsafe {
                for _ in 0..len - 1 - (at - 1) {
                    node = (*node).prev.unwrap();
                }
            }
            node
        };

        let second_part_head;

        unsafe {
            second_part_head = (*split_node).next.take();
            if let Some(mut head) = second_part_head {
                (*head).prev = None;
            }
        }

        let second_part = LinkedList {
            head: second_part_head,
            tail: self.tail,
            len: len - at,
            marker: PhantomData,
        };

        self.tail = Some(split_node);
        self.len = at;
        second_part
    }

    pub fn set_off(&mut self, at: usize, elt: T) {
        assert!(at <= self.len(), "Cannot split off at a nonexistent index");
        let mut node = self.index_off(at);
        node.element = elt;
    }

    pub fn delete_first_n_filter<F>(&mut self, mut n: usize, f: F)
        where F: Fn(&T) -> bool {
        if self.len() == 0 {
            return;
        }
        let mut node = self.head;

        while let Some(ptr) = node.take() {
            unsafe {
                if f(&mut (*ptr).element) {
                    n -= 1;
                    node = self.unlink_node(ptr);
                    // drain this node
                    let _ = Box::from_raw(ptr);
                } else {
                    node = (*ptr).next;
                }
            }
            if n == 0 {
                return;
            }
        }
    }

    pub fn delete_last_n_filter<F>(&mut self, mut n: usize, f: F)
        where F: Fn(&T) -> bool {
        if self.len() == 0 {
            return;
        }
        let mut node = self.tail;

        while let Some(ptr) = node.take() {
            unsafe {
                if f(&mut (*ptr).element) {
                    n -= 1;
                    self.unlink_node(ptr);
                    node = (*ptr).prev;
                    // drain this node
                    let _ = Box::from_raw(ptr);
                } else {
                    node = (*ptr).prev
                }
            }
            if n == 0 {
                return;
            }
        }
    }
}

impl<T> Extend<T> for LinkedList<T> {
    fn extend<I: IntoIterator<Item=T>>(&mut self, iter: I) {
        for i in iter {
            self.push_back(i)
        }
    }
}

impl<T> FromIterator<T> for LinkedList<T> {
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        let mut list = Self::new();
        for i in iter {
            list.push_back(i);
        }
        list
    }
}

impl<T> PartialEq for LinkedList<T> {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl<T: Clone> Clone for LinkedList<T> {
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        while let Some(_) = self.pop_front_node() {}
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        if self.len == 0 {
            None
        } else {
            self.head.map(|node| unsafe {
                // Need an unbound lifetime to get 'a
                self.len -= 1;
                self.head = (*node).next;
                &(*node).element
            })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[cfg(test)]
    fn list_from<T: Clone>(v: &[T]) -> LinkedList<T> {
        v.iter().cloned().collect()
    }

    pub fn check_links<T>(list: &LinkedList<T>) {
        unsafe {
            let mut len = 0;
            let mut last_ptr: Option<&Node<T>> = None;
            let mut node_ptr: &Node<T>;
            match list.head {
                None => {
                    // tail node should also be None.
                    assert!(list.tail.is_none());
                    assert_eq!(0, list.len);
                    return;
                }
                Some(node) => node_ptr = &(*node),
            }
            loop {
                match (last_ptr, node_ptr.prev) {
                    (None, None) => {}
                    (None, _) => panic!("prev link for head"),
                    (Some(p), Some(pptr)) => {
                        assert_eq!(p as *const Node<T>, pptr as *const Node<T>);
                    }
                    _ => panic!("prev link is none, not good"),
                }
                match node_ptr.next {
                    Some(next) => {
                        last_ptr = Some(node_ptr);
                        node_ptr = &(*next);
                        len += 1;
                    }
                    None => {
                        len += 1;
                        break;
                    }
                }
            }

            // verify that the tail node points to the last node.
            let tail = list.tail.as_ref().expect("some tail node");
            assert_eq!(*tail as *const Node<T>, node_ptr as *const Node<T>);
            // check that len matches interior links.
            assert_eq!(len, list.len);
        }
    }

    #[test]
    fn test_append() {
        // Empty to empty
        {
            let mut m = LinkedList::<i32>::new();
            let mut n = LinkedList::new();
            m.append(&mut n);
            check_links(&m);
            assert_eq!(m.len(), 0);
            assert_eq!(n.len(), 0);
        }
        // Non-empty to empty
        {
            let mut m = LinkedList::new();
            let mut n = LinkedList::new();
            n.push_back(2);
            check_links(&n);
            m.append(&mut n);
            check_links(&m);
            assert_eq!(m.len(), 1);
            assert_eq!(m.pop_back(), Some(2));
            assert_eq!(n.len(), 0);
            check_links(&m);
        }
        // Empty to non-empty
        {
            let mut m = LinkedList::new();
            let mut n = LinkedList::new();
            m.push_back(2);
            m.append(&mut n);
            check_links(&m);
            assert_eq!(m.len(), 1);
            assert_eq!(m.pop_back(), Some(2));
            check_links(&m);
        }

        // Non-empty to non-empty
        let v = vec![1, 2, 3, 4, 5];
        let u = vec![9, 8, 1, 2, 3, 4, 5];
        let mut m = list_from(&v);
        let mut n = list_from(&u);
        m.append(&mut n);
        check_links(&m);
        let mut sum = v;
        sum.extend_from_slice(&u);
        assert_eq!(sum.len(), m.len());
        for elt in sum {
            assert_eq!(m.pop_front(), Some(elt))
        }
        assert_eq!(n.len(), 0);
        // let's make sure it's working properly, since we
        // did some direct changes to private members
        n.push_back(3);
        assert_eq!(n.len(), 1);
        assert_eq!(n.pop_front(), Some(3));
        check_links(&n);
    }

    #[test]
    fn test_split_off() {
        let mut v1 = LinkedList::new();
        v1.push_front(1);
        v1.push_front(1);
        v1.push_front(1);
        v1.push_front(1);

        // test all splits
        for ix in 0..1 + v1.len() {
            let mut a = v1.clone();
            let b = a.split_off(ix);
            check_links(&a);
            check_links(&b);
            a.extend(b.iter().map(|x| *x));
            for p in a.iter().zip(v1.iter()) {
                assert_eq!(*p.0, *p.1);
            }
        }
    }

    #[cfg(test)]
    fn fuzz_test(sz: i32) {
        let mut m: LinkedList<_> = LinkedList::new();
        let mut v = vec![];
        for i in 0..sz {
            check_links(&m);
            let r: u8 = thread_rng().next_u32() as u8;
            match r % 6 {
                0 => {
                    m.pop_back();
                    v.pop();
                }
                1 => {
                    if !v.is_empty() {
                        m.pop_front();
                        v.remove(0);
                    }
                }
                2 | 4 => {
                    m.push_front(-i);
                    v.insert(0, -i);
                }
                3 | 5 | _ => {
                    m.push_back(i);
                    v.push(i);
                }
            }
        }

        check_links(&m);

        let mut i = 0;
        for (a, &b) in m.iter().zip(&v) {
            i += 1;
            assert_eq!(*a, b);
        }
        assert_eq!(i, v.len());
    }

    #[test]
    fn test_fuzz() {
        for _ in 0..25 {
            fuzz_test(3);
            fuzz_test(16);
            #[cfg(not(miri))] // Miri is too slow
                fuzz_test(189);
        }
    }

    #[test]
    fn test_delete_filter() {
        {
            let mut m = list_from(&[1, 2, 3, 4, 5, 6]);
            check_links(&m);
            m.delete_first_n_filter(3, |i| {
                *i % 2 == 0
            });
            check_links(&m);
            assert_eq!(m.len(), 3);
            for p in m.iter().zip([1, 3, 5].iter()) {
                assert_eq!(*p.1, *p.0);
            }
        }
        {
            let mut m = list_from(&[1, 2, 3, 4, 5, 6]);
            check_links(&m);
            m.delete_first_n_filter(2, |i| {
                *i % 2 == 0
            });
            check_links(&m);
            assert_eq!(m.len(), 4);
            for p in m.iter().zip([1, 3, 5, 6].iter()) {
                assert_eq!(*p.1, *p.0);
            }
        }
        {
            let mut m = list_from(&[0, 1, 2, 3, 4, 5, 6]);
            check_links(&m);
            m.delete_last_n_filter(4, |i| {
                *i % 2 == 0
            });
            check_links(&m);
            assert_eq!(m.len(), 3);
            for p in m.iter().zip([1, 3, 5].iter()) {
                assert_eq!(*p.1, *p.0);
            }
        }
        {
            let mut m = list_from(&[0, 1, 2, 3, 4, 5, 6]);
            check_links(&m);
            m.delete_last_n_filter(2, |i| {
                *i % 2 == 0
            });
            check_links(&m);
            assert_eq!(m.len(), 5);
            for p in m.iter().zip([0, 1, 2, 3, 5].iter()) {
                assert_eq!(*p.1, *p.0);
            }
        }
    }
}
