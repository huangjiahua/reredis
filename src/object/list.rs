use crate::object::linked_list::LinkedList;
use crate::object::RobjPtr;

pub type List = LinkedList<RobjPtr>;

#[derive(Clone, Copy, PartialEq)]
pub enum ListWhere {
    Head,
    Tail,
}

//impl ObjectData for List {
//    fn list_ref(&self) -> &List {
//        self
//    }
//}

//struct List<T>(LinkedList<T>);
//
//impl<T> List<T> {
//    fn len(&self) -> usize {
//        self.0.len()
//    }
//
//    fn first(&self) -> Option<&T> {
//        self.0.front()
//    }
//
//    fn first_mut(&mut self) -> Option<&mut T> {
//        self.0.front_mut()
//    }
//
//    fn last(&self) -> Option<&T> {
//        self.0.back()
//    }
//
//    fn last_mut(&mut self) -> Option<&mut T> {
//        self.0.back_mut()
//    }
//
//    fn new() -> Self {
//        LinkedList::new()
//    }
//
//    fn release(&mut self) {
//        self.0.clear();
//    }
//}
