use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;
use std::time::SystemTime;

type Pointer = Rc<RefCell<dyn Any>>;

struct Robj {
    pub obj_type: RobjType,
    pub encoding: RobjEncoding,
    pub lru: SystemTime,
    pub ptr: Pointer,
}

#[derive(Copy, Clone)]
enum RobjType {
    String,
    List,
    Set,
    Zset,
    Hash,
}

#[derive(Copy, Clone)]
enum RobjEncoding {
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

impl Robj {
    fn create_object(otype: RobjType, ptr: Pointer) -> Robj {
        Robj {
            obj_type: otype,
            encoding: Raw,
            lru: SystemTime::now(),
            ptr,
        }
    }

}


#[cfg(test)]
mod test {
    use super::*;



}

