pub mod object;
//
//use std::rc::Rc;
//use std::cell::RefCell;
//use std::any::Any;
//use std::time::SystemTime;
//
//trait ObjectData {}
//
//type Robj = Rc<RefCell<RedisObject>>;
//type Pointer = Box<ObjectData>;
//
//trait ObjectPtr {
//    fn create_object(t: RobjType, ptr: Pointer) -> Robj;
//}
//
//impl ObjectPtr for Robj {
//    fn create_object(t: RobjType, ptr: Pointer) -> Rc<RefCell<RedisObject>> {
//        Rc::new(RefCell::new(
//            RedisObject {
//                obj_type: t,
//                encoding: RobjEncoding::Raw,
//                lru: SystemTime::now(),
//                ptr,
//            }
//        ))
//    }
//}
//
//struct RedisObject {
//    pub obj_type: RobjType,
//    pub encoding: RobjEncoding,
//    pub lru: SystemTime,
//    pub ptr: Pointer,
//}
//
//impl RedisObject {}
//
//#[derive(Copy, Clone)]
//enum RobjType {
//    String,
//    List,
//    Set,
//    Zset,
//    Hash,
//}
//
//#[derive(Copy, Clone)]
//enum RobjEncoding {
//    Raw,
//    Int,
//    Ht,
//    ZipMap,
//    LinkedList,
//    ZipList,
//    IntSet,
//    SkipList,
//    EmbStr,
//}
//
//

