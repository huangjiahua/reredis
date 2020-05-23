use crate::object::{Robj, RobjPtr};

thread_local!(
    pub static CRLF: RobjPtr = Robj::create_string_object("\r\n");
    pub static OK: RobjPtr = Robj::create_string_object("+OK\r\n");
    pub static ERR: RobjPtr = Robj::create_string_object("-ERR\r\n");
    pub static EMPTY_BULK: RobjPtr = Robj::create_string_object("$0\r\n\r\n");
    pub static CZERO: RobjPtr = Robj::create_string_object(":0\r\n");
    pub static CONE: RobjPtr = Robj::create_string_object(":1\r\n");
    pub static NULL_BULK: RobjPtr = Robj::create_string_object("$-1\r\n");
    pub static NULL_MULTI_BULK: RobjPtr = Robj::create_string_object("*-1\r\n");
    pub static EMPTY_MULTI_BULK: RobjPtr = Robj::create_string_object("*0\r\n");
    pub static PONG: RobjPtr = Robj::create_string_object("+PONG\r\n");
    pub static COLON: RobjPtr = Robj::create_string_object(":");
    pub static WRONG_TYPE: RobjPtr = Robj::create_string_object(
        "-WRONGTYPE Operation \
        against a key holding the wrong kind of value\r\n",
    );
);

#[macro_export]
macro_rules! shared_object {
    ($($x: expr), *) => {
        $(
            ($x).with( |k| {
                Rc::clone(&k)
            })
        )*
    };
}
