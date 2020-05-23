use crate::object::{Robj, RobjPtr};
use crate::util::{bulk_reply_to_int, int_reply_to_int, multi_bulk_reply_to_int};
use rlua::{Context, Error, FromLua, ToLua, Value};
use std::collections::HashMap;
use std::ffi::CString;

#[derive(Clone)]
pub struct LuaRobj(RobjPtr);

#[derive(Clone)]
pub enum RobjFromLua {
    Nil,
    Robj(RobjPtr),
    Table(Vec<RobjFromLua>),
}

impl LuaRobj {
    pub fn new(obj: RobjPtr) -> LuaRobj {
        LuaRobj(obj)
    }

    pub fn into_obj_ptr(self) -> RobjPtr {
        self.0
    }
}

pub fn to_lua(obj: RobjPtr) -> LuaRobj {
    LuaRobj::new(obj)
}

impl<'lua> ToLua<'lua> for LuaRobj {
    fn to_lua(self, lua: Context<'lua>) -> Result<Value<'lua>, Error> {
        if let Ok(n) = self.0.borrow().object_to_long() {
            return n.to_lua(lua);
        }
        if let Ok(n) = self.0.borrow().parse_to_float() {
            return n.to_lua(lua);
        }
        if let Ok(s) = CString::new(self.0.borrow().string().to_vec()) {
            return s.to_lua(lua);
        }
        Err(rlua::Error::ToLuaConversionError {
            from: "",
            to: "",
            message: None,
        })
    }
}

impl<'lua> FromLua<'lua> for RobjFromLua {
    fn from_lua(lua_value: Value<'lua>, _lua: Context<'lua>) -> Result<Self, Error> {
        match lua_value {
            Value::Nil => Ok(Self::Nil),
            Value::Integer(n) => Ok(Self::Robj(Robj::create_string_object_from_long(n))),
            Value::Number(n) => Ok(Self::Robj(Robj::create_string_object_from_double(n))),
            Value::String(s) => Ok(Self::Robj(Robj::from_bytes(s.as_bytes().to_vec()))),
            Value::Table(t) => {
                let len = t.len()? as usize;
                let mut vec: Vec<RobjFromLua> = Vec::with_capacity(len);
                for j in 0..len {
                    let val: RobjFromLua = t.get(j)?;
                    vec.push(val);
                }
                Ok(Self::Table(vec))
            }
            _ => panic!("Unknown lua type"),
        }
    }
}

pub enum LuaRedis {
    Integer(i64),
    Bulk(CString),
    MultiBulk(Vec<LuaRedis>),
    Status(CString),
    Error(CString),
    Nil,
}

impl LuaRedis {
    pub fn new(reply: &[RobjPtr]) -> Self {
        let first = reply[0].borrow().string().to_vec();
        if first[0] == b'+' {
            return Self::Status(CString::new(first[1..first.len() - 2].to_vec()).unwrap());
        }
        if first[0] == b'-' {
            return Self::Error(CString::new(first[1..first.len() - 2].to_vec()).unwrap());
        }
        if first[0] == b'$' {
            assert!(reply.len() > 1);
            if bulk_reply_to_int(&first) == -1 {
                return Self::Nil;
            }
            return Self::Bulk(CString::new(reply[1].borrow().string().to_vec()).unwrap());
        }
        if first[0] == b':' {
            return Self::Integer(int_reply_to_int(&first));
        }
        if first[0] == b'*' {
            if multi_bulk_reply_to_int(&first) == -1 {
                return Self::Nil;
            }
            let mut v = vec![];
            let mut i = 1;
            while i < reply.len() {
                let o = LuaRedis::new(&reply[i..]);
                v.push(o);
                if reply[i].borrow().string()[0] == b'$' {
                    i += 2;
                }
                i += 1;
            }
            return LuaRedis::MultiBulk(v);
        }
        LuaRedis::Nil
    }
}

impl<'lua> ToLua<'lua> for LuaRedis {
    fn to_lua(self, lua: Context<'lua>) -> Result<Value<'lua>, Error> {
        match self {
            Self::Integer(i) => i.to_lua(lua),
            Self::Bulk(s) => s.to_lua(lua),
            Self::MultiBulk(v) => v.to_lua(lua),
            Self::Status(s) => {
                let mut map: HashMap<String, CString> = HashMap::new();
                map.insert("ok".to_string(), s).unwrap();
                map.to_lua(lua)
            }
            Self::Error(s) => {
                let mut map: HashMap<String, CString> = HashMap::new();
                map.insert("ok".to_string(), s).unwrap();
                map.to_lua(lua)
            }
            Self::Nil => false.to_lua(lua),
        }
    }
}

impl<'lua> FromLua<'lua> for LuaRedis {
    fn from_lua(lua_value: Value<'lua>, _lua: Context<'lua>) -> Result<Self, Error> {
        let r = match lua_value {
            Value::Integer(i) => LuaRedis::Integer(i),
            Value::Number(n) => LuaRedis::Integer(n as i64),
            Value::Table(t) => {
                if t.contains_key("ok")? {
                    let status: CString = t.get("ok")?;
                    LuaRedis::Status(status)
                } else if t.contains_key("err")? {
                    let error: CString = t.get("err")?;
                    LuaRedis::Error(error)
                } else {
                    let len = t.len()?;
                    let mut v = Vec::with_capacity(len as usize);
                    for i in 1..len + 1 {
                        let val: LuaRedis = t.get(i)?;
                        v.push(val);
                    }
                    LuaRedis::MultiBulk(v)
                }
            }
            Value::Nil => LuaRedis::Nil,
            Value::Boolean(b) => {
                if b {
                    LuaRedis::Integer(1)
                } else {
                    LuaRedis::Nil
                }
            }
            Value::Error(e) => {
                return Err(e);
            }
            _ => unreachable!(),
        };
        Ok(r)
    }
}

const BOILERPLATE_BEGIN: &[u8] = br#"
redis = {}
redis.call = function(...)
    local args = {...}
    redis_call_internal(args)
    local ret = RETURN_FROM_RUST
    RETURN_FROM_RUST = nil
    return ret
end

function eval_outer()
--user code begin
"#;

const BOILERPLATE_END: &[u8] = br#"
--user code end
end

-- defined in scope end

-- running code begin
BACK_TO_RUST = eval_outer()
if BACK_TO_RUST ~= nil then
    print(BACK_TO_RUST)
end
-- running code en
"#;
