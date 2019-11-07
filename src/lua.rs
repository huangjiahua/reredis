use crate::object::{RobjPtr, Robj};
use rlua::{ToLua, Context, Value, Error, FromLua};
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
        Err(rlua::Error::ToLuaConversionError { from: "", to: "", message: None })
    }
}

impl<'lua> FromLua<'lua> for RobjFromLua {
    fn from_lua(lua_value: Value<'lua>, _lua: Context<'lua>) -> Result<Self, Error> {
        match lua_value {
            Value::Nil => Ok(Self::Nil),
            Value::Integer(n) => Ok(Self::Robj(Robj::create_int_object(n))),
            Value::Number(n) =>
                Ok(Self::Robj(Robj::create_string_object_from_double(n))),
            Value::String(s) =>
                Ok(Self::Robj(Robj::from_bytes(s.as_bytes().to_vec()))),
            Value::Table(t) => {
                let len = t.len()? as usize;
                let mut vec: Vec<RobjFromLua> = Vec::with_capacity(len);
                for j in 0..len {
                    let val: RobjFromLua = t.get(j)?;
                    vec.push(val);
                }
                Ok(Self::Table(vec))
            }
            _ => panic!("Unknown lua type")
        }
    }
}