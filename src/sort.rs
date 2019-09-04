use crate::object::RobjPtr;
use std::cmp::Ordering;

#[derive(Copy, Clone, PartialEq)]
pub enum SortType {
    Numeric,
    Alphabetic,
}

#[derive(Copy, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Copy, Clone, PartialEq)]
pub enum SortError {
    ParseNumericError,
}

type CmpFn<T> = fn(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering;

pub struct SortOptions {
    pub sort_type: SortType,
    pub sort_order: SortOrder,
}


impl SortOptions {
    pub fn sort<T>(&self, v: &mut Vec<(RobjPtr, T)>) -> Result<(), SortError> {
        if self.sort_type == SortType::Numeric && !Self::all_numeric(v) {
            return Err(SortError::ParseNumericError);
        }
        v.sort_unstable_by(self.get_cmp_func());
        Ok(())
    }

    fn all_numeric<T>(v: &mut Vec<(RobjPtr, T)>) -> bool {
        for o in v.iter().map(|t| &t.0) {
            if let Err(_) = o.borrow().parse_to_float() {
                return false;
            }
        }
        true
    }

    fn numeric_lt<T>(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering {
        l.0.borrow().float().partial_cmp(&r.0.borrow().float()).unwrap()
    }

    fn numeric_gt<T>(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering {
        l.0.borrow().float().partial_cmp(&r.0.borrow().float()).unwrap().reverse()
    }

    fn alphabetic_lt<T>(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering {
        l.0.borrow().string().cmp(r.0.borrow().string())
    }

    fn alphabetic_gt<T>(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering {
        l.0.borrow().string().cmp(r.0.borrow().string()).reverse()
    }

    fn get_cmp_func<T>(&self) -> CmpFn<T> {
        use SortType::*;
        use SortOrder::*;
        match (self.sort_type, self.sort_order) {
            (Numeric, Asc) => Self::numeric_lt,
            (Numeric, Desc) => Self::numeric_gt,
            (Alphabetic, Asc) => Self::alphabetic_lt,
            (Alphabetic, Desc) => Self::alphabetic_gt,
        }
    }
}

