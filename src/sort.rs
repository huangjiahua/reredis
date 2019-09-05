use crate::object::RobjPtr;
use std::cmp::Ordering;
use std::ops::Range;
use crate::util::bytes_to_i64;

pub fn parse_sort_command(cmd: &[RobjPtr]) -> Result<SortInfo, SortSyntaxError> {
    let mut info = SortInfo {
        options: SortOptions {
            sort_type: SortType::Numeric,
            sort_order: SortOrder::Asc,
        },
        limit: None,
        get: None,
        by: None,
        dst: None,
    };

    let mut idx: usize = 0;
    while idx < cmd.len() {
        let s = cmd[idx].borrow().string().to_ascii_lowercase();
        match &s[..] {
            b"asc" => info.options.sort_order = SortOrder::Asc,
            b"desc" => info.options.sort_order = SortOrder::Desc,
            b"alpha" => info.options.sort_type = SortType::Alphabetic,
            b"by" => {
                if idx + 1 >= cmd.len() {
                    return Err(SortSyntaxError::NotEnoughArguments);
                }
                info.by = Some(cmd[idx + 1].borrow().string().to_vec());
                idx += 1;
            }
            b"get" => {
                if idx + 1 >= cmd.len() {
                    return Err(SortSyntaxError::NotEnoughArguments);
                }
                if info.get.is_none() {
                    info.get = Some(Vec::with_capacity(1))
                }
                info.get
                    .as_mut()
                    .unwrap()
                    .push(cmd[idx + 1].borrow().string().to_vec());
                idx += 1;
            }
            b"store" => {
                if idx + 1 >= cmd.len() {
                    return Err(SortSyntaxError::NotEnoughArguments);
                }
                info.dst = Some(cmd[idx + 1].borrow().string().to_vec());
                idx += 1;
            }
            b"limit" => {
                if idx + 2 >= cmd.len() {
                    return Err(SortSyntaxError::NotEnoughArguments);
                }
                let left = match bytes_to_i64(cmd[idx + 1].borrow().string()) {
                    Ok(i) => std::cmp::max(0, i) as usize,
                    Err(_) => return Err(SortSyntaxError::LimitInvalid),
                };
                let right = match bytes_to_i64(cmd[idx + 2].borrow().string()) {
                    Ok(i) => {
                        if i < 0 {
                            std::usize::MAX
                        } else {
                            i as usize
                        }
                    },
                    Err(_) => return Err(SortSyntaxError::LimitInvalid),
                };
                info.limit = Some(left..right);
                idx += 2;
            }
            _ => return Err(SortSyntaxError::Unknown),
        }
        idx += 1;
    }

    Ok(info)
}

pub struct SortInfo {
    pub options: SortOptions,
    pub limit: Option<Range<usize>>,
    pub get: Option<Vec<Vec<u8>>>,
    pub by: Option<Vec<u8>>,
    pub dst: Option<Vec<u8>>,
}

pub enum SortSyntaxError {
    LimitInvalid,
    Unknown,
    NotEnoughArguments,
}

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
        l.0.borrow().string_cmp(&(r.0))
    }

    fn alphabetic_gt<T>(l: &(RobjPtr, T), r: &(RobjPtr, T)) -> Ordering {
        l.0.borrow().string_cmp(&(r.0)).reverse()
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

