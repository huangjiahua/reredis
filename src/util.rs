use std::error::Error;
use std::num::ParseIntError;

pub fn case_eq(lhs: &[u8], rhs: &[u8]) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    for p in rhs
        .iter()
        .map(|x| x.to_ascii_lowercase())
        .zip(lhs.iter()) {
        if p.0 != *p.1 {
            return false;
        }
    }
    true
}

pub fn bytes_vec(b: &[u8]) -> Vec<u8> {
    b.iter().cloned().collect()
}

pub fn bytes_to_i64(b: &[u8]) -> Result<i64, Box<dyn Error>> {
    let s = std::str::from_utf8(b)?;
    let i = s.parse::<i64>()?;
    Ok(i)
}

pub fn bytes_to_usize(b: &[u8]) -> Result<usize, Box<dyn Error>> {
    let s = std::str::from_utf8(b)?;
    let i = s.parse::<usize>()?;
    Ok(i)
}

#[inline]
pub fn parse_usize(s: &str) -> Result<usize, ParseIntError> {
    s.parse::<usize>()
}

#[inline]
pub fn parse_port(s: &str) -> Result<u16, ParseIntError> {
    s.parse::<u16>()
}

#[inline]
pub fn parse_usize_pair(s1: &str, s2: &str) -> Result<(usize, usize), ParseIntError> {
    let a: usize = parse_usize(s1)?;
    let b: usize = parse_usize(s2)?;
    Ok((a, b))
}

pub fn human_size(s: &str) -> Result<usize, ParseIntError> {
    if let Ok(n) = parse_usize(s) {
        return Ok(n);
    }
    let err = s.parse::<usize>();

    let human = s.to_ascii_lowercase();
    if let Some(i) = human.find(|ch: char| { ch.is_ascii_alphabetic() }) {
        let (num, suffix) = human.split_at(i);
        let num: usize = match num.parse::<usize>() {
            Ok(n) => n,
            Err(_) => return err,
        };
        let power: usize = match suffix {
            "b" => 1,
            "kb" => 1024,
            "mb" => 1024 * 1024,
            "gb" => 1024 * 1024 * 1024,
            "tb" => 1024 * 1024 * 1024 * 1024,
            "pb" => 1024 * 1024 * 1024 * 1024 * 1024,
            _ => return err,
        };
        return Ok(num * power);
    }

    err
}

pub fn yes_or_no(s: &str) -> Option<bool> {
    if s.eq_ignore_ascii_case("yes") {
        return Some(true);
    } else if s.eq_ignore_ascii_case("no") {
        return Some(false);
    }
    None
}

pub fn is_prefix_of(p: &str, haystack: &str) -> bool {
    let p = p.as_bytes();
    let hay = haystack.as_bytes();
    if p.len() <= hay.len() {
        for i in 0..p.len() {
            if p[i] != hay[i] {
                break;
            }
            if i == p.len() - 1 {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_human_size() {
        assert_eq!(human_size("15").unwrap(), 15);
        assert_eq!(human_size("0").unwrap(), 0);
        assert_eq!(human_size("17B").unwrap(), 17);
        assert_eq!(human_size("17kb").unwrap(), 17 * 1024);
        assert_eq!(human_size("5gb").unwrap(), 5 * (1 << 30));
        assert!(human_size("kb").is_err());
        assert!(human_size("2mib").is_err());
    }

    #[test]
    fn test_prefix_of() {
        assert!(is_prefix_of("--", "--good"));
        assert!(is_prefix_of("--", "--"));
        assert!(!is_prefix_of("--", "dm"));
    }
}
