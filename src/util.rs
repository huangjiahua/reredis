use std::error::Error;
use std::num::ParseIntError;
use std::time::{Duration, SystemTime};

pub fn case_eq(lhs: &[u8], rhs: &[u8]) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    for p in rhs.iter().map(|x| x.to_ascii_lowercase()).zip(lhs.iter()) {
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

pub fn bytes_to_f64(b: &[u8]) -> Result<f64, Box<dyn Error>> {
    let s = std::str::from_utf8(b)?;
    let n = s.parse::<f64>()?;
    Ok(n)
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

pub fn parse_port_from_bytes(b: &[u8]) -> Result<u16, Box<dyn Error>> {
    let s = std::str::from_utf8(b)?;
    let port = parse_port(s)?;
    Ok(port)
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
    if let Some(i) = human.find(|ch: char| ch.is_ascii_alphabetic()) {
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

pub fn generate_key_from_pattern(pat: &[u8], s: &[u8]) -> Vec<u8> {
    let k = match pat.iter().enumerate().filter(|c| *((*c).1) == b'*').next() {
        Some(i) => i.0,
        None => return pat.to_vec(),
    };
    let mut ret: Vec<u8> = Vec::with_capacity(pat.len() + s.len() - 1);
    ret.extend_from_slice(&pat[0..k]);
    ret.extend_from_slice(&s[..]);
    ret.extend_from_slice(&pat[k + 1..]);
    ret
}

pub fn unix_timestamp(t: &SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn to_system_time(timestamp: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp)
}

fn reply_preceding_to_int(bytes: &[u8]) -> i64 {
    let content = &bytes[1..bytes.len() - 2];
    bytes_to_i64(content).unwrap()
}

pub fn int_reply_to_int(bytes: &[u8]) -> i64 {
    assert!(bytes.len() > 3);
    assert_eq!(bytes[0], b':');
    reply_preceding_to_int(bytes)
}

pub fn bulk_reply_to_int(bytes: &[u8]) -> i64 {
    assert!(bytes.len() > 3);
    assert_eq!(bytes[0], b'$');
    reply_preceding_to_int(bytes)
}

pub fn multi_bulk_reply_to_int(bytes: &[u8]) -> i64 {
    assert!(bytes.len() > 3);
    assert_eq!(bytes[0], b'*');
    reply_preceding_to_int(bytes)
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

    #[test]
    fn test_integer_reply_to_integer() {
        assert_eq!(int_reply_to_int(b":1\r\n"), 1);
        assert_eq!(int_reply_to_int(b":1000\r\n"), 1000);
        assert_eq!(int_reply_to_int(b":-1000\r\n"), -1000);
    }
}
