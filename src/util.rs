use std::error::Error;

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
