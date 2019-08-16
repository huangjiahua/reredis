pub fn case_eq(lhs: &str, rhs: &str) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    let r = lhs.as_bytes();
    for p in rhs.as_bytes()
        .iter()
        .map(|x| x.to_ascii_lowercase())
        .zip(r.iter()) {
        if p.0 != *p.1 {
            return false;
        }
    }
    true
}