use std::mem::swap;

fn glob_match(pattern: &[u8], string: &[u8], no_case: bool) -> bool {
    let mut pattern = pattern;
    let mut string = string;
    let mut last_pattern: &[u8];

    while pattern.len() > 0 {
        match pattern[0] {
            b'*' => {
                while pattern[1] == b'*' {
                    pattern = &pattern[1..];
                }
                if pattern.len() == 1 {
                    return true;
                }
                while string.len() > 0 {
                    if glob_match(&pattern[1..], string, no_case) {
                        return true;
                    }
                    string = &string[1..];
                }
                return false;
            }
            b'?' => {
                if string.len() == 0 {
                    return false;
                }
                string = &string[1..];
            }
            b'[' => {
                let not: bool;
                let mut fit: bool;

                last_pattern = pattern;
                pattern = &pattern[1..];

                not = pattern[0] == b'^';
                if not {
                    last_pattern = pattern;
                    pattern = &pattern[1..];
                }
                fit = false;
                loop {
                    if pattern.len() > 0 && pattern[0] == b'\\' {
                        pattern = &pattern[1..];
                        if pattern[0] == string[0] {
                            fit = true;
                        }
                    } else if pattern.len() > 0 && pattern[0] == b']' {
                        break;
                    } else if pattern.len() == 0 {
                        pattern = last_pattern;
                        break;
                    } else if pattern.len() > 1 && pattern[1] == b'-' && pattern.len() > 3 {
                        let mut start: u8 = pattern[0];
                        let mut end: u8 = pattern[2];
                        let mut c: u8 = string[0];
                        if start > end {
                            swap(&mut start, &mut end);
                        }
                        if no_case {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        pattern = &pattern[2..];
                        if c >= start && c <= end {
                            fit = true;
                        }
                    } else {
                        if !no_case {
                            if pattern[0] == string[0] {
                                fit = true;
                            }
                        } else {
                            if pattern[0].to_ascii_lowercase() == string[0].to_ascii_lowercase() {
                                fit = true;
                            }
                        }
                    }
                    last_pattern = pattern;
                    pattern = &pattern[1..];
                }
                if not {
                    fit = !fit;
                }
                if !fit {
                    return false;
                }
                string = &string[1..];
            }
            _ => {
                if pattern[0] == b'\\' && pattern.len() >= 2 {
                    pattern = &pattern[1..];
                }

                if !no_case {
                    if pattern[0] != string[0] {
                        return false;
                    }
                } else {
                    if pattern[0].to_ascii_lowercase() != string[0].to_ascii_lowercase() {
                        return false;
                    }
                }
                string = &string[1..];
            }
        }
        pattern = &pattern[1..];
        if string.len() == 0 {
            while pattern.len() > 0 && pattern[0] == b'*' {
                pattern = &pattern[1..];
            }
            break;
        }
    }
    if pattern.len() == 0 && string.len() == 0 {
        true
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pattern() {
        assert!(glob_match(b"a*b", b"a_b", false));
        assert!(glob_match(b"a*b", b"a__b", false));
        assert!(glob_match(b"a**b", b"a_b", false));
        assert!(glob_match(b"a**b**", b"a_b", false));
    }

    #[test]
    fn test_unclosed_brackets() {
        assert!(!glob_match(b"abc[def", b"abc[def", false));
        assert!(glob_match(b"abc[def", b"abcd", false));
    }

    fn case_match(pat: &str, s: &str) {
        assert!(glob_match(pat.as_bytes(), s.as_bytes(), false));
    }

    fn no_case_match(pat: &str, s: &str) {
        assert!(glob_match(pat.as_bytes(), s.as_bytes(), true));
    }

    fn case_not_match(pat: &str, s: &str) {
        assert!(!glob_match(pat.as_bytes(), s.as_bytes(), false));
    }

    #[test]
    fn test_wildcards() {
        case_match("a*b", "a_b");
        case_match("a*b*c", "abc");
        case_not_match("a*b*c", "abcd");
        case_match("a*b*c", "a_b_c");
        case_match("a*b*c", "a____b__c");
        case_match("abc*abc*abc", "abcabcabcabcabc");
        case_not_match("abc*abc*abc", "abcabcabcabcabca");
        case_match("a*b[xyz]c*d", "abxcdbxcddd");
    }

    #[test]
    fn test_range_pattern() {
        let pat = "a[0-9]b";
        for i in 0..10 {
            case_match(pat, &format!("a{}b", i));
        }
        case_not_match(pat, "a_b");

        let pat = "a[^0-9]b";
        for i in 0..10 {
            case_not_match(pat, &format!("a{}b", i));
        }
        case_match(pat, "a_b");

        let pats = ["[a-z123]", "[1a-z23]", "[123z-a]"];
        for &p in pats.iter() {
            for c in "abcdefghijklmnopqrstuvwxyz".chars() {
                case_match(p, &c.to_string());
            }

            for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars() {
                case_not_match(p, &c.to_string());
                no_case_match(p, &c.to_string());
            }

            case_match(p, "1");
            case_match(p, "2");
            case_match(p, "3");
        }

        let pats = ["[abc-]", "[-abc]", "[a-c-]"];
        for &p in pats.iter() {
            case_match(p, "a");
            case_match(p, "b");
            case_match(p, "c");
            case_match(p, "-");
            case_not_match(p, "d");
        }

        case_match("[-]", "-");
        case_not_match("[^-]", "-");
    }
}

