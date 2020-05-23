use std::io::Write;
use std::process::abort;
use std::thread::sleep;
use std::time::Duration;

pub fn oom(s: &str) {
    write!(&mut std::io::stderr(), "{}: Out of memory\n", s).unwrap();
    std::io::stderr().flush().unwrap();
    sleep(Duration::from_secs(1));
    abort();
}
