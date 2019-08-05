use std::io::Write;
use std::time::Duration;
use std::thread::sleep;
use std::process::abort;

pub fn oom(s: &str) {
    write!(&mut std::io::stderr(), "{}: Out of memory\n", s);
    std::io::stderr().flush();
    sleep(Duration::from_secs(1));
    abort();
}