extern crate chrono;

use std::io;

#[derive(Copy, Clone)]
pub enum LogLevel {
    Debug = 0,
    Notice = 1,
    Warning = 2,
}

pub fn write_log(level: LogLevel, min: LogLevel, w: &mut dyn io::Write, s: &str) {
    if level as i32 >= min as i32 {
        let t = chrono::Local::now().format("%d %m %H:%M:%S");
        let c = match level {
            LogLevel::Debug => ".",
            LogLevel::Notice => "-",
            LogLevel::Warning => "*",
        };
        writeln!(w, "{} {} {}", t, c, s);
    }
}