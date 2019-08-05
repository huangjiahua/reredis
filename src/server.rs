use crate::log::*;
use std::fs::File;

pub struct Server {
    // port
    pub port: u16,
    // min log level
    pub verbosity: LogLevel,
    // log file
    pub log_file: Option<File>,
    // whether server run as a daemon
    pub daemonize: bool,
}

impl Server {
    pub fn new() -> Server {
        Server {
            port: 6379,
            verbosity: LogLevel::Notice,
            log_file: None,
            daemonize: false,
        }
    }
}