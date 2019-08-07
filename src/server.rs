use log::LevelFilter;
use std::fs::File;
use crate::ae::*;
use mio::net::TcpListener;
use std::rc::Rc;
use std::error::Error;

pub fn accept_handler(el: &mut AeEventLoop, fd: &Fd, data: &Box<dyn ClientData>, mask: i32) {
    let listener = fd.unwrap_listener();

    debug!("ready to accept");
    let r = listener.accept();

    let (stream, info) = match r {
        Ok(p) => p,
        Err(e) => {
            debug!("Accepting client connection: {}", e.description());
            return;
        }
    };
    debug!("Accepted {}:{}", info.ip(), info.port());
}


pub struct Server {
    pub fd: Fd,
    pub el: AeEventLoop,
    // port
    pub port: u16,
    // min log level
    pub verbosity: LevelFilter,
    // log file
    pub log_file: Option<File>,
    // whether server run as a daemon
    pub daemonize: bool,
}

impl Server {
    pub fn new() -> Server {
        // TODO: change this
        let addr = "127.0.0.1:6379".parse().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        let fd = Rc::new(Fdp::Listener(server));
        Server {
            fd,
            el: AeEventLoop::new(1024),
            port: 6379,
            verbosity: LevelFilter::Debug,
            log_file: None,
            daemonize: false,
        }
    }
}