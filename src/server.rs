use log::LevelFilter;
use std::fs::File;
use crate::ae::*;
use mio::net::TcpListener;
use std::rc::Rc;
use std::error::Error;
use std::cell::RefCell;
use crate::client::Client;


pub struct Server {
    pub stat_num_connections: usize,
    pub stat_num_commands: usize,


    pub max_clients: usize,
    pub clients: Vec<Rc<RefCell<Client>>>,
    pub fd: Fd,
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
        let fd = Rc::new(RefCell::new(Fdp::Listener(server)));
        Server {
            stat_num_connections: 0,
            stat_num_commands: 0,
            max_clients: 100,
            clients: Vec::new(),
            fd,
            port: 6379,
            verbosity: LevelFilter::Debug,
            log_file: None,
            daemonize: false,
        }
    }

    pub fn free_client(&mut self, c: &Rc<RefCell<Client>>) {
        for i in 0..self.clients.len() {
            if Rc::ptr_eq(&c, &self.clients[i]) {
                self.clients.remove(i);
                return;
            }
        }
    }
}