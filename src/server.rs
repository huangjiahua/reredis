use log::LevelFilter;
use std::fs::File;
use crate::ae::*;
use mio::net::TcpListener;
use std::rc::Rc;
use std::cell::RefCell;
use crate::client::Client;
use crate::db::DB;


pub struct Server {
    pub stat_num_connections: usize,
    pub stat_num_commands: usize,

    pub require_pass: Option<Vec<u8>>,

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

    pub dirty: usize,
    pub db: Vec<DB>,
}

impl Server {
    pub fn new() -> Server {
        // TODO: change this
        let addr = "127.0.0.1:6379".parse().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        let fd = Rc::new(RefCell::new(Fdp::Listener(server)));

        let mut db: Vec<DB> = Vec::with_capacity(12);
        for i in 0..12 {
            db.push(DB::new(i));
        }

        Server {
            stat_num_connections: 0,
            stat_num_commands: 0,
            require_pass: None,
            max_clients: 100,
            clients: Vec::new(),
            fd,
            port: 6379,
            verbosity: LevelFilter::Debug,
            log_file: None,
            daemonize: false,
            dirty: 0,
            db,
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

    pub fn find_client(&self, c: &Client) -> Rc<RefCell<Client>> {
        let ptr1 = c as *const Client;
        for i in 0..self.clients.len() {
            let ptr2 = self.clients[i].as_ptr();
            if ptr1 == ptr2 {
                return Rc::clone(&self.clients[i]);
            }
        }
        unreachable!()
    }

    pub fn flush_db(&mut self, idx: usize) {
        self.db[idx] = DB::new(idx);
    }

    pub fn flush_all(&mut self) {
        for i in 0..self.db.len() {
            self.db[i] = DB::new(i);
        }
    }
}