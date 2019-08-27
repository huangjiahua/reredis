use log::LevelFilter;
use std::fs::File;
use crate::ae::*;
use mio::net::TcpListener;
use std::rc::Rc;
use std::cell::RefCell;
use crate::client::Client;
use crate::db::DB;
use crate::env::Config;
use std::net::SocketAddr;
use std::time::SystemTime;
use crate::object::linked_list::LinkedList;


pub struct Server {
    pub port: u16,
    pub fd: Fd,
    pub db: Vec<DB>,
    // TODO: sharing pool
    pub dirty: usize,
    pub clients: LinkedList<Rc<RefCell<Client>>>,
    // TODO: slaves and monitors
    pub cron_loops: usize,
    pub last_save: SystemTime,
    pub used_memory: usize,

    // for stats
    pub stat_start_time: SystemTime,
    pub stat_num_commands: usize,
    pub stat_num_connections: usize,

    // configuration
    pub verbosity: LevelFilter,
    pub glue_output: bool,
    pub max_idle_time: usize,
    pub daemonize: bool,
    pub bg_save_in_progress: bool,
    pub bg_save_child_pid: i64,
    pub save_params: Vec<(usize, usize)>,
    pub log_file: Option<File>,
    pub bind_addr: String,
    pub db_filename: String,
    pub require_pass: Option<String>,
    // TODO: replication
    pub max_clients: usize,
}

impl Server {
    pub fn new(config: &Config) -> Server {
        // TODO: change this
        let addr: SocketAddr = format!("{}:{}", config.bind_addr, config.port).parse().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        let fd = Rc::new(RefCell::new(Fdp::Listener(server)));

        let mut db: Vec<DB> = Vec::with_capacity(config.db_num);
        for i in 0..config.db_num {
            db.push(DB::new(i));
        }

        let log_file = match &config.log_file {
            Some(f) => Some(File::open(f).unwrap()),
            None => None,
        };

        Server {
            port: config.port,
            fd,
            db,
            dirty: 0,
            clients: LinkedList::new(),

            cron_loops: 0,
            last_save: SystemTime::now(),
            used_memory: 0,

            stat_start_time: SystemTime::now(),
            stat_num_commands: 0,
            stat_num_connections: 0,

            verbosity: config.log_level,
            glue_output: config.glue_output,
            max_idle_time: config.max_idle_time,
            daemonize: config.daemonize,
            bg_save_in_progress: false,
            bg_save_child_pid: -1,
            save_params: config.save_params.clone(),
            log_file,
            bind_addr: config.bind_addr.clone(),
            db_filename: config.db_filename.clone(),
            require_pass: config.require_pass.clone(),

            max_clients: config.max_clients,
        }
    }

    pub fn free_client(&mut self, c: &Rc<RefCell<Client>>) {
        let mut i: i64 = -1;
        for (k, client) in self.clients.iter().enumerate() {
            if Rc::ptr_eq(&c, client) {
                i = k as i64;
                break;
            }
        }
        if i >= 0 {
            let mut tmp = self.clients.split_off(i as usize);
            tmp.pop_front();
            self.clients.append(&mut tmp);
        }
    }

    pub fn find_client(&self, c: &Client) -> Rc<RefCell<Client>> {
        let ptr1 = c as *const Client;
        for client in self.clients.iter() {
            let ptr2 = client.as_ptr();
            if ptr1 == ptr2 {
                return Rc::clone(client);
            }
        }
        unreachable!()
    }

    pub fn close_timeout_clients(&mut self, _el: &mut AeEventLoop) {
        assert!(self.max_idle_time > 0);
        let now = SystemTime::now();
        let len = self.clients.len();
        let max_idle_time = self.max_idle_time;
        self.clients.delete_first_n_filter(len, |x| {
            let elapsed =
                now.duration_since(x.borrow().last_interaction)
                    .unwrap()
                    .as_secs() as usize;
            if elapsed > max_idle_time {
                true
            } else {
                false
            }
        })
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