use log::LevelFilter;
use std::fs::File;
use crate::ae::*;
use mio::net::TcpListener;
use mio::net;
use std::rc::Rc;
use std::cell::RefCell;
use crate::client::{Client, ReplyState, CLIENT_MASTER};
use crate::db::DB;
use crate::env::Config;
use std::net::SocketAddr;
use std::time::SystemTime;
use crate::object::linked_list::LinkedList;
use crate::{zalloc, rdb};
use crate::object::RobjPtr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::error::Error;
use std::io::{Write, BufReader, BufRead, Read};
use crate::util::*;
use rand::prelude::*;
use std::fs;


pub struct Server {
    pub port: u16,
    pub fd: Fd,
    pub db: Vec<DB>,
    // TODO: sharing pool
    pub dirty: usize,
    pub clients: LinkedList<Rc<RefCell<Client>>>,
    pub slaves: LinkedList<Rc<RefCell<Client>>>,
    pub monitors: LinkedList<Rc<RefCell<Client>>>,
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
    pub bg_save_child_pid: i32,
    pub save_params: Vec<(usize, usize)>,
    pub log_file: Option<File>,
    pub bind_addr: String,
    pub db_filename: String,
    pub require_pass: Option<String>,
    // TODO: replication
    pub is_slave: bool,
    pub master_host: Option<String>,
    pub master_port: u16,
    pub master: Option<Rc<RefCell<Client>>>,
    pub reply_state: ReplyState,
    pub max_clients: usize,
    pub max_memory: usize,

    pub shutdown_asap: Arc<AtomicBool>,
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

        let shutdown_asap = Arc::new(AtomicBool::new(false));
        set_up_signal_handling(&shutdown_asap);

        Server {
            port: config.port,
            fd,
            db,
            dirty: 0,
            clients: LinkedList::new(),
            slaves: LinkedList::new(),
            monitors: LinkedList::new(),

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

            is_slave: false,
            master_host: None,
            master_port: 0,
            master: None,
            reply_state: ReplyState::None,
            max_clients: config.max_clients,
            max_memory: config.max_memory,

            shutdown_asap,
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

    pub fn transfer_client_to_slaves(&mut self, c: &Client, monitor: bool) {
        let c = self.find_client(c);
        if monitor {
            self.monitors.push_back(c);
        } else {
            self.slaves.push_back(c);
        }
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

    pub fn free_memory_if_needed(&mut self) {
        while self.max_memory > 0 && zalloc::allocated_memory() > self.max_memory {
            // for now only keys in expires table will be freed
            let mut freed: usize = 0;
            for db in self.db.iter_mut() {
                if db.expires.len() == 0 {
                    continue;
                }
                let mut min_key: Option<RobjPtr> = None;
                let mut min_t: Option<SystemTime> = None;

                for _ in 0..3 {
                    let (key, t) = db.expires.random_key_value();
                    match min_t {
                        None => {
                            min_key = Some(Rc::clone(key));
                            min_t = Some(t.clone());
                        }
                        Some(time) => {
                            if *t < time {
                                min_key = Some(Rc::clone(key));
                                min_t = Some(t.clone());
                            } else {
                                min_t = Some(time);
                            }
                        }
                    }
                }
                if let Some(key) = min_key {
                    let _ = db.delete_key(&key);
                    freed += 1;
                }
            }
            if freed == 0 {
                return;
            }
        }
    }

    pub fn flush_db(&mut self, idx: usize) {
        self.db[idx] = DB::new(idx);
    }

    pub fn flush_all(&mut self) {
        for i in 0..self.db.len() {
            self.db[i] = DB::new(i);
        }
    }

    pub fn prepare_shutdown(&mut self) {
        if self.bg_save_in_progress {
            warn!("There is a living child. Killing it!");
            rdb::rdb_kill_background_saving(self);
        }

        match rdb::rdb_save(self) {
            Ok(_) => {
                warn!("{} bytes used at exit", crate::zalloc::allocated_memory());
                warn!("Server exit now, bye bye...");
            }
            Err(e) => {
                warn!("Error trying to save the DB: {}", e.description());
            }
        }
    }

    pub fn sync_with_master(&mut self) -> Result<(), Box<dyn Error>> {
        let addr = self.master_host.as_ref().unwrap();
        let addr = format!("{}:{}", addr, self.master_port).parse()?;
        let mut buf: [u8; 1024] = [0; 1024];
        let mut line_buf = String::from("");
        let mut socket = net::TcpStream::connect(&addr).map_err(|e| {
            warn!("Unable to connect to MASTER: {}", addr);
            e
        })?;

        let mut rng = rand::thread_rng();

        // sync write
        socket.write_all(b"*1\r\n$4\r\nsync\r\n").map_err(|e| {
            warn!("I/O error writing to MASTER: {}", e.description());
            e
        })?;

        let mut reader = BufReader::new(socket);
        //sync read
        reader.read_line(&mut line_buf).map_err(|e| {
            warn!("I/O reading bulk count from MASTER: {}", e.description());
            e
        })?;
        let mut dump_size = (&line_buf[1..]).parse::<usize>().map_err(|e| {
            warn!("Error parsing dump size: {}", e.description());
            e
        })?;
        info!("Receiving {} bytes data dump from MASTER", dump_size);

        let temp_file = format!(
            "temp-{}.{}.rdb",
            unix_timestamp(&SystemTime::now()),
            rng.gen::<usize>(),
        );

        {
            let mut file = File::open(&temp_file).map_err(|e| {
                warn!("Opening the temp file needed for MASTER <-> SLAVE synchronization: {}",
                      e.description());
                e
            })?;

            while dump_size > 0 {
                let buf = &mut buf[..std::cmp::min(1024, dump_size)];
                reader.read_exact(buf).map_err(|e| {
                    warn!("I/O error trying to sync with MASTER: {}", e.description());
                    e
                })?;
                file.write_all(buf).map_err(|e| {
                    warn!("Write error writing to the DB dump file needed for MASTER <-> \
                       SLAVE synchronization: {}", e.description());
                    e
                })?;
                dump_size -= buf.len();
            }
        }

        fs::rename(&temp_file, &self.db_filename).map_err(|e| {
            warn!("Failed trying to rename the temp DB into dump.rdb \
                   in MASTER <-> SLAVE synchronization: {}", e.description());
            let _ = fs::remove_file(&temp_file);
            e
        })?;
        self.flush_all();

        self.rdb_load().map_err(|e| {
            warn!("Failed trying to load the MASTER synchronization DB from disk");
            e
        })?;

        let socket = reader.into_inner();
        let fd = Rc::new(RefCell::new(Fdp::Stream(socket)));
        let master = Client::with_fd(fd);
        master.borrow_mut().flags |= CLIENT_MASTER;
        self.master = Some(master);

        self.reply_state = ReplyState::Connected;

        Ok(())
    }

    pub fn rdb_load(&mut self) -> std::io::Result<()> {
        rdb::rdb_load(self)
    }
}

fn set_up_signal_handling(sig_term_sign: &Arc<AtomicBool>) {
    signal_hook::flag::register(
        signal_hook::SIGTERM,
        Arc::clone(sig_term_sign),
    ).unwrap();
    signal_hook::flag::register(
        signal_hook::SIGINT,
        Arc::clone(sig_term_sign),
    ).unwrap();
    // ignore SIGPIPE and SIGHUP
    let useless_flag = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(
        signal_hook::SIGPIPE,
        Arc::clone(&useless_flag),
    ).unwrap();
    signal_hook::flag::register(
        signal_hook::SIGHUP,
        useless_flag,
    ).unwrap();
}