use crate::ae::{default_ae_event_finalizer_proc, AeEventLoop, Fd, Fdp, AE_READABLE, AE_WRITABLE};
use crate::client::*;
use crate::object::RobjEncoding;
use crate::rdb;
use crate::replicate;
use crate::server::Server;
use crate::util::*;
use crate::zalloc;
use chrono::Local;
use log::{Level, LevelFilter};
use mio::net::TcpStream;
use nix::sys::wait::*;
use nix::unistd::Pid;
use std::cell::RefCell;
use std::env::set_current_dir;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::process::exit;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

pub const REREDIS_VERSION: &str = "0.0.1";
pub const REREDIS_REQUEST_MAX_SIZE: usize = 1024 * 1024 * 256;
pub const REREDIS_MAX_WRITE_PER_EVENT: usize = 64 * 1024;
pub const REREDIS_EXPIRE_LOOKUPS_PER_CRON: usize = 100;
pub const REREIDS_IO_BUF_LEN: usize = 1024;

#[derive(Clone)]
pub struct Config {
    pub config_file: Option<String>,
    pub max_idle_time: usize,
    pub port: u16,
    pub bind_addr: String,
    pub log_level: LevelFilter,
    pub log_file: Option<String>,
    pub db_num: usize,
    pub max_clients: usize,
    pub max_memory: usize,
    pub master_host: Option<String>,
    pub master_port: u16,
    pub glue_output: bool,
    pub daemonize: bool,
    pub require_pass: Option<String>,
    pub db_filename: String,
    pub save_params: Vec<(usize, usize)>,
}

impl Config {
    pub fn new() -> Config {
        Config {
            config_file: None,
            max_idle_time: 5 * 60,
            port: 6379,
            bind_addr: "127.0.0.1".to_string(),
            log_level: LevelFilter::Debug,
            log_file: None,
            db_num: 16,
            max_clients: 0,
            max_memory: 0,
            master_host: None,
            master_port: 6379,
            glue_output: true,
            daemonize: false,
            require_pass: None,
            db_filename: "dump.rdb".to_string(),
            save_params: vec![(3600, 1), (300, 100), (60, 10000)],
        }
    }

    pub fn reset_server_save_params(&mut self) {
        self.save_params.clear();
    }

    pub fn config_from_args(&mut self, args: &[String]) {
        let mut first: bool = false;
        let mut content: String = String::new();
        for s in args.iter().skip(1) {
            if !first && !is_prefix_of("--", s) {
                Self::args_error(s, "option name should begin with '--'");
            }
            first = true;
            if is_prefix_of("--", s) {
                fmt::write(&mut content, format_args!("\n{} ", &s[2..])).unwrap();
            } else {
                fmt::write(&mut content, format_args!("{} ", s)).unwrap();
            }
        }
        self.load_config_from_string(&content);
    }

    fn args_error(s: &str, desc: &str) {
        eprintln!("argument error > {}: {}", s, desc);
        exit(1);
    }

    pub fn load_server_config(&mut self, filename: &str) {
        let mut contents = String::new();

        if filename != "-" {
            let mut file = File::open(filename).unwrap_or_else(|e| {
                eprintln!(
                    "Fatal error, can't open config file {}: {}",
                    filename,
                    &e.to_string()
                );
                exit(1);
            });

            file.read_to_string(&mut contents).unwrap_or_else(|e| {
                eprintln!(
                    "Fatal error, can't read config file {}: {}",
                    filename,
                    &e.to_string()
                );
                exit(1);
            });
        } else {
            let stdin = io::stdin();
            let mut handle = stdin.lock();

            handle.read_to_string(&mut contents).unwrap_or_else(|e| {
                eprintln!(
                    "Fatal error, can't read config from stdin: {}",
                    &e.to_string()
                );
                exit(1);
            });
        }

        self.load_config_from_string(&contents[..]);
    }

    fn load_config_from_string(&mut self, s: &str) {
        self.reset_server_save_params();
        for (i, line) in s.lines().enumerate() {
            let line = line.trim();
            let argv: Vec<&str>;
            let argc: usize;
            let main: String;

            // skip comments and blank lines
            if line.len() == 0 || line.as_bytes()[0] == b'#' {
                continue;
            }

            argv = line.split_ascii_whitespace().collect();
            argc = argv.len();

            if argv.is_empty() {
                continue;
            }

            main = argv[0].to_ascii_lowercase();

            match (&main[..], argc) {
                ("timeout", 2) => {
                    self.max_idle_time = parse_usize(argv[1]).unwrap_or_else(|e| {
                        Self::load_error(i, line, &e.to_string());
                        0
                    });
                }
                ("port", 2) => {
                    self.port = parse_port(argv[1]).unwrap_or_else(|e| {
                        Self::load_error(i, line, &e.to_string());
                        0
                    });
                }
                ("bind", 2) => {
                    self.bind_addr = argv[1].to_string();
                }
                ("save", 3) => {
                    let pair = parse_usize_pair(argv[1], argv[2]).unwrap_or_else(|e| {
                        Self::load_error(i, line, &e.to_string());
                        (0, 0)
                    });
                    self.save_params.push(pair);
                }
                ("dir", 2) => {
                    set_current_dir(argv[1]).unwrap_or_else(|e| {
                        eprint!("Can't change dir to {}: {}", argv[1], e);
                        exit(1);
                    });
                }
                ("loglevel", 2) => match argv[1] {
                    "debug" => self.log_level = LevelFilter::Debug,
                    "notice" => self.log_level = LevelFilter::Info,
                    "warning" => self.log_level = LevelFilter::Warn,
                    _ => Self::load_error(
                        i,
                        line,
                        "Invalid log level. Must be one of debug, notice, warning",
                    ),
                },
                ("logfile", 2) => {
                    self.log_file = Some(argv[1].to_string());
                    if argv[1].eq_ignore_ascii_case("stdout") {
                        self.log_file = None;
                    }
                    if let Some(f) = self.log_file.as_ref() {
                        let ok = File::open(f);
                        if let Err(e) = ok {
                            Self::load_error(i, line, &e.to_string());
                        }
                    }
                }
                ("databases", 2) => {
                    let num: i64 = argv[1].parse().unwrap_or(-1);
                    if num < 1 {
                        Self::load_error(i, line, "Invalid number of databases");
                    }
                    self.db_num = num as usize;
                }
                ("maxclients", 2) => {
                    self.max_clients = parse_usize(argv[1]).unwrap_or_else(|e| {
                        Self::load_error(i, line, &e.to_string());
                        0
                    });
                }
                ("maxmemory", 2) => {
                    let max_memory: usize = human_size(argv[1]).unwrap_or_else(|_| {
                        Self::load_error(i, line, "cannot parse size");
                        0
                    });
                    self.max_memory = max_memory;
                }
                ("slaveof", 3) => {
                    self.master_host = Some(argv[1].to_string());
                    self.master_port = parse_port(argv[2]).unwrap_or_else(|e| {
                        Self::load_error(i, line, &e.to_string());
                        0
                    });
                }
                ("glueoutputbuf", 2) => {
                    self.glue_output = yes_or_no(argv[1]).unwrap_or_else(|| {
                        Self::load_error(i, line, "must be 'yes' or 'no'");
                        false
                    });
                }
                ("daemonize", 2) => {
                    self.daemonize = yes_or_no(argv[1]).unwrap_or_else(|| {
                        Self::load_error(i, line, "must be 'yes' or 'no'");
                        false
                    });
                }
                ("requirepass", 2) => {
                    self.require_pass = Some(argv[1].to_string());
                }
                ("dbfilename", 2) => {
                    self.db_filename = argv[1].to_string();
                }
                (_, _) => {
                    println!(
                        "Warning: '{}' is not supported or argument number is incorrect",
                        main
                    );
                }
            }
        }
    }

    fn load_error(line_num: usize, line: &str, err: &str) {
        eprintln!("\n*** FATAL CONFIG FILE ERROR ***");
        eprintln!("Reading the configuration file, at line {}", line_num);
        eprintln!(">>> '{}'", line);
        eprintln!("{}", err);
        exit(1);
    }
}

pub struct Env {
    pub server: Server,
    pub el: AeEventLoop,
}

impl Env {
    pub fn new(config: &Config) -> Env {
        let server = Server::new(config);
        let el = AeEventLoop::new(512);
        Env { server, el }
    }

    pub fn reset_server_save_params(&mut self) {}

    pub fn load_server_config(&mut self, _filename: &str) {}

    pub fn daemonize(&mut self) {}

    pub fn init_server(&mut self) {
        self.el.create_time_event(
            Duration::from_millis(1000),
            server_cron,
            ClientData::Nil(),
            default_ae_event_finalizer_proc,
        );
    }

    pub fn rdb_load(&mut self) -> Result<(), ()> {
        if let Err(e) = rdb::rdb_load(&mut self.server) {
            if let ErrorKind::NotFound = e.kind() {
                return Err(());
            }
            warn!("{}", e);
            exit(1);
        }
        Ok(())
    }

    pub fn create_first_file_event(&mut self) -> Result<(), ()> {
        self.el.create_file_event(
            Rc::clone(&self.server.fd),
            AE_READABLE,
            accept_handler,
            ClientData::Nil(),
        )?;
        Ok(())
    }

    pub fn ae_main(&mut self) {
        self.el.main(&mut self.server);
    }
}

fn level_to_sign(level: Level) -> &'static str {
    match level {
        Level::Info => "-",
        Level::Warn => "*",
        Level::Debug => ".",
        _ => "-",
    }
}

pub fn init_logger(level: log::LevelFilter) {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(level);
    builder.format(|buf, record| {
        writeln!(
            buf,
            "{} {} {}",
            Local::now().format("%d %b %H:%M:%S"),
            level_to_sign(record.level()),
            record.args()
        )
    });

    builder.init();
}

pub fn accept_handler(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    _data: &ClientData,
    _mask: i32,
) {
    let fd = fd.borrow();
    let listener = fd.unwrap_listener();

    debug!("ready to accept");
    let r = listener.accept();

    let (stream, info) = match r {
        Ok(p) => p,
        Err(e) => {
            debug!("Accepting client connection: {}", e);
            return;
        }
    };
    debug!("Accepted {}:{}", info.ip(), info.port());

    let c = match Client::with_fd_and_el(Rc::new(RefCell::new(Fdp::Stream(stream))), el) {
        Err(()) => {
            warn!("Error allocation resources for the client");
            return;
        }
        Ok(e) => {
            server.clients.push_back(Rc::clone(&e));
            e
        }
    };

    if server.max_clients > 0 && server.clients.len() > server.max_clients {
        let c = c.as_ref().borrow_mut();
        let mut fd = c.fd.as_ref().borrow_mut();
        let w = fd.unwrap_stream_mut();
        // this is a best-effort message, so the result is ignored
        let _ = write!(w, "-ERR max number of clients reached");
        return;
    }
    server.stat_num_connections += 1;
}

fn free_client_occupied_in_el(
    server: &mut Server,
    el: &mut AeEventLoop,
    client_ptr: &Rc<RefCell<Client>>,
    stream: &TcpStream,
    flags: i32,
) {
    server.free_client_with_flags(&client_ptr, flags);
    el.async_delete_active_file_event();
    el.deregister_stream(stream);
}

fn free_active_client(
    server: &mut Server,
    el: &mut AeEventLoop,
    client: &Client,
    socket: &TcpStream,
) {
    server.free_client_by_ref(client);
    el.async_delete_active_file_event();
    el.deregister_stream(socket);
}

pub fn read_query_from_client(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    data: &ClientData,
    _mask: i32,
) {
    let client_ptr = Rc::clone(data.unwrap_client());
    let mut client = client_ptr.borrow_mut();
    let n_read;
    let curr_len;

    {
        let mut fd_ref = fd.borrow_mut();
        let socket = fd_ref.unwrap_stream_mut();

        let read_len = REREIDS_IO_BUF_LEN;

        curr_len = client.query_buf.len();
        client.query_buf.resize(curr_len + read_len, 0u8);

        n_read = match socket.read(&mut client.query_buf[curr_len..]) {
            Ok(n) => n,
            Err(e) => {
                if let ErrorKind::WouldBlock = e.kind() {
                } else {
                    debug!("Reading from client: {}", e);
                    free_active_client(server, el, client.deref(), socket);
                }
                return;
            }
        };

        if n_read == 0 {
            debug!("Reading from client: {}", "Client closed connection");
            free_active_client(server, el, client.deref(), socket);
            return;
        }
    }

    client.last_interaction = SystemTime::now();
    if client.flags & CLIENT_MASTER != 0 {
        client.reply_off += n_read;
    }

    // TODO: close client if query buffer's len exceed max value
    client.query_buf.resize(curr_len + n_read, 0);

    client.process_input_buffer(server, el);
    if !client.reply.is_empty() {
        if let Err(()) = client.prepare_to_write(el) {
            client.reply.clear();
        }
    }
}

pub fn send_reply_to_client(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    data: &ClientData,
    _mask: i32,
) {
    let mut client = data.unwrap_client().as_ref().borrow_mut();
    let mut written_bytes: usize = 0;
    let mut written_elem: usize = 0;
    if client.reply.is_empty() {
        return;
    }

    let mut fd_ref = fd.as_ref().borrow_mut();
    let stream = fd_ref.unwrap_stream_mut();

    for rep in client.reply.iter().map(|x| x.borrow()) {
        let (write_result, n) = match rep.encoding() {
            RobjEncoding::Int => {
                let s = rep.integer().to_string();
                (stream.write_all(s.as_bytes()), s.as_bytes().len())
            }
            _ => (stream.write_all(rep.string()), rep.string().len()),
        };
        match write_result {
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    debug!("Error writing to client: {}", e);
                    free_client_occupied_in_el(
                        server,
                        el,
                        data.unwrap_client(),
                        stream,
                        client.flags,
                    );
                    return;
                }
            }
            Ok(_) => written_bytes += n,
        }
        written_elem += 1;
        if written_bytes >= REREDIS_MAX_WRITE_PER_EVENT {
            break;
        }
    }

    client.last_interaction = SystemTime::now();
    if written_elem == client.reply.len() {
        el.async_reduce_active_file_event(AE_WRITABLE);
        client.reply.clear();
    } else {
        client.reply.drain(0..written_elem);
    }
}

pub fn send_bulk_to_slave(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    data: &ClientData,
    _mask: i32,
) {
    let mut buf: [u8; REREIDS_IO_BUF_LEN] = [0; REREIDS_IO_BUF_LEN];
    let client_ptr = data.unwrap_client();
    let mut slave = client_ptr.borrow_mut();
    let flags = slave.flags;
    let buf_len;
    {
        let mut fd_ref = fd.borrow_mut();
        let stream = fd_ref.unwrap_stream_mut();
        if slave.reply_db_off == 0 {
            let bulk_len = format!("${}\r\n", slave.reply_db_size);
            if let Err(_) = stream.write_all(bulk_len.as_bytes()) {
                free_client_occupied_in_el(server, el, client_ptr, stream, flags);
                return;
            }
        }

        let file = slave.reply_db_file.as_mut().unwrap();
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            warn!("Seek Error sending DB to slave: {}", e);
            free_client_occupied_in_el(server, el, client_ptr, stream, flags);
        }
        buf_len = match file.read(&mut buf) {
            Ok(0) => {
                warn!("Read error sending DB to slave: {}", "premature EOF");
                free_client_occupied_in_el(server, el, client_ptr, stream, flags);
                return;
            }
            Err(e) => {
                warn!("Read error sending DB to slave: {}", e);
                free_client_occupied_in_el(server, el, client_ptr, stream, flags);
                return;
            }
            Ok(size) => size,
        };
        if let Err(e) = stream.write_all(&buf[..buf_len]) {
            warn!("Write error sending DB to slave: {}", e);
            free_client_occupied_in_el(server, el, client_ptr, stream, flags);
            return;
        }
    }

    slave.reply_db_off += buf_len as u64;
    if slave.reply_db_off == slave.reply_db_size {
        let _ = slave.reply_db_file.take();
        slave.reply_state = ReplyState::Online;
        el.async_modify_active_file_event(AE_WRITABLE, send_reply_to_client);
        slave.add_str_reply("");
        info!("Synchronization with slave succeeded");
    }
}

pub fn server_cron(server: &mut Server, el: &mut AeEventLoop, _id: i64, _data: &ClientData) -> i32 {
    server.cron_loops += 1;

    // update global state with the amount of used memory
    server.used_memory = zalloc::allocated_memory();

    let loops = server.cron_loops;

    // show some info about non-empty databases
    for (i, db) in server.db.iter().enumerate() {
        let slot: usize = db.dict.slot();
        let used: usize = db.dict.len();
        let vkeys: usize = db.expires.len();

        if loops % 5 == 0 && (used != 0 || vkeys != 0) {
            debug!(
                "DB {}: {} keys ({} volatile) in {} slots HT.",
                i, used, vkeys, slot
            );
        }
    }

    // show information about connected clients
    if loops % 5 == 0 {
        debug!(
            "{} clients connected, {} bytes in use",
            server.clients.len(),
            server.used_memory
        );
    }

    // close connections of timeout clients
    if server.max_idle_time > 0 && loops % 10 == 0 {
        server.close_timeout_clients(el);
    }

    // check if a background saving in progress terminated
    if server.bg_save_in_progress {
        let wait_flag = Some(WaitPidFlag::WNOHANG);
        let r = waitpid(Pid::from_raw(-1), wait_flag);
        if let Ok(stat) = r {
            match stat {
                WaitStatus::Exited(_, exitcode) => {
                    let exit_ok: bool;
                    if exitcode == 0 {
                        info!("Background saving terminated with success");
                        server.dirty = 0;
                        server.last_save = SystemTime::now();
                        exit_ok = true;
                    } else {
                        warn!("Background saving error");
                        exit_ok = false;
                    }
                    server.bg_save_in_progress = false;
                    server.bg_save_child_pid = -1;
                    replicate::update_slaves_waiting_bgsave(server, el, exit_ok);
                }
                WaitStatus::StillAlive => {}
                _ => {
                    warn!("Background saving terminated by signal");
                    server.bg_save_in_progress = false;
                    server.bg_save_child_pid = -1;
                    replicate::update_slaves_waiting_bgsave(server, el, false);
                }
            }
        }
    } else {
        let now = SystemTime::now();
        for (seconds, changes) in server.save_params.iter() {
            if server.dirty >= *changes
                && now.duration_since(server.last_save).unwrap().as_secs() as usize > *seconds
            {
                let _ = rdb::rdb_save_in_background(server);
                break;
            }
        }
    }

    // try to expire a few timeout keys
    for db in server.db.iter_mut() {
        let mut num: usize = db.expires.len();

        if num > 0 {
            let now: SystemTime = SystemTime::now();

            if num > REREDIS_EXPIRE_LOOKUPS_PER_CRON {
                num = REREDIS_EXPIRE_LOOKUPS_PER_CRON;
            }

            for _ in 0..num {
                let (key, t) = db.expires.random_key_value();
                if *t < now {
                    let key = Rc::clone(key);
                    let _ = db.delete_key(&key);
                }
            }
        }
    }

    if let ReplyState::Connect = server.reply_state {
        info!("Connecting to MASTER...");
        if let Ok(_) = server.sync_with_master(el) {
            info!("MASTER <-> SLAVE sync succeeded");
        }
    }

    server.free_clients_in_async_free_queue(el);

    1000
}
