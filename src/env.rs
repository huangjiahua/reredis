use crate::server::Server;
use std::error::Error;
use crate::ae::{AE_READABLE, default_ae_event_finalizer_proc, AeEventLoop, Fd, Fdp, default_ae_file_proc};
use std::rc::Rc;
use chrono::Local;
use std::io::{Write, Read, ErrorKind};
use log::{Level, LevelFilter};
use std::cell::RefCell;
use crate::client::*;
use mio::net::TcpStream;
use std::time::{Duration, SystemTime};
use std::fs::File;
use std::process::exit;
use std::io;
use crate::util::*;
use std::env::set_current_dir;
use std::fmt;
use crate::zalloc;

pub const REREDIS_VERSION: &str = "0.0.1";
pub const REREDIS_REQUEST_MAX_SIZE: usize = 1024 * 1024 * 256;
pub const REREDIS_MAX_WRITE_PER_EVENT: usize = 64 * 1024;
pub const REREDIS_EXPIRE_LOOKUPS_PER_CRON: usize = 100;

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

    pub fn reset_server_save_params(&mut self) {}

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
                eprintln!("Fatal error, can't open config file {}: {}",
                          filename, e.description());
                exit(1);
            });

            file.read_to_string(&mut contents).unwrap_or_else(|e| {
                eprintln!("Fatal error, can't read config file {}: {}",
                          filename, e.description());
                exit(1);
            });
        } else {
            let stdin = io::stdin();
            let mut handle = stdin.lock();

            handle.read_to_string(&mut contents).unwrap_or_else(|e| {
                eprintln!("Fatal error, can't read config from stdin: {}",
                          e.description());
                exit(1);
            });
        }

        self.load_config_from_string(&contents[..]);
    }

    fn load_config_from_string(&mut self, s: &str) {
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
                        Self::load_error(i, line, e.description());
                        0
                    });
                }
                ("port", 2) => {
                    self.port = parse_port(argv[1]).unwrap_or_else(|e| {
                        Self::load_error(i, line, e.description());
                        0
                    });
                }
                ("bind", 2) => {
                    self.bind_addr = argv[1].to_string();
                }
                ("save", 3) => {
                    let pair =
                        parse_usize_pair(argv[1], argv[2]).unwrap_or_else(|e| {
                            Self::load_error(i, line, e.description());
                            (0, 0)
                        });
                    self.save_params.push(pair);
                }
                ("dir", 2) => {
                    set_current_dir(argv[1]).unwrap_or_else(|e| {
                        eprint!("Can't change dir to {}: {}", argv[1], e.description());
                        exit(1);
                    });
                }
                ("loglevel", 2) => {
                    match argv[1] {
                        "debug" => self.log_level = LevelFilter::Debug,
                        "notice" => self.log_level = LevelFilter::Info,
                        "warning" => self.log_level = LevelFilter::Warn,
                        _ => Self::load_error(
                            i, line,
                            "Invalid log level. Must be one of debug, notice, warning",
                        ),
                    }
                }
                ("logfile", 2) => {
                    self.log_file = Some(argv[1].to_string());
                    if argv[1].eq_ignore_ascii_case("stdout") {
                        self.log_file = None;
                    }
                    if let Some(f) = self.log_file.as_ref() {
                        let ok = File::open(f);
                        if let Err(e) = ok {
                            Self::load_error(i, line, e.description());
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
                        Self::load_error(i, line, e.description());
                        0
                    });
                }
                ("maxmemory", 2) => {
                    let max_memory: usize =
                        human_size(argv[1]).unwrap_or_else(|_| {
                            Self::load_error(i, line, "cannot parse size");
                            0
                        });
                    self.max_memory = max_memory;
                }
                ("slaveof", 3) => {
                    self.master_host = Some(argv[1].to_string());
                    self.master_port = parse_port(argv[2]).unwrap_or_else(|e| {
                        Self::load_error(i, line, e.description());
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
                    println!("Warning: '{}' is not supported or argument number is incorrect",
                             main);
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
        Env {
            server,
            el,
        }
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

    pub fn rdb_load(&mut self) -> Result<(), Box<dyn Error>> {
//        unimplemented!()
        Ok(())
    }

    pub fn create_first_file_event(&mut self) -> Result<(), ()> {
        self.el.create_file_event(
            Rc::clone(&self.server.fd),
            AE_READABLE,
            accept_handler,
            default_ae_file_proc,
            ClientData::Nil(),
            default_ae_event_finalizer_proc,
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
    builder.format(
        |buf, record|
            writeln!(
                buf,
                "{} {} {}",
                Local::now().format("%d %b %H:%M:%S"),
                level_to_sign(record.level()),
                record.args()
            )
    );

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
            debug!("Accepting client connection: {}", e.description());
            return;
        }
    };
    debug!("Accepted {}:{}", info.ip(), info.port());

    let c = match Client::with_fd(
        Rc::new(RefCell::new(Fdp::Stream(stream))
        ), el) {
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
) {
    server.free_client(&client_ptr);
    el.try_delete_occupied();
    el.deregister_stream(stream);
}

pub fn read_query_from_client(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    data: &ClientData,
    _mask: i32
) {
    let client_ptr = Rc::clone(data.unwrap_client());
    let mut fd_ref = fd.as_ref().borrow_mut();
    let stream = fd_ref.unwrap_stream_mut();
    let mut buf = [0u8; 1024];

    let r = stream.read(&mut buf);
    let nread: usize = match r {
        Err(err) => {
            match err.kind() {
                ErrorKind::Interrupted => 0,
                _ => {
                    debug!("Reading from client: {}", err.description());
                    free_client_occupied_in_el(server, el, &client_ptr, stream);
                    return;
                }
            }
        }
        Ok(n) => {
            match n {
                0 => {
                    debug!("Client closed connection");
                    free_client_occupied_in_el(server, el, &client_ptr, stream);
                    return;
                }
                _ => n,
            }
        }
    };

    if nread > 0 {
        client_ptr.as_ref()
            .borrow_mut()
            .query_buf
            .extend_from_slice(&buf[..nread]);
    } else {
        return;
    }

    let mut client = client_ptr.as_ref().borrow_mut();

    loop {
        if let Some(_bulk_len) = client.bulk_len {
            unimplemented!()
        } else {
            let p = client.query_buf
                .iter()
                .enumerate()
                .find(|x| *x.1 == '\n' as u8)
                .map(|x| *x.1);

            if let Some(_) = p {
                if let Err(e) = client.parse_query_buf() {
                    debug!("{}", e.description());
                    free_client_occupied_in_el(server, el, &client_ptr, stream);
                    return;
                }
                if client.argc() > 0 {
                    if let Err(e) = client.process_command(server, el) {
                        debug!("{}", e.description());
                        match e {
                            CommandError::Quit =>
                                free_client_occupied_in_el(server, el, &client_ptr, stream),
                            CommandError::Close =>
                                free_client_occupied_in_el(server, el, &client_ptr, stream),
                            _ => {}
                        }
                        return;
                    }
                    if client.query_buf.is_empty() {
                        return;
                    }
                }
            } else if client.query_buf.len() > REREDIS_REQUEST_MAX_SIZE {
                debug!("Client protocol error");
                free_client_occupied_in_el(server, el, &client_ptr, stream);
                return;
            }
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

    for rep in client.reply
        .iter()
        .map(|x| x.as_ref()) {
        match stream.write(rep.borrow().string()) {
            Err(e) => if e.kind() != ErrorKind::Interrupted {
                debug!("Error writing to client: {}", e.description());
                free_client_occupied_in_el(server, el, data.unwrap_client(), stream);
                return;
            }
            Ok(n) => written_bytes += n,
        }
        written_elem += 1;
        if written_bytes >= REREDIS_MAX_WRITE_PER_EVENT {
            break;
        }
    }

    client.last_interaction = SystemTime::now();
    if written_elem == client.reply.len() {
        client.reply.clear();
    } else {
        client.reply.drain(0..written_elem);
    }
}

pub fn server_cron(
    server: &mut Server,
    el: &mut AeEventLoop,
    _id: i64,
    _data: &ClientData,
) -> i32 {
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
            debug!("DB {}: {} keys ({} volatile) in {} slots HT.", i, used, vkeys, slot);
        }
    }

    // show information about connected clients
    if loops % 5 == 0 {
        debug!("{} clients connected, {} bytes in use",
               server.clients.len(), server.used_memory);
    }

    // close connections of timeout clients
    if server.max_idle_time > 0 && loops % 10 == 0 {
        server.close_timeout_clients(el);
    }

    // check if a background saving in progress terminated
    if server.bg_save_in_progress {
        // TODO: check if bg_save finish
    } else {
        // TODO: bg_save
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
    // TODO: check if we should connect to master

    1000
}
