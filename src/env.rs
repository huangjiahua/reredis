use crate::server::Server;
use std::borrow::{BorrowMut, Borrow};
use std::error::Error;
use crate::ae::{AE_READABLE, default_ae_event_finalizer_proc, AeEventLoop, Fd, ClientData, Fdp};
use std::rc::Rc;
use chrono::Local;
use std::io::Write;
use log::{LevelFilter, Level};
use std::cell::RefCell;
use crate::client::Client;

pub const REREDIS_VERSION: &str = "0.0.1";

pub struct Env {
    pub server: Server,
    pub el: AeEventLoop,
}

impl Env {
    pub fn new() -> Env {
        let server = Server::new();
        let el = AeEventLoop::new(512);
        Env {
            server,
            el,
        }
    }

    pub fn reset_server_save_params(&mut self) {}

    pub fn load_server_config(&mut self, filename: &str) {}

    pub fn daemonize(&mut self) {}

    pub fn init_server(&mut self) {
//        unimplemented!()
    }

    pub fn rdb_load(&mut self) -> Result<(), Box<dyn Error>> {
//        unimplemented!()
        Ok(())
    }

    pub fn create_first_file_event(&mut self) -> Result<(), Box<dyn Error>> {
        self.el.create_file_event(
            Rc::clone(&self.server.fd),
            AE_READABLE,
            accept_handler,
            ClientData::Nil(),
            default_ae_event_finalizer_proc,
        );
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
    data: &ClientData,
    mask: i32,
) {
    let fd = fd.as_ref().borrow();
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

    let mut c = match Client::with_fd(
        Rc::new(RefCell::new(Fdp::Stream(stream))
        ), el) {
        Err(()) => {
            warn!("Error allocation resources for the client");
            return;
        }
        Ok(e) => e,
    };

    if server.max_clients > 0 && server.clients.len() > server.max_clients {
        let c = c.as_ref().borrow_mut();
        let mut fd = c.fd.as_ref().borrow_mut();
        let w = fd.unwrap_stream_mut();
        write!(w, "-ERR max number of clients reached");
        return;
    }
    server.stat_num_connections += 1;
}

pub fn read_query_from_client(
    server: &mut Server,
    el: &mut AeEventLoop,
    fd: &Fd,
    data: &ClientData,
    mask: i32) {
    unimplemented!()
}