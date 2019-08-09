use crate::server::Server;
use std::borrow::{BorrowMut, Borrow};
use std::error::Error;
use crate::ae::{AE_READABLE, default_ae_event_finalizer_proc, AeEventLoop, Fd, Fdp};
use std::rc::Rc;
use chrono::Local;
use std::io::{Write, Read, ErrorKind};
use log::{LevelFilter, Level};
use std::cell::RefCell;
use crate::client::*;
use mio::net::TcpStream;
use std::time::Duration;

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
    mask: i32) {
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
        let _ = stream.write(&buf[..nread]); // for debug only
    } else {
        return;
    }
}

pub fn server_cron(
    server: &mut Server,
    el: &mut AeEventLoop,
    id: i64,
    data: &ClientData,
) -> i32 {
    debug!("Executing server_cron");
    1000
}