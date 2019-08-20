use crate::server::Server;
use std::error::Error;
use crate::ae::{AE_READABLE, default_ae_event_finalizer_proc, AeEventLoop, Fd, Fdp, default_ae_file_proc, AE_WRITABLE};
use std::rc::Rc;
use chrono::Local;
use std::io::{Write, Read, ErrorKind};
use log::{LevelFilter, Level};
use std::cell::RefCell;
use crate::client::*;
use mio::net::TcpStream;
use std::time::{Duration, SystemTime};

pub const REREDIS_VERSION: &str = "0.0.1";
pub const REREDIS_REQUEST_MAX_SIZE: usize = 1024 * 1024 * 256;
pub const REREDIS_MAX_WRITE_PER_EVENT: usize = 64 * 1024;

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
    data: &ClientData,
    mask: i32,
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

    let mut c = match Client::with_fd(
        Rc::new(RefCell::new(Fdp::Stream(stream))
        ), el) {
        Err(()) => {
            warn!("Error allocation resources for the client");
            return;
        }
        Ok(e) => {
            server.clients.push(Rc::clone(&e));
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
        // TODO: delete this
        debug!("Received: {}", std::str::from_utf8(&buf[..nread]).unwrap());
    } else {
        return;
    }

    let mut client = client_ptr.as_ref().borrow_mut();

    loop {
        if let Some(bulk_len) = client.bulk_len {
            unimplemented!()
        } else {
            let p = client.query_buf
                .iter()
                .enumerate()
                .find(|x| *x.1 == '\n' as u8)
                .map(|x| *x.1);

            if let Some(p) = p {
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
    mask: i32,
) {
    let mut client = data.unwrap_client().as_ref().borrow_mut();
    let mut written_bytes: usize = 0;
    let mut written_elem: usize = 0;
    if client.reply.is_empty() {
        return;
    }

    let mut fd_ref = fd.as_ref().borrow_mut();
    let stream = fd_ref.unwrap_stream_mut();
    debug!("ready to reply");

    for rep in client.reply
        .iter()
        .map(|x| x.as_ref()) {
        match stream.write(rep.borrow().string().as_bytes()) {
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
    id: i64,
    data: &ClientData,
) -> i32 {
    1000
}