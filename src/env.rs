use crate::server::{Server, accept_handler};
use std::borrow::BorrowMut;
use std::error::Error;
use crate::ae::{AE_READABLE, default_ae_event_finalizer_proc};
use std::rc::Rc;
use chrono::Local;
use std::io::Write;
use log::{LevelFilter, Level};

pub const REREDIS_VERSION: &str = "0.0.1";

pub struct Env {
    pub server: Server,
}

impl Env {
    pub fn new() -> Env {
        let server = Server::new();
        Env {
            server,
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
        self.server.el.create_file_event(
            Rc::clone(&self.server.fd),
            AE_READABLE,
            accept_handler,
            Box::new(0i32),
            default_ae_event_finalizer_proc,
        );
        Ok(())
    }

    pub fn ae_main(&mut self) {
        self.server.el.main();
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