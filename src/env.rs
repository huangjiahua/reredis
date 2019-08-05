use crate::server::Server;
use crate::log::*;
use std::borrow::BorrowMut;
use std::error::Error;

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
        Ok(())
    }

    pub fn ae_main(&mut self) {}

    pub fn log(&mut self, level: LogLevel, s: &str) {
        match self.server.log_file.borrow_mut() {
            None => write_log(level, self.server.verbosity, &mut std::io::stdout(), s),
            Some(f) => write_log(level, self.server.verbosity, f, s),
        }
    }
}