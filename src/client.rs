use crate::ae::*;
use std::time::SystemTime;
use std::rc::Rc;
use crate::env::read_query_from_client;
use std::cell::RefCell;
use crate::object::{Sds, RobjPtr};
use crate::protocol;
use std::error::Error;
use std::fmt::Display;
use crate::server::Server;
use std::borrow::Borrow;
use mio::net::TcpStream;

pub struct Client {
    pub fd: Fd,
    pub dict_id: usize,
    pub query_buf: Vec<u8>,
    pub last_interaction: SystemTime,
    pub bulk_len: Option<usize>,
    pub argv: Vec<RobjPtr>,
}

impl Client {
    pub fn with_fd(fd: Fd, el: &mut AeEventLoop) -> Result<Rc<RefCell<Client>>, ()> {
        let client = Rc::new(RefCell::new(Client {
            fd,
            dict_id: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
            bulk_len: None,
            argv: vec![],
        }));
        el.create_file_event(
            Rc::clone(&client.as_ref().borrow().fd),
            AE_READABLE,
            read_query_from_client,
            ClientData::Client(Rc::clone(&client)),
            default_ae_event_finalizer_proc,
        )?;

        Ok(client)
    }

    pub fn parse_query_buf(&mut self) -> Result<(), CommandError> {
        assert!(self.argv.is_empty());
        let iter = match protocol::decode(&self.query_buf) {
            Err(_) => return Err(CommandError::Malformed),
            Ok(i) => i,
        };

        for obj in iter {
            let obj = match obj {
                Err(_) => return Err(CommandError::Malformed),
                Ok(o) => o,
            };
            self.argv.push(obj);
        }

        self.query_buf.clear();
        Ok(())
    }

    pub fn process_command(
        &mut self,
        stream: &mut TcpStream,
        server: &mut Server,
        el: &mut AeEventLoop,
    ) -> Result<(), CommandError> {
        assert!(!self.argv.is_empty());
        // TODO: free memory if needed

        let main_cmd_obj = self.argv[0].as_ref().borrow();
        let main_cmd = main_cmd_obj.string();
        if case_eq(main_cmd, "quit") {
            return Err(CommandError::Quit)
        }


        Ok(())
    }

    pub fn argc(&self) -> usize {
        self.argv.len()
    }
}

pub enum ClientData {
    Client(Rc<RefCell<Client>>),
    Nil(),
}

impl ClientData {
    pub fn unwrap_client(&self) -> &Rc<RefCell<Client>> {
        match self {
            ClientData::Client(c) => c,
            _ => panic!("not a client"),
        }
    }

    pub fn is_client(&self) -> bool {
        match self {
            ClientData::Client(_) => true,
            _ => false,
        }
    }
}

pub enum CommandError {
    Malformed,
    Quit,
}

impl CommandError {
    pub fn description(&self) -> &'static str {
        match self {
            CommandError::Malformed => "Client protocol error",
            CommandError::Quit => "Client quit",
            _ => "",
        }
    }
}

fn case_eq(lhs: &str, rhs: &str) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    let r = rhs.as_bytes();
    for p in lhs.as_bytes()
        .iter()
        .map(|x| x.to_ascii_lowercase())
        .zip(r.iter()) {
        if p.0 != *p.1 {
            return false;
        }
    }
    true
}
