use crate::ae::*;
use std::time::SystemTime;
use std::rc::Rc;
use crate::env::read_query_from_client;
use std::cell::RefCell;
use crate::object::{Sds, RobjPtr};
use std::error::Error;
use std::fmt::Display;

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
            Rc::clone(&client.borrow().fd),
            AE_READABLE,
            read_query_from_client,
            ClientData::Client(Rc::clone(&client)),
            default_ae_event_finalizer_proc,
        )?;

        Ok(client)
    }

    pub fn parse_query_buf(&mut self) -> Result<(), CommandError> {
        Ok(())
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
}

impl CommandError {
    pub fn description(&self) -> &'static str {
        match self {
            CommandError::Malformed => "Client protocol error",
            _ => "",
        }
    }
}
