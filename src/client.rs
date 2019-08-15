use crate::ae::*;
use std::time::SystemTime;
use std::rc::Rc;
use crate::env::{read_query_from_client, send_reply_to_client};
use std::cell::RefCell;
use crate::object::{Sds, RobjPtr, Robj};
use crate::protocol;
use std::error::Error;
use std::fmt::Display;
use crate::server::Server;
use std::borrow::Borrow;
use mio::net::TcpStream;
use crate::command::{lookup_command, CMD_BULK};

const CLIENT_CLOSE: i32 = 0b0001;
const CLIENT_SLAVE: i32 = 0b0010;
const CLIENT_MASTER: i32 = 0b0100;
const CLIENT_MONITOR: i32 = 0b1000;

#[derive(Copy, Clone, PartialEq)]
pub enum ReplyState {
    None,
    Connect,
    Connected,
    WaitBgSaveStart,
    WaitBgSaveEnd,
    SendBulk,
    Online,
}

pub struct Client {
    pub fd: Fd,
    pub dict_id: usize,
    pub flags: i32,
    pub query_buf: Vec<u8>,
    pub last_interaction: SystemTime,
    pub bulk_len: Option<usize>,
    pub argv: Vec<RobjPtr>,

    pub reply_state: ReplyState,
    pub reply: Vec<RobjPtr>,

    pub db_idx: usize,
}

impl Client {
    pub fn with_fd(fd: Fd, el: &mut AeEventLoop) -> Result<Rc<RefCell<Client>>, ()> {
        let client = Rc::new(RefCell::new(Client {
            fd,
            dict_id: 0,
            flags: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
            bulk_len: None,
            argv: vec![],
            reply_state: ReplyState::None,
            reply: vec![],
            db_idx: 0,
        }));
        el.create_file_event(
            Rc::clone(&client.as_ref().borrow().fd),
            AE_READABLE | AE_WRITABLE,
            read_query_from_client,
            send_reply_to_client,
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
        server: &mut Server,
        el: &mut AeEventLoop,
    ) -> Result<(), CommandError> {
        assert!(!self.argv.is_empty());
        // TODO: free memory if needed

        if case_eq(self.argv[0].as_ref().borrow().string(), "quit") {
            return Err(CommandError::Quit);
        }

        let cmd = lookup_command(
            self.argv[0].as_ref().borrow().string(),
        );
        let cmd = match cmd {
            None => {
                self.add_str_reply("-Error unknown command\r\n");
                self.reset();
                return Err(CommandError::Unknown);
            }
            Some(c) => c,
        };

        if (cmd.arity > 0 && cmd.arity as usize != self.argc())
            || (cmd.arity < 0 && (self.argc() < (-cmd.arity) as usize)) {
            self.add_str_reply("-Error wrong number of arguments\r\n");
            self.reset();
            return Err(CommandError::WrongNumber);
            // TODO: max memory
        } else if cmd.flags & CMD_BULK != 0 && self.bulk_len.is_none() {
            // TODO: figure out what bulk really mean
            // for now, I use the latest version of redis protocol
            // it seems that the all commands are bulk
        }

        // TODO: share objects to save memory

        // TODO: authenticate

        // TODO: save server dirty bit and tackle problems with slave server ans monitors
        cmd.proc.borrow()(self, server, el);

        server.stat_num_commands += 1;

        if self.flags & CLIENT_CLOSE != 0 {
            return Err(CommandError::Close);
        }
        self.reset();
        Ok(())
    }

    pub fn argc(&self) -> usize {
        self.argv.len()
    }

    pub fn reset(&mut self) {
        self.argv.clear();
        self.bulk_len = None;
    }

    pub fn add_reply(&mut self, r: RobjPtr) {
        self.reply.push(r);
    }

    pub fn add_str_reply(&mut self, s: &str) {
        self.add_reply(
            Robj::create_string_object(s),
        );
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
    Close,
    Unknown,
    WrongNumber,
}

impl CommandError {
    pub fn description(&self) -> &'static str {
        match self {
            CommandError::Malformed => "Client protocol error",
            CommandError::Quit => "Client quit",
            CommandError::Close => "Client close",
            CommandError::Unknown => "Unknown command",
            CommandError::WrongNumber => "Wrong number of arguments",
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
