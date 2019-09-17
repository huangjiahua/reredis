use crate::ae::*;
use std::time::SystemTime;
use std::rc::Rc;
use crate::env::{read_query_from_client, send_reply_to_client};
use std::cell::RefCell;
use crate::object::{RobjPtr, Robj};
use crate::protocol;
use crate::server::Server;
use crate::command::{lookup_command, CMD_DENY_OOM};
use crate::util::*;
use crate::zalloc;
use crate::replicate;

pub const CLIENT_CLOSE: i32 = 0b0001;
pub const CLIENT_SLAVE: i32 = 0b0010;
pub const CLIENT_MASTER: i32 = 0b0100;
pub const CLIENT_MONITOR: i32 = 0b1000;

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
    pub flags: i32,
    pub query_buf: Vec<u8>,
    pub last_interaction: SystemTime,
    pub bulk_len: Option<usize>,
    pub argv: Vec<RobjPtr>,

    pub authenticate: bool,
    pub reply_state: ReplyState,
    pub reply: Vec<RobjPtr>,

    pub db_idx: usize,

    pub slave_select_db: usize,
}

impl Client {
    pub fn with_fd_and_el(fd: Fd, el: &mut AeEventLoop) -> Result<Rc<RefCell<Client>>, ()> {
        let client = Rc::new(RefCell::new(Client {
            fd,
            flags: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
            bulk_len: None,
            argv: vec![],
            authenticate: false,
            reply_state: ReplyState::None,
            reply: vec![],
            db_idx: 0,
            slave_select_db: 0,
        }));
        el.create_file_event(
            Rc::clone(&client.borrow().fd),
            AE_READABLE | AE_WRITABLE,
            read_query_from_client,
            send_reply_to_client,
            ClientData::Client(Rc::clone(&client)),
            default_ae_event_finalizer_proc,
        )?;

        Ok(client)
    }

    pub fn with_fd(fd: Fd) -> Rc<RefCell<Client>> {
        let client = Rc::new(RefCell::new(Client {
            fd,
            flags: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
            bulk_len: None,
            argv: vec![],
            authenticate: false,
            reply_state: ReplyState::None,
            reply: vec![],
            db_idx: 0,
            slave_select_db: 0,
        }));
        client
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

        if server.max_memory > 0 {
            server.free_memory_if_needed()
        }

        if case_eq(self.argv[0].borrow().string(), "quit".as_bytes()) {
            return Err(CommandError::Quit);
        }

        let cmd = lookup_command(
            self.argv[0].borrow().string(),
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
        } else if server.max_memory > 0 &&
            cmd.flags & CMD_DENY_OOM != 0 &&
            zalloc::allocated_memory() > server.max_memory {
            self.add_str_reply("-ERR command not allowed when used memory > 'maxmemory'\r\n");
            self.reset();
            return Err(CommandError::OOM);
        }

        // TODO: share objects to save memory

        if server.require_pass.is_some() && !self.authenticate && cmd.name != "auth" {
            self.add_str_reply("-ERR operation not permitted\r\n");
            self.reset();
            return Err(CommandError::NotPermitted);
        }

        // TODO: save server dirty bit and tackle problems with slave server ans monitors
        (&cmd.proc)(self, server, el);
        if !server.monitors.is_empty() {
            replicate::feed_slaves(self, &server.monitors, self.db_idx);
        }

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

    pub fn add_reply_from_string(&mut self, s: String) {
        self.add_reply(
            Robj::from_bytes(s.into_bytes())
        )
    }

    pub fn glue_reply(&mut self) {
        let mut glued: Vec<u8> = vec![];
        for obj in self.reply.iter() {
            glued.extend_from_slice(obj.borrow().string())
        }
        self.reply.clear();
        self.reply.push(Robj::from_bytes(glued));
    }
}

pub enum ClientData {
    Client(Rc<RefCell<Client>>),
    Nil(),
}

impl ClientData {
    pub fn unwrap_client(&self) -> &Rc<RefCell<Client>> {
        match self {
            Self::Client(c) => c,
            _ => panic!("not a client"),
        }
    }

    pub fn is_client(&self) -> bool {
        match self {
            Self::Client(_) => true,
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
    OOM,
    NotPermitted,
}

impl CommandError {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Malformed => "Client protocol error",
            Self::Quit => "Client quit",
            Self::Close => "Client close",
            Self::Unknown => "Unknown command",
            Self::WrongNumber => "Wrong number of arguments",
            Self::OOM => "Out of memory",
            Self::NotPermitted => "Client's action is not permitted",
        }
    }
}
