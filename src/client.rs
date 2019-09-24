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
use std::fs::File;

pub const CLIENT_CLOSE: i32 = 0b0001;
pub const CLIENT_SLAVE: i32 = 0b0010;
pub const CLIENT_MASTER: i32 = 0b0100;
pub const CLIENT_MONITOR: i32 = 0b1000;

pub const CLIENT_CLOSE_ASAP: i32 = 0b1_0000;

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

#[derive(Copy, Clone, PartialEq)]
pub enum RequestType {
    Unknown,
    MultiBulk,
    Inline,
}

enum ProcessQueryError {
    Protocol(usize),
    NotEnough,
}

pub struct Client {
    pub fd: Fd,
    pub flags: i32,
    pub query_buf: Vec<u8>,
    pub last_interaction: SystemTime,
    pub argv: Vec<RobjPtr>,

    pub authenticate: bool,
    pub reply_state: ReplyState,
    pub reply: Vec<RobjPtr>,
    pub reply_db_file: Option<File>,
    pub reply_db_off: u64,
    pub reply_db_size: u64,
    pub reply_off: usize,

    pub request_type: RequestType,
    pub multi_bulk_len: usize,
    pub bulk_len: Option<usize>,

    pub db_idx: usize,

    pub slave_select_db: usize,
}

impl Client {
    pub fn with_fd_and_el(fd: Fd, el: &mut AeEventLoop) -> Result<Rc<RefCell<Client>>, ()> {
        let client = Rc::new(RefCell::new(
            Self::new_default_client(fd)
        ));
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
        let client = Rc::new(RefCell::new(
            Self::new_default_client(fd)
        ));
        client
    }

    fn new_default_client(fd: Fd) -> Client {
        Client {
            fd,
            flags: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
            argv: vec![],
            authenticate: false,
            reply_state: ReplyState::None,
            reply: vec![],
            reply_db_file: None,
            reply_db_off: 0,
            reply_db_size: 0,
            reply_off: 0,

            request_type: RequestType::Unknown,
            multi_bulk_len: 0,
            bulk_len: None,

            db_idx: 0,
            slave_select_db: 0,
        }
    }

    pub fn parse_query_buf(&mut self) -> Result<(), CommandError> {
        assert!(self.argv.is_empty());

        // parse inline
        if self.query_buf[0] != b'*' {
            self.parse_inline_query()?;
            self.query_buf.clear();
            return Ok(());
        }

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

    fn parse_inline_query(&mut self) -> Result<(), CommandError> {
        let command = match std::str::from_utf8(&self.query_buf) {
            Ok(s) => s,
            Err(_) => return Err(CommandError::Malformed),
        };
        for k in command.split_ascii_whitespace() {
            self.argv.push(Robj::create_string_object(k));
        }
        Ok(())
    }

    pub fn process_input_buffer(&mut self, server: &mut Server, el: &mut AeEventLoop) {
        while !self.query_buf.is_empty() {
            if let RequestType::Unknown = self.request_type {
                if self.query_buf[0] == b'*' {
                    self.request_type = RequestType::MultiBulk;
                } else {
                    self.request_type = RequestType::Inline;
                }
            }

            let result = match self.request_type {
                RequestType::Inline => {
                    self.process_inline_buffer()
                }
                RequestType::MultiBulk => {
                    self.process_multi_bulk_buffer()
                }
                _ => {
                    unreachable!();
                }
            };

            if let Err(e) = result {
                if let ProcessQueryError::Protocol(_) = e {
                    server.async_free_client(self);
                }
                break;
            }

            if self.argc() == 0 {
                self.reset();
            } else {
                if let Ok(_) = self.process_command(server, el) {
                    self.reset()
                }
            }
        }
    }

    fn process_inline_buffer(&mut self) -> Result<(), ProcessQueryError> {
        let mut pos: usize;
        let new_line = self.query_buf.iter()
            .enumerate()
            .find(|(_, ch)| **ch == b'\n')
            .map(|x| x.0);

        pos = new_line.ok_or(ProcessQueryError::NotEnough)?;
        // TODO: max inline buffer

        if pos > 0 && self.query_buf[pos - 1] == b'\r' {
            pos -= 1;
        }

        let s = std::str::from_utf8(&self.query_buf[0..pos]);

        let s = match s {
            Ok(s) => s,
            Err(_) => {
                self.add_str_reply("-ERR Protocol Error: Unknown char\r\n");
                return Err(ProcessQueryError::Protocol(0));
            }
        };

        for arg in s.split_ascii_whitespace() {
            self.argv.push(
                Robj::create_string_object(arg)
            );
        }

        if self.argv.is_empty() {
            self.add_str_reply("-ERR Protocol error: unbalanced quotes in request\r\n");
            return Err(ProcessQueryError::Protocol(0));
        }

        self.query_buf.drain(0..pos + 2);

        Ok(())
    }

    fn process_multi_bulk_buffer(&mut self) -> Result<(), ProcessQueryError> {
        let mut new_line;
        let mut pos: usize = 0;
        if self.multi_bulk_len == 0 {
            assert_eq!(self.argc(), 0);
            new_line = self.query_buf.iter()
                .enumerate()
                .find(|(_, ch)| **ch == b'\r')
                .map(|x| x.0);

            pos = new_line.ok_or(ProcessQueryError::NotEnough)?;

            // TODO: if pos exceed max inline length, return error

            // buffer should also contain '\n'
            if pos > self.query_buf.len() - 2 {
                return Err(ProcessQueryError::NotEnough);
            }

            assert_eq!(self.query_buf[0], b'*');

            let ll = bytes_to_i64(&self.query_buf[1..pos])
                .map_err(|_| ())
                .and_then(|x| {
                    if x > 1024 * 1024 {
                        Err(())
                    } else {
                        Ok(x)
                    }
                })
                .map_err(|_| {
                    self.add_str_reply("-ERR Protocol Error: invalid bulk length\r\n");
                    ProcessQueryError::Protocol(1)
                })?;

            pos += 2;

            if ll <= 0 {
                self.query_buf.drain(0..pos);
                return Ok(());
            }

            self.multi_bulk_len = ll as usize;

            if !self.argv.is_empty() {
                self.argv.clear();
            }

            self.argv.reserve(self.multi_bulk_len);
        }

        assert!(self.multi_bulk_len > 0);

        while self.multi_bulk_len > 0 {
            if let None = self.bulk_len {
                new_line = self.query_buf.iter()
                    .enumerate()
                    .skip(pos)
                    .find(|(_, ch)| **ch == b'\r')
                    .map(|x| x.0);

                let new_line = match new_line {
                    None => break,
                    Some(n) => n,
                };
                // TODO: if pos exceed max inline length, return error

                if pos > self.query_buf.len() - 2 {
                    break;
                }

                if self.query_buf[pos] != b'$' {
                    self.add_reply_from_string(
                        format!("-ERR Protocol Error: expected '$', got {}\r\n",
                                self.query_buf[pos] as char)
                    );
                    return Err(ProcessQueryError::Protocol(pos));
                }

                let ll = bytes_to_usize(&self.query_buf[pos + 1..new_line])
                    .map_err(|_| ())
                    .and_then(|x| {
                        if x > 512 * 1024 * 1024 {
                            Err(())
                        } else {
                            Ok(x)
                        }
                    })
                    .map_err(|_| {
                        self.add_str_reply("-ERR Protocol Error: invalid bulk length\r\n");
                        ProcessQueryError::Protocol(pos)
                    })?;

                pos = new_line + 2;
                self.bulk_len = Some(ll);
            }

            if self.query_buf.len() - pos < self.bulk_len.unwrap() + 2 {
                break;
            } else {
                let arg = Robj::create_bytes_object(
                    &self.query_buf[pos..pos + self.bulk_len.unwrap()]
                );
                pos += self.bulk_len.unwrap() + 2;
                self.argv.push(arg);
                self.bulk_len = None;
                self.multi_bulk_len -= 1;
            }
        }
        if pos > 0 {
            self.query_buf.drain(0..pos);
        }

        if self.multi_bulk_len == 0 {
            return Ok(());
        }

        Err(ProcessQueryError::NotEnough)
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
