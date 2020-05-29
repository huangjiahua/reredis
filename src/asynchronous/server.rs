use crate::asynchronous::common::DBArgs;
use crate::asynchronous::{ClientHandle, EnvConfig, EventLoopHandle, ServerHandle};
use crate::command::CMD_DENY_OOM;
use crate::object::Robj;
use crate::zalloc;
use std::rc::Rc;

pub struct Server {
    server_handle: ServerHandle,
    el_handle: EventLoopHandle,
}

impl Server {
    pub fn new(ec: &EnvConfig) -> Server {
        let server_handle = ServerHandle::new_handle(ec);
        let el_handle = EventLoopHandle::new_handle();
        Server {
            server_handle,
            el_handle,
        }
    }

    pub fn execute(&mut self, args: &mut DBArgs) -> Result<Reply, Error> {
        let cmd = args.cmd;

        if self.max_memory() > 0
            && cmd.flags & CMD_DENY_OOM != 0
            && zalloc::allocated_memory() > self.max_memory()
        {
            return Err(Error::OOM);
        }

        // TODO: Auth

        // TODO: Save dirty bit here

        let mut client_handle = ClientHandle::new_client_handle();
        client_handle.argv = args.args.drain(..).map(|x| Robj::from_bytes(x)).collect();
        client_handle.db_idx = args.db_id;

        (&cmd.proc)(
            &mut client_handle,
            &mut self.server_handle,
            &mut self.el_handle,
        );

        // TODO: feed slaves and monitors here

        let reply = Reply::from_client_handle(&mut client_handle);

        Ok(reply)
    }

    pub fn port(&self) -> u16 {
        self.server_handle.port
    }

    pub fn max_memory(&self) -> usize {
        self.server_handle.max_memory
    }
}

pub struct Args {}

pub struct Reply {
    pub reply: Vec<Vec<u8>>,
}

impl Reply {
    pub fn new(reply: Vec<Vec<u8>>) -> Reply {
        Reply { reply }
    }

    pub fn from_single(r: Vec<u8>) -> Reply {
        let reply = vec![r];
        Reply { reply }
    }

    pub fn from_str(s: &str) -> Reply {
        let reply = vec![s.as_bytes().to_vec()];
        Reply { reply }
    }

    pub fn ok() -> Reply {
        let reply = vec!["+OK\r\n".as_bytes().to_vec()];
        Reply { reply }
    }

    pub fn pong() -> Reply {
        let reply = vec!["+PONG\r\n".as_bytes().to_vec()];
        Reply { reply }
    }

    fn from_client_handle(handle: &mut ClientHandle) -> Reply {
        let reply = handle
            .reply
            .drain(..)
            .map(|x| match Rc::try_unwrap(x) {
                Ok(o) => o.into_inner().unwrap_data().unwrap_bytes(),
                Err(ro) => ro.borrow().string().to_vec(),
            })
            .collect();

        Reply { reply }
    }
}

pub enum Error {
    UnknownCommand,
    WrongArgNum,
    OOM,
    Quit,
}

impl Error {
    pub fn err_msg(&self) -> &str {
        match self {
            Self::UnknownCommand => "-Error unknown command\r\n",
            Self::WrongArgNum => "-Error wrong number of arguments\r\n",
            Self::OOM => "-ERR command not allowed when used memory > 'maxmemory'\r\n",
            Self::Quit => "-ERR you should not see this message\r\n",
        }
    }
}
