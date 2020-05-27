use crate::asynchronous::{ClientHandle, EnvConfig, EventLoopHandle, ServerHandle};
use crate::command::{lookup_command, CMD_DENY_OOM};
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

    pub fn execute(&mut self, mut args: Vec<Vec<u8>>) -> Result<Reply, Error> {
        let cmd =
            lookup_command(&args[0]).ok_or(Error::with_message("-Error unknown command\r\n"))?;

        if (cmd.arity > 0 && cmd.arity as usize != args.len())
            || (cmd.arity < 0 && (args.len() < (-cmd.arity) as usize))
        {
            return Err(Error::with_message("-Error wrong number of arguments\r\n"));
        } else if self.max_memory() > 0
            && cmd.flags & CMD_DENY_OOM != 0
            && zalloc::allocated_memory() > self.max_memory()
        {
            return Err(Error::with_message(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n",
            ));
        }

        // TODO: Auth

        // TODO: Save dirty bit here

        let mut client_handle = ClientHandle::new_client_handle();
        client_handle.argv = args.drain(..).map(|x| Robj::from_bytes(x)).collect();

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

pub struct Error {
    pub err_msg: String,
}

impl Error {
    fn with_message(s: &str) -> Error {
        Error {
            err_msg: s.to_string(),
        }
    }
}
