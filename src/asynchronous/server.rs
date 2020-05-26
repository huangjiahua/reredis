use crate::asynchronous::{ClientHandle, EnvConfig, EventLoopHandle, ServerHandle};
use crate::command::lookup_command;
use crate::object::Robj;

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
        for arg in args.iter() {
            println!("{}", std::str::from_utf8(arg).unwrap());
        }

        let cmd =
            lookup_command(&args[0]).ok_or(Error::with_message("-Error unknown command\r\n"))?;

        let mut client_handle = ClientHandle::new_client_handle();
        client_handle.argv = args.drain(..).map(|x| Robj::from_bytes(x)).collect();

        (&cmd.proc)(
            &mut client_handle,
            &mut self.server_handle,
            &mut self.el_handle,
        );

        let reply = Reply::from_client_handle(&mut client_handle);

        Ok(reply)
    }

    pub fn port(&self) -> u16 {
        self.server_handle.port
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
            .map(|x| x.borrow().string().to_vec())
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
