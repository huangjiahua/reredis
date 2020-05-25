use std::sync::Arc;
use tokio::sync::Mutex;

use crate::asynchronous::config::Config;
use crate::asynchronous::stat::Stat;
use crate::asynchronous::state::State;
use crate::asynchronous::ClientHandle;
use crate::asynchronous::{EnvConfig, EventLoopHandle};
use crate::command::lookup_command;
use crate::object::Robj;
use futures::StreamExt;

pub struct Server {
    state: State,
    config: Config,
    stat: Stat,
    el_handle: EventLoopHandle,
}

impl Server {
    pub fn new(ec: &EnvConfig) -> Server {
        let state = State::new(ec);
        let config = Config::new(ec);
        let stat = Stat::new();
        let el_handle = EventLoopHandle::new_handle();

        Server {
            state,
            config,
            stat,
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
            &mut self.state.handle,
            &mut self.el_handle,
        );

        let reply = Reply::from_client_handle(&mut client_handle);

        Ok(reply)
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
