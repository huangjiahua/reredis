use crate::asynchronous::common::DBArgs;
use crate::asynchronous::server::{Error, Reply};

use crate::asynchronous::shared_state::SharedState;
use crate::command::{lookup_command, Command, CMD_PREPROCESS};
use crate::util::{bytes_to_i64, case_eq};
use std::sync::Arc;
use tokio::sync::oneshot;

pub enum Preprocessed {
    Done(Result<Reply, Error>),
    NotYet((DBArgs, oneshot::Receiver<Result<Reply, Error>>)),
}

pub struct Preprocessor {
    db_id: usize,
    shared_state: Arc<SharedState>,
}

impl Preprocessor {
    pub fn new(ss: Arc<SharedState>) -> Preprocessor {
        Preprocessor {
            db_id: 0,
            shared_state: ss,
        }
    }

    pub fn process(&mut self, args: Vec<Vec<u8>>) -> Preprocessed {
        if case_eq(&args[0], "quit".as_bytes()) {
            return Preprocessed::Done(Err(Error::Quit));
        }

        let cmd = match lookup_command(&args[0]) {
            None => return Preprocessed::Done(Err(Error::UnknownCommand)),
            Some(c) => c,
        };

        if (cmd.arity > 0 && cmd.arity as usize != args.len())
            || (cmd.arity < 0 && (args.len() < (-cmd.arity) as usize))
        {
            return Preprocessed::Done(Err(Error::WrongArgNum));
        }

        if cmd.flags & CMD_PREPROCESS != 0 {
            return self.execute_preprocessed_command(cmd, args);
        }

        let (t, r) = oneshot::channel();
        Preprocessed::NotYet((
            DBArgs {
                cmd,
                db_id: self.db_id,
                args,
                chan: t,
            },
            r,
        ))
    }

    fn execute_preprocessed_command(&mut self, cmd: &Command, args: Vec<Vec<u8>>) -> Preprocessed {
        if cmd.name == "select" {
            return self.execute_select(args);
        }
        if cmd.name == "ping" {
            return self.execute_ping();
        }
        if cmd.name == "echo" {
            return self.execute_echo(args);
        }
        if cmd.name == "command" {
            return Preprocessed::Done(Ok(Reply::ok()));
        }
        unimplemented!()
    }

    fn execute_select(&mut self, args: Vec<Vec<u8>>) -> Preprocessed {
        let idx = bytes_to_i64(&args[1]);
        match idx {
            Err(_) => Preprocessed::Done(Ok(Reply::from_str("-ERR invalid DB index\r\n"))),
            Ok(idx) => {
                if idx < 0 || idx > self.shared_state.db_cnt() as i64 {
                    return Preprocessed::Done(Ok(Reply::from_str("-ERR invalid DB index\r\n")));
                }
                self.db_id = idx as usize;
                Preprocessed::Done(Ok(Reply::ok()))
            }
        }
    }

    fn execute_ping(&self) -> Preprocessed {
        Preprocessed::Done(Ok(Reply::pong()))
    }

    fn execute_echo(&self, mut args: Vec<Vec<u8>>) -> Preprocessed {
        let l = format!("${}\r\n", args[1].len()).as_bytes().to_vec();
        let r = vec![l, args.remove(1), "\r\n".as_bytes().to_vec()];
        Preprocessed::Done(Ok(Reply::new(r)))
    }
}
