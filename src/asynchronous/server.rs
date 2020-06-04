use crate::asynchronous::common::DBArgs;
use crate::asynchronous::{ClientHandle, EnvConfig, EventLoopHandle, ServerHandle};
use crate::command::CMD_DENY_OOM;
use crate::object::Robj;
use crate::{rdb, zalloc};
use nix::sys::wait::*;
use nix::unistd::Pid;
use std::rc::Rc;
use std::time::SystemTime;
use crate::env::REREDIS_EXPIRE_LOOKUPS_PER_CRON;
use std::process::exit;

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

    pub fn cron(&mut self) {
        self.server_handle.cron_loops += 1;

        self.server_handle.used_memory = zalloc::allocated_memory();

        let loops = self.server_handle.cron_loops;

        for (i, db) in self.server_handle.db.iter().enumerate() {
            let slot: usize = db.dict.slot();
            let used: usize = db.dict.len();
            let vkeys: usize = db.expires.len();

            if loops % 5 == 0 && (used != 0 || vkeys != 0) {
                debug!(
                    "DB {}: {} keys ({} volatile) in {} slots HT.",
                    i, used, vkeys, slot
                );
            }
        }

        // TODO: show connected clients
        if loops % 5 == 0 {
            debug!("{} bytes in use", self.server_handle.used_memory);
        }

        if self.server_handle.bg_save_in_progress {
            let wait_flag = Some(WaitPidFlag::WNOHANG);
            let r = waitpid(Pid::from_raw(-1), wait_flag);
            if let Ok(stat) = r {
                match stat {
                    WaitStatus::Exited(_, exit_code) => {
                        let _exit_ok;
                        if exit_code == 0 {
                            info!("Background saving terminated with success");
                            self.server_handle.dirty = 0;
                            self.server_handle.last_save = SystemTime::now();
                            _exit_ok = true;
                        } else {
                            warn!("Background saving error");
                            _exit_ok = false;
                        }
                        self.server_handle.bg_save_in_progress = false;
                        self.server_handle.bg_save_child_pid = -1;
                        // TODO: feed replicate here
                    }
                    WaitStatus::StillAlive => {}
                    _ => {
                        warn!("Background saving terminated by signal");
                        self.server_handle.bg_save_in_progress = false;
                        self.server_handle.bg_save_child_pid = -1;
                        // TODO: feed replicate here
                    }
                }
            }
        } else {
            let now = SystemTime::now();
            for (seconds, changes) in self.server_handle.save_params.iter() {
                if self.server_handle.dirty >= *changes
                    && now
                        .duration_since(self.server_handle.last_save)
                        .unwrap()
                        .as_secs() as usize
                        > *seconds
                {
                    let _ = rdb::rdb_save_in_background(&mut self.server_handle);
                    break;
                }
            }
        }

        // try to expire a few timeout keys
        for db in self.server_handle.db.iter_mut() {
            let mut num: usize = db.expires.len();

            if num > 0 {
                let now: SystemTime = SystemTime::now();

                if num > REREDIS_EXPIRE_LOOKUPS_PER_CRON {
                    num = REREDIS_EXPIRE_LOOKUPS_PER_CRON;
                }

                for _ in 0..num {
                    let (key, t) = db.expires.random_key_value();
                    if *t < now {
                        let key = Rc::clone(key);
                        let _ = db.delete_key(&key);
                    }
                }
            }
        }

        // TODO: connect to master
    }

    pub fn port(&self) -> u16 {
        self.server_handle.port
    }

    pub fn max_memory(&self) -> usize {
        self.server_handle.max_memory
    }

    pub fn load_rdb(&mut self) {
        if let Err(e) = rdb::rdb_load(&mut self.server_handle) {
            if let std::io::ErrorKind::NotFound = e.kind() {
                return;
            }
            warn!("{}", e);
            exit(1);
        }
    }

    pub fn prepare_shutdown(mut self) {
        self.server_handle.prepare_shutdown();
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
