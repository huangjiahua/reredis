use crate::asynchronous::EnvConfig;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

pub struct SharedState {
    is_killed: Arc<AtomicBool>,
    db_cnt: AtomicUsize,
    password: Option<String>,
    pub max_idle_time: Option<usize>,
}

impl SharedState {
    pub fn new(config: &EnvConfig) -> SharedState {
        let is_killed = Arc::new(AtomicBool::new(false));
        let db_cnt = AtomicUsize::new(config.db_num);
        let max_idle_time = if config.max_idle_time > 0 {
            Some(config.max_idle_time)
        } else {
            None
        };
        let password = config.require_pass.clone();
        crate::server::set_up_signal_handling(&is_killed);
        SharedState {
            is_killed,
            db_cnt,
            password,
            max_idle_time,
        }
    }

    pub fn is_killed(&self) -> bool {
        self.is_killed.load(Ordering::SeqCst)
    }

    pub fn db_cnt(&self) -> usize {
        self.db_cnt.load(Ordering::Acquire)
    }

    pub fn require_pass(&self) -> bool {
        self.password.is_some()
    }

    pub fn auth_pass(&self, pw: &[u8]) -> bool {
        match self.password.as_ref() {
            None => true,
            Some(p) => p.as_bytes() == pw,
        }
    }
}
