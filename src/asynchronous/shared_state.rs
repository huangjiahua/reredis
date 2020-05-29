use crate::asynchronous::EnvConfig;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

pub struct SharedState {
    is_killed: Arc<AtomicBool>,
    db_cnt: AtomicUsize,
}

impl SharedState {
    pub fn new(config: &EnvConfig) -> SharedState {
        let is_killed = Arc::new(AtomicBool::new(false));
        let db_cnt = AtomicUsize::new(config.db_num);
        crate::server::set_up_signal_handling(&is_killed);
        SharedState { is_killed, db_cnt }
    }

    pub fn is_killed(&self) -> bool {
        return self.is_killed.load(Ordering::SeqCst);
    }

    pub fn db_cnt(&self) -> usize {
        return self.db_cnt.load(Ordering::Acquire);
    }
}
