use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct SharedState {
    is_killed: Arc<AtomicBool>,
}

impl SharedState {
    pub fn new() -> SharedState {
        let is_killed = Arc::new(AtomicBool::new(false));
        crate::server::set_up_signal_handling(&is_killed);
        SharedState {
            is_killed,
        }
    }

    pub fn is_killed(&self) -> bool {
        return self.is_killed.load(Ordering::SeqCst);
    }
}