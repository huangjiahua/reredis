use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;

pub struct Stat {
    // how many commands have been executed
    num_commands: AtomicUsize,
    // how many client is currently connected
    num_connections: AtomicUsize,
    // time when the server start running
    start_time: SystemTime,
}

impl Stat {
    pub fn new() -> Stat {
        Stat {
            num_commands: AtomicUsize::new(0),
            num_connections: AtomicUsize::new(0),
            start_time: SystemTime::now(),
        }
    }
}
