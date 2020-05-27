use std::time::Duration;
use tokio::time::Instant;

const LOOP_DURATION: u64 = 1000;

pub struct Timer {
    when: Instant,
}

impl Timer {
    pub fn new() -> Timer {
        let when = Instant::now()
            .checked_add(Duration::from_millis(LOOP_DURATION))
            .expect("Time value overflow");
        Timer { when }
    }

    pub fn when(&self) -> Instant {
        self.when
    }

    pub fn update(&mut self) {
        self.when = self
            .when
            .checked_add(Duration::from_millis(LOOP_DURATION))
            .expect("Time value overflow");
    }
}
