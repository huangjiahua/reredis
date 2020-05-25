use crate::asynchronous::EnvConfig;
use crate::asynchronous::ServerHandle;

pub struct State {
    pub handle: ServerHandle,
}

impl State {
    pub fn new(ec: &EnvConfig) -> State {
        State {
            handle: ServerHandle::new_handle(ec),
        }
    }
}
