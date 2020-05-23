use crate::asynchronous::EnvConfig;

pub struct Config {
    max_clients: usize,
}

impl Config {
    pub fn new(ec: &EnvConfig) -> Config {
        Config {
            max_clients: ec.max_clients,
        }
    }
}
