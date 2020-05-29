use crate::asynchronous::EnvConfig;

pub struct Config {
    max_clients: usize,
    client_timeout: usize,
}

impl Config {
    pub fn new(ec: &EnvConfig) -> Config {
        Config {
            max_clients: ec.max_clients,
            client_timeout: ec.max_idle_time,
        }
    }
}
