use std::sync::Arc;
use tokio::sync::Mutex;

use crate::asynchronous::config::Config;
use crate::asynchronous::stat::Stat;
use crate::asynchronous::state::State;
use crate::env::Config as EnvConfig;

pub struct Server {
    state: Arc<Mutex<State>>,
    config: Config,
    stat: Stat,
}

impl Server {
    pub fn new(ec: &EnvConfig) -> Server {
        let state = Arc::new(Mutex::new(State::new(ec)));
        let config = Config::new(ec);
        let stat = Stat::new();

        Server {
            state,
            config,
            stat,
        }
    }

    pub fn execute(&mut self, args: Vec<Vec<u8>>) -> Result<(), ()> {
        for arg in args.iter() {
            println!("{}", std::str::from_utf8(arg).unwrap());
        }
        Ok(())
    }
}
