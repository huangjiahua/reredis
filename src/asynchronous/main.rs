#[macro_use]
extern crate log;
extern crate env_logger;

use std::sync::Arc;

use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::task;

// use reredis::env::{Env, REREDIS_VERSION, init_logger, Config};
// use reredis::oom::oom;
use reredis::asynchronous::executor::*;
use reredis::asynchronous::server::Server;
use reredis::asynchronous::shared_state::SharedState;
use reredis::asynchronous::EnvConfig;
use reredis::env::init_logger;
use reredis::zalloc::Zalloc;

#[global_allocator]
static A: Zalloc = Zalloc;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = EnvConfig::new();

    if args.len() == 2 {
        config.reset_server_save_params();
        config.load_server_config(&args[1]);
    } else if args.len() > 2 {
        config.config_from_args(&args[..]);
    } else {
        println!(
            "no config file specified, using the default config. \
            In order to specify a config file use 'reredis-server \
            /path/to/reredis.conf'"
        );
    }

    init_logger(config.log_level);

    let addr = format!("{}:{}", config.bind_addr, config.port);
    let mut listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    let config_clone = config.clone();

    let (tx, rx) = mpsc::channel(10);

    let shared_state = Arc::new(SharedState::new());
    let local = task::LocalSet::new();
    let local_runner = local.run_until(async move {
        let server = Server::new(&config_clone);
        db_executor(rx, server, shared_state).await;
    });

    tokio::spawn(async move {
        warn!("Server running on localhost");
        let tx = tx;
        let mut incoming = listener.incoming();
        while let Some(socket_res) = incoming.next().await {
            match socket_res {
                Ok(socket) => {
                    println!("Accepted connection from {:?}", socket.peer_addr());
                    tokio::spawn(handle_client(socket, tx.clone()));
                }
                Err(err) => {
                    // Handle error by printing to STDOUT.
                    println!("accept error = {:?}", err);
                }
            }
        }
    });

    local_runner.await;
}
