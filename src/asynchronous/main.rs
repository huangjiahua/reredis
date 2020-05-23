#[macro_use]
extern crate log;
extern crate env_logger;

use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

// use reredis::env::{Env, REREDIS_VERSION, init_logger, Config};
// use reredis::oom::oom;
use reredis::asynchronous::client::{read_query_from_client, send_reply_to_client};
use reredis::asynchronous::query::{QueryBuilder, QueryError};
use reredis::asynchronous::server::Server;
use reredis::asynchronous::EnvConfig;
use reredis::zalloc::Zalloc;
use std::rc::Rc;

#[global_allocator]
static A: Zalloc = Zalloc;

async fn db_executor(
    mut rx: mpsc::Receiver<(Vec<Vec<u8>>, oneshot::Sender<Result<(), ()>>)>,
    mut server: Server,
) {
    println!("db_executor start running");
    loop {
        let (args, t) = match rx.recv().await {
            Some((args, t)) => (args, t),
            None => break,
        };

        let res = server.execute(args);

        let _ = t.send(res);
    }
}

async fn handle_client(
    mut sock: TcpStream,
    mut tx: mpsc::Sender<(Vec<Vec<u8>>, oneshot::Sender<Result<(), ()>>)>,
) {
    let (mut reader, mut writer) = sock.split();
    let mut query_builder = QueryBuilder::new();

    loop {
        let args = match read_query_from_client(&mut query_builder, &mut reader).await {
            Ok(args) => args,
            Err(QueryError::EOF) => {
                println!("Reading from client: {}", "Client closed connection");
                break
            },
            Err(e) => {
                eprintln!("{:?}", e);
                break;
            }
        };

        let (t, r) = oneshot::channel();

        if let Err(_) = tx.send((args, t)).await {
            break;
        }

        let reply = match r.await {
            Ok(reply) => reply,
            Err(_) => break,
        };

        if let Err(_) = send_reply_to_client(&mut writer, reply).await {
            break;
        }
    }
}

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

    let server = Arc::new(Server::new(&config));
    let addr = format!("{}:{}", config.bind_addr, config.port);
    let mut listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    let config_clone = config.clone();

    let (tx, rx) = mpsc::channel(10);

    let local = task::LocalSet::new();

    let local_runner = local.run_until(async move {
        let server = Server::new(&config_clone);
        db_executor(rx, server).await;
    });

    tokio::spawn(async move {
        println!("Server running on localhost");
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
