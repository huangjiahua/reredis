use crate::asynchronous::client::{read_query_from_client, send_reply_to_client};
use crate::asynchronous::common::DBArgs;
use crate::asynchronous::preprocess::{Preprocessed, Preprocessor};
use crate::asynchronous::query::{QueryBuilder, QueryError};
use crate::asynchronous::server;
use crate::asynchronous::server::Server;
use crate::asynchronous::shared_state::SharedState;
use crate::asynchronous::timer;
use crate::env::REREDIS_VERSION;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::time::timeout_at;

pub async fn db_executor(
    mut rx: mpsc::Receiver<DBArgs>,
    mut server: Server,
    shared_state: Arc<SharedState>,
) {
    info!("Server started, Reredis version {}", REREDIS_VERSION);
    // TODO: RDB load
    info!(
        "The server is now ready to accept connections on port {}",
        server.port()
    );

    let mut timer = timer::Timer::new();

    while !shared_state.is_killed() {
        let tm = timeout_at(timer.when(), rx.recv());

        let mut db_args = match tm.await {
            Ok(Some(a)) => a,
            Ok(None) => break,
            Err(_) => {
                timer.update();
                server.cron(&shared_state);
                continue;
            }
        };

        let res = server.execute(&mut db_args);

        let _ = db_args.chan.send(res);
    }
    server.prepare_shutdown();
}

pub async fn handle_client(
    mut sock: TcpStream,
    mut tx: mpsc::Sender<DBArgs>,
    shared_state: Arc<SharedState>,
) {
    let (mut reader, mut writer) = sock.split();
    let mut query_builder = QueryBuilder::new(shared_state.max_idle_time);
    let mut preprocessor = Preprocessor::new(shared_state);

    loop {
        let args = match read_query_from_client(&mut query_builder, &mut reader).await {
            Ok(args) => args,
            Err(QueryError::EOF) => {
                debug!("Reading from client: {}", "Client closed connection");
                break;
            }
            Err(QueryError::Protocol(_, err_msg)) => {
                let _ = writer.write_all(err_msg.as_bytes()).await;
                break;
            }
            Err(e) => {
                debug!("{:?}", e);
                break;
            }
        };

        let reply = match preprocessor.process(args) {
            Preprocessed::Done(r) => r,
            Preprocessed::NotYet((db_args, r)) => {
                if let Err(_) = tx.send(db_args).await {
                    break;
                }
                let reply = match r.await {
                    Ok(reply) => reply,
                    Err(_) => break,
                };
                reply
            }
        };

        if let Err(server::Error::Quit) = reply {
            break;
        }

        if let Err(_) = send_reply_to_client(&mut writer, reply).await {
            break;
        }
    }
}
