use crate::asynchronous::client::{read_query_from_client, send_reply_to_client};
use crate::asynchronous::common::DBArgs;
use crate::asynchronous::query::{QueryBuilder, QueryError};
use crate::asynchronous::server::Server;
use crate::asynchronous::shared_state::SharedState;
use crate::asynchronous::timer;
use crate::env::REREDIS_VERSION;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::{mpsc, oneshot};
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

        let (args, t) = match tm.await {
            Ok(Some((args, t))) => (args, t),
            Ok(None) => break,
            Err(_) => {
                debug!("Server Cron should execute now...");
                timer.update();
                continue;
            }
        };

        let res = server.execute(args);

        let _ = t.send(res);
    }
    println!("caught signal to quit");
}

pub async fn handle_client(mut sock: TcpStream, mut tx: mpsc::Sender<DBArgs>) {
    let (mut reader, mut writer) = sock.split();
    let mut query_builder = QueryBuilder::new();

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
