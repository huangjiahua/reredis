use tokio::sync::oneshot;

use crate::asynchronous::server;

pub type DBArgs = (
    Vec<Vec<u8>>,
    oneshot::Sender<Result<server::Reply, server::Error>>,
);
