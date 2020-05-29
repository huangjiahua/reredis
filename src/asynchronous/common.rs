use tokio::sync::oneshot;

use crate::asynchronous::server;
use crate::command::Command;

pub struct DBArgs {
    pub cmd: &'static Command,
    pub db_id: usize,
    pub args: Vec<Vec<u8>>,
    pub chan: oneshot::Sender<Result<server::Reply, server::Error>>,
}
