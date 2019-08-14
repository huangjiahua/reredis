use crate::client::Client;
use mio::net::TcpStream;
use crate::server::Server;
use crate::ae::AeEventLoop;

type CommandProc = fn(
    client: &mut Client,
    stream: &mut TcpStream,
    server: &mut Server,
    el: &AeEventLoop,
);

// Command flags
const CMD_BULK: i32 = 0b0001;
const CMD_INLINE: i32 = 0b0010;
const CMD_DENY_OOM: i32 = 0b0100;

struct Command<'a> {
    name: &'a str,
    proc: CommandProc,
    arity: i32,
    flags: i32,
}