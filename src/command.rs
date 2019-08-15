use crate::client::Client;
use crate::server::Server;
use crate::ae::AeEventLoop;

type CommandProc = fn(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
);

// Command flags
pub const CMD_BULK: i32 = 0b0001;
pub const CMD_INLINE: i32 = 0b0010;
pub const CMD_DENY_OOM: i32 = 0b0100;

pub struct Command {
    pub name: &'static str,
    pub proc: CommandProc,
    pub arity: i32,
    pub flags: i32,
}

pub fn get_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    debug!("use the get_command proc");
}

pub fn set_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    debug!("use the set_command proc");
}


const CMD_TABLE: &[Command] = &[
    Command { name: "get", proc: get_command, arity: 2, flags: CMD_INLINE },
    Command { name: "set", proc: set_command, arity: 3, flags: CMD_INLINE },
];

pub fn lookup_command(name: &str) -> Option<&'static Command> {
    CMD_TABLE.iter()
        .find(|x| x.name == name)
}
