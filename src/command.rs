use crate::client::Client;
use crate::server::Server;
use crate::ae::AeEventLoop;
use std::rc::Rc;

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
    let r = server.db[client.db_idx].look_up_key_read(
        &client.argv[1],
    );

    match r {
        None => client.add_str_reply("$-1\r\n"),
        Some(s) => {
            let b = format!(
                "${}\r\n{}\r\n",
                s.borrow().string().len(),
                s.borrow().string()
            );
            client.add_str_reply(&b);
        }
    }
}

pub fn set_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    debug!("use the set_command proc");
    set_generic_command(client, server, el, 0);
}

fn set_generic_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
    nx: usize,
) {
    let db = &mut server.db[client.db_idx];
    let r = db.dict.add(
        Rc::clone(&client.argv[1]),
        Rc::clone(&client.argv[2]),
    );

    if r.is_err() {
        if nx == 0 {
            db.dict.replace(
                Rc::clone(&client.argv[1]),
                Rc::clone(&client.argv[2]),
            );
        } else {
            // TODO: shared object
            client.add_str_reply(":0\r\n");
            return;
        }
    }

    server.dirty += 1;
    db.remove_expire(&client.argv[1]);
    client.add_str_reply("+OK\r\n");
}


const CMD_TABLE: &[Command] = &[
    Command { name: "get", proc: get_command, arity: 2, flags: CMD_INLINE },
    Command { name: "set", proc: set_command, arity: 3, flags: CMD_INLINE },
];

pub fn lookup_command(name: &str) -> Option<&'static Command> {
    CMD_TABLE.iter()
        .find(|x| x.name == name)
}
