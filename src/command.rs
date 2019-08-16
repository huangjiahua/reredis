use crate::client::Client;
use crate::server::Server;
use crate::ae::AeEventLoop;
use std::rc::Rc;
use crate::shared::{OK, NULL_BULK, CRLF, CZERO, CONE, COLON, WRONG_TYPE};
use crate::util::case_eq;
use crate::object::{Robj, RobjPtr};
use std::mem::swap;

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
    let r = server.db[client.db_idx].look_up_key_read(
        &client.argv[1],
    );

    match r {
        None => client.add_reply(shared_object!(NULL_BULK)),
        Some(s) => {
            if !s.borrow().is_string() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            client.add_str_reply(&format!("${}\r\n", s.borrow().string().len()));
            client.add_reply(s);
            client.add_reply(shared_object!(CRLF));
        }
    }
}

pub fn set_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    set_generic_command(client, server, el, false);
}

pub fn setnx_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    set_generic_command(client, server, el, true);
}


fn set_generic_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
    nx: bool,
) {
    let db = &mut server.db[client.db_idx];
    let r = db.dict.add(
        Rc::clone(&client.argv[1]),
        Rc::clone(&client.argv[2]),
    );

    if r.is_err() {
        if !nx {
            db.dict.replace(
                Rc::clone(&client.argv[1]),
                Rc::clone(&client.argv[2]),
            );
        } else {
            client.add_reply(shared_object!(CZERO));
            return;
        }
    }

    server.dirty += 1;
    let _ = db.remove_expire(&client.argv[1]);
    let reply = match nx {
        true => shared_object!(CONE),
        false => shared_object!(OK),
    };
    client.add_reply(reply);
}

pub fn del_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    let mut deleted: usize = 0;
    for key in client.argv
        .iter()
        .skip(1) {
        if db.delete_key(key).is_ok() {
            deleted += 1;
            server.dirty += 1;
        }
    }

    match deleted {
        0 => client.add_reply(shared_object!(CZERO)),
        1 => client.add_reply(shared_object!(CONE)),
        _ => client.add_str_reply(&format!(":{}\r\n", deleted)),
    }
}

pub fn exists_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    let r = match db.look_up_key_read(&client.argv[1]) {
        Some(_) => shared_object!(CONE),
        None => shared_object!(CZERO),
    };
    client.add_reply(r);
}

pub fn incr_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    incr_decr_command(client, server, el, 1);
}

pub fn decr_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    incr_decr_command(client, server, el, -1);
}

pub fn incr_decr_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
    incr: i64,
) {
    let db = &mut server.db[client.db_idx];
    let mut val: i64;

    let r = db.look_up_key_read(&client.argv[1]);

    val = match r {
        None => 0,
        Some(v) => {
            let n = v.borrow().object_to_long();
            match n {
                Ok(i) => i,
                Err(_) => {
                    client.add_str_reply("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            }
        }
    };
    val = match val.checked_add(incr) {
        None => {
            client.add_str_reply("-ERR increment or decrement would overflow\r\n");
            return;
        }
        Some(v) => v,
    };
    let o = Robj::create_string_object_from_long(val);
    db.dict.replace(Rc::clone(&client.argv[1]), Rc::clone(&o));
    client.add_reply(shared_object!(COLON));
    client.add_reply(o);
    client.add_reply(shared_object!(CRLF));
}

pub fn mget_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let n = client.argc() - 1;
    let db = &mut server.db[client.db_idx];
    client.add_str_reply(&format!("*{}\r\n", n));
    let mut argv: Vec<RobjPtr> = vec![];
    swap(&mut argv, &mut client.argv);
    for key in argv
        .iter()
        .skip(1) {
        let r = db.look_up_key_read(key);
        match r {
            None => client.add_reply(shared_object!(NULL_BULK)),
            Some(o) => {
                if !o.borrow().is_string() {
                    client.add_reply(shared_object!(NULL_BULK));
                }
                client.add_str_reply(&format!("${}\r\n", o.borrow().string().len()));
                client.add_reply(o);
                client.add_reply(shared_object!(CRLF));
            }
        }
    }
}

pub fn rpush_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

pub fn lpush_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

pub fn rpop_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

pub fn lpop_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

pub fn llen_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

const CMD_TABLE: &[Command] = &[
    Command { name: "get", proc: get_command, arity: 2, flags: CMD_INLINE },
    Command { name: "set", proc: set_command, arity: 3, flags: CMD_INLINE },
    Command { name: "setnx", proc: setnx_command, arity: 3, flags: CMD_INLINE },
    Command { name: "del", proc: del_command, arity: -2, flags: CMD_INLINE },
    Command { name: "exists", proc: exists_command, arity: 2, flags: CMD_INLINE },
    Command { name: "incr", proc: incr_command, arity: 2, flags: CMD_INLINE },
    Command { name: "decr", proc: decr_command, arity: 2, flags: CMD_INLINE },
    Command { name: "mget", proc: mget_command, arity: -2, flags: CMD_INLINE },
];

pub fn lookup_command(name: &str) -> Option<&'static Command> {
    CMD_TABLE.iter()
        .find(|x| case_eq(x.name, name))
}
