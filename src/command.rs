use crate::client::Client;
use crate::server::Server;
use crate::ae::AeEventLoop;
use std::rc::Rc;
use crate::shared::{OK, NULL_BULK, CRLF, CZERO, CONE, COLON, WRONG_TYPE, PONG};
use crate::util::case_eq;
use crate::object::{Robj, RobjPtr, RobjEncoding, RobjType};
use std::mem::swap;
use crate::object::list::ListWhere;
use crate::object::RobjType::List;

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
    push_generic_command(client, server, el, ListWhere::Tail);
}

pub fn lpush_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    push_generic_command(client, server, el, ListWhere::Head);
}

pub fn push_generic_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
    w: ListWhere,
) {
    let db = &mut server.db[client.db_idx];
    let r = db.look_up_key_read(&client.argv[1]);
    let mut create_new: bool = false;

    let list_obj = match r {
        None => {
            create_new = true;
            Robj::create_zip_list_object()
        }
        Some(o) => o,
    };

    if !list_obj.borrow().is_list() {
        client.add_reply(shared_object!(WRONG_TYPE));
        return;
    }

    for key in client.argv
        .iter()
        .skip(2) {
        match w {
            ListWhere::Tail => list_obj.borrow_mut().list_push_back(Rc::clone(key)),
            ListWhere::Head => list_obj.borrow_mut().list_push_front(Rc::clone(key)),
        }
    }

    if create_new {
        db.dict.add(Rc::clone(&client.argv[1]), list_obj).unwrap();
    }

    server.dirty += 1;
    if client.argc() - 2 == 1 {
        client.add_reply(shared_object!(CZERO));
    } else {
        client.add_str_reply(&format!(":{}\r\n", client.argc() - 2));
    }
}

pub fn rpop_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    pop_generic_command(client, server, el, ListWhere::Tail);
}

pub fn lpop_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    pop_generic_command(client, server, el, ListWhere::Head);
}

fn pop_generic_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
    w: ListWhere,
) {
    let db = &mut server.db[client.db_idx];
    let r = db.look_up_key_read(&client.argv[1]);

    let list_obj = match r {
        None => {
            client.add_reply(shared_object!(NULL_BULK));
            return;
        }
        Some(o) => o,
    };

    let o = list_obj.borrow_mut().list_pop(w);

    match o {
        None => client.add_reply(shared_object!(NULL_BULK)),
        Some(o) => {
            client.add_str_reply(&format!("${}\r\n", o.borrow().string().len()));
            client.add_reply(o);
            client.add_reply(shared_object!(CRLF));
            server.dirty += 1;
        }
    }
}

pub fn llen_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    unimplemented!()
}

pub fn incr_by_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let r = client.argv[2].borrow().object_to_long();
    match r {
        Ok(n) => incr_decr_command(client, server, el, n),
        Err(_) => client.add_str_reply("-ERR value is not an integer or out of range\r\n"),
    }
}

pub fn decr_by_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let r = client.argv[2].borrow().object_to_long();
    match r {
        Ok(n) => {
            if n == std::i64::MIN {
                client.add_str_reply("-ERR value is not an integer or out of range\r\n");
                return;
            }
            incr_decr_command(client, server, el, -n)
        }
        Err(_) => client.add_str_reply("-ERR value is not an integer or out of range\r\n"),
    }
}

pub fn get_set_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    get_command(client, server, el);
    let db = &mut server.db[client.db_idx];
    db.dict.replace(Rc::clone(&client.argv[1]),
                    Rc::clone(&client.argv[2]));
    db.remove_expire(&client.argv[1]);
    server.dirty += 1;
}

pub fn select_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let idx = client.argv[1].borrow().object_to_long();
    match idx {
        Err(_) => {
            client.add_str_reply("-ERR invalid DB index\r\n")
        }
        Ok(idx) => {
            if idx < 0 || idx >= server.db.len() as i64 {
                client.add_str_reply("-ERR invalid DB index\r\n");
                return;
            }
            client.db_idx = idx as usize;
            client.add_reply(shared_object!(OK));
        }
    }
}

pub fn ping_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    client.add_reply(shared_object!(PONG));
}

pub fn command_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    client.add_reply(shared_object!(OK));
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
    Command { name: "rpush", proc: rpush_command, arity: -3, flags: CMD_INLINE },
    Command { name: "lpush", proc: lpush_command, arity: -3, flags: CMD_INLINE },
    Command { name: "lpop", proc: lpop_command, arity: 2, flags: CMD_INLINE },
    Command { name: "rpop", proc: rpop_command, arity: 2, flags: CMD_INLINE },
    // TODO
    Command { name: "incrby", proc: incr_by_command, arity: 3, flags: CMD_INLINE },
    Command { name: "decrby", proc: decr_by_command, arity: 3, flags: CMD_INLINE },
    Command { name: "getset", proc: get_set_command, arity: 3, flags: CMD_INLINE },
    // TODO
    Command { name: "select", proc: select_command, arity: 2, flags: CMD_INLINE },
    // TODO
    Command { name: "ping", proc: ping_command, arity: 1, flags: CMD_INLINE },
    // TODO
    Command { name: "command", proc: command_command, arity: 1, flags: CMD_INLINE },
];

pub fn lookup_command(name: &str) -> Option<&'static Command> {
    CMD_TABLE.iter()
        .find(|x| case_eq(x.name, name))
}
