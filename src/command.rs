use std::mem::swap;
use std::rc::Rc;
use std::cmp::{min, Ordering};
use crate::client::Client;
use crate::server::Server;
use crate::ae::AeEventLoop;
use crate::shared::{OK, NULL_BULK, CRLF, CZERO, CONE, COLON, WRONG_TYPE, PONG, EMPTY_MULTI_BULK};
use crate::util::{case_eq, bytes_to_i64};
use crate::object::{Robj, RobjPtr, RobjEncoding, RobjType};
use crate::object::list::ListWhere;
use crate::object::RobjType::List;
use rand::Rng;

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
            let enc = s.borrow().encoding();
            let rep = match enc {
                RobjEncoding::Raw => s,
                RobjEncoding::EmbStr => s,
                RobjEncoding::Int => s.borrow().gen_string(),
                _ => {
                    client.add_reply(shared_object!(WRONG_TYPE));
                    return;
                }
            };
            add_single_reply(client, rep);
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
    let o = to_int_if_needed(&client.argv[2]);

    let db = &mut server.db[client.db_idx];
    let r = db.dict.add(
        Rc::clone(&client.argv[1]),
        Rc::clone(&o),
    );

    if r.is_err() {
        if !nx {
            db.dict.replace(
                Rc::clone(&client.argv[1]),
                o,
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

pub fn to_int_if_needed(o: &RobjPtr) -> RobjPtr {
    let can_be_int = o.borrow().object_to_long();
    match can_be_int {
        Err(_) => Rc::clone(o),
        Ok(i) => Robj::create_int_object(i),
    }
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

    client.add_reply(gen_usize_reply(deleted));
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
    // TODO: no need to replace, just change the inner data
    let db = &mut server.db[client.db_idx];
    let mut val: i64;

    let r = db.look_up_key_read(&client.argv[1]);

    val = match r {
        None => 0,
        Some(v) => {
            let enc = v.borrow().encoding();
            match enc {
                RobjEncoding::Int => v.borrow().integer(),
                _ => {
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
    let o = Robj::create_int_object(val);
    db.dict.replace(Rc::clone(&client.argv[1]), Rc::clone(&o));
    client.add_reply(shared_object!(COLON));
    client.add_reply(o.borrow().gen_string());
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
                add_single_reply(client, o);
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
        list_obj.borrow_mut().list_push(Rc::clone(key), w);
    }

    let len = list_obj.borrow().list_len();

    if create_new {
        db.dict.add(Rc::clone(&client.argv[1]), list_obj).unwrap();
    }

    server.dirty += 1;
    if len == 0 {
        client.add_reply(shared_object!(CZERO));
    } else {
        client.add_str_reply(&format!(":{}\r\n", len));
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
    if list_obj.borrow().list_len() == 0 {
        let _ = db.delete_key(&client.argv[1]);
    }

    match o {
        None => client.add_reply(shared_object!(NULL_BULK)),
        Some(o) => {
            add_single_reply(client, o);
            server.dirty += 1;
        }
    }
}

pub fn llen_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    match db.look_up_key_read(&client.argv[1]) {
        None => client.add_reply(shared_object!(CZERO)),
        Some(o) => {
            if o.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
            } else {
                client.add_reply(gen_usize_reply(o.borrow().list_len()));
            }
        }
    }
}

pub fn lindex_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let to_int = client.argv[2].borrow().object_to_long();

    let idx = match to_int {
        Ok(i) => i,
        Err(_) => {
            client.add_str_reply("-ERR value is not an integer or out of range\r\n");
            return;
        }
    };

    match db.look_up_key_read(&client.argv[1]) {
        None => client.add_reply(shared_object!(NULL_BULK)),
        Some(o) => {
            if o.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
            } else {
                let len = o.borrow().list_len();
                let real_idx = real_list_index(idx, len);

                if real_idx < 0 {
                    client.add_reply(shared_object!(NULL_BULK));
                    return;
                }

                match o.borrow().list_index(real_idx as usize) {
                    None => client.add_reply(shared_object!(NULL_BULK)),
                    Some(r) => {
                        add_single_reply(client, r);
                    }
                }
            }
        }
    }
}

pub fn lset_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let to_int = client.argv[2].borrow().object_to_long();

    let idx = match to_int {
        Ok(i) => i,
        Err(_) => {
            client.add_str_reply("-ERR value is not an integer or out of range\r\n");
            return;
        }
    };

    match db.look_up_key_read(&client.argv[1]) {
        None => client.add_str_reply("-ERR no such key\r\n"),
        Some(o) => {
            if o.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
            } else {
                let len = o.borrow().list_len();
                let real_idx = real_list_index(idx, len);

                if real_idx < 0 {
                    client.add_str_reply("-ERR index out of range\r\n");
                    return;
                }

                match o.borrow_mut()
                    .list_set(real_idx as usize, Rc::clone(&client.argv[3])) {
                    Ok(_) => client.add_reply(shared_object!(OK)),
                    Err(_) => client.add_str_reply("-ERR index out of range\r\n"),
                }
            }
        }
    }
    server.dirty += 1;
}

pub fn lrange_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let (left, right)
        = (client.argv[2].borrow().object_to_long(),
           client.argv[3].borrow().object_to_long());

    if left.is_err() || right.is_err() {
        client.add_str_reply("-ERR value is not an integer or out of range\r\n");
        return;
    }

    let (left, right) = (left.unwrap(), right.unwrap());

    let o = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(EMPTY_MULTI_BULK));
            return;
        }
        Some(obj) => {
            if obj.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            obj
        }
    };

    let len = o.borrow().list_len();

    let (mut left, mut right) = (real_list_index(left, len),
                                 real_list_index(right, len));

    if (left < 0 && right < 0) || (left >= 0 && left as usize >= len) || left > right {
        client.add_reply(shared_object!(EMPTY_MULTI_BULK));
        return;
    }

    if left < 0 {
        left = 0;
    }

    if right >= len as i64 {
        right = len as i64 - 1;
    }

    client.add_str_reply(
        &format!("*{}\r\n", right - left + 1)
    );

    for r in o.borrow().list_iter().skip(left as usize) {
        add_single_reply(client, r);
        right -= 1;
        if right < left {
            break;
        }
    }
}

pub fn ltrim_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let (left, right)
        = (client.argv[2].borrow().object_to_long(),
           client.argv[3].borrow().object_to_long());

    if left.is_err() || right.is_err() {
        client.add_str_reply("-ERR value is not an integer or out of range\r\n");
        return;
    }

    let (left, right) = (left.unwrap(), right.unwrap());

    let o = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(EMPTY_MULTI_BULK));
            return;
        }
        Some(obj) => {
            if obj.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            obj
        }
    };

    let len = o.borrow().list_len();

    let (mut left, mut right) =
        (real_list_index(left, len), real_list_index(right, len));

    if left > right || right < 0 || left >= len as i64 {
        o.borrow_mut().list_trim(len, len - 1);
    } else {
        if left < 0 {
            left = 0;
        }
        if right >= len as i64 {
            right = len as i64 - 1;
        }
        o.borrow_mut().list_trim(left as usize, right as usize);
    }
    if o.borrow().list_len() == 0 {
        let _ = db.delete_key(&client.argv[1]);
    }
    client.add_reply(shared_object!(OK));
    server.dirty += 1;
}

pub fn lrem_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let to_int = client.argv[2].borrow().object_to_long();
    let i = match to_int {
        Err(_) => {
            client.add_str_reply("-ERR value is not an integer or out of range\r\n");
            return;
        }
        Ok(i) => i,
    };

    let o = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(CZERO));
            return;
        }
        Some(obj) => {
            if obj.borrow().object_type() != RobjType::List {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            obj
        }
    };

    let (n, w) = if i < 0 {
        (-i as usize, ListWhere::Tail)
    } else if i == 0 {
        (o.borrow().list_len(), ListWhere::Head)
    } else {
        (i as usize, ListWhere::Head)
    };

    let n = o.borrow_mut().list_del_n(w, n, &client.argv[3]);
    if o.borrow().list_len() == 0 {
        let _ = db.delete_key(&client.argv[1]);
    }
    server.dirty += 1;
    client.add_reply(gen_usize_reply(n));
}

pub fn sadd_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    let mut old_len: usize = 0;

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            let o = Robj::create_int_set_object();
            let _ = db.dict.add(Rc::clone(&client.argv[1]), Rc::clone(&o));
            o
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            old_len = o.borrow().set_len();
            o
        }
    };

    for idx in 2..client.argv.len() {
        let new = to_int_if_needed(&client.argv[idx]);
        let _ = set_obj.borrow_mut().set_add(new);
    }

    client.add_reply(gen_usize_reply(set_obj.borrow().set_len() - old_len));
    server.dirty += 1;
}

pub fn srem_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    let old_len: usize;
    let cur_len: usize;

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(CZERO));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    old_len = set_obj.borrow().set_len();

    for o in client.argv.iter().skip(2) {
        set_obj.borrow_mut().set_delete(o);
        if set_obj.borrow().set_len() == 0 {
            break;
        }
    }

    cur_len = set_obj.borrow().set_len();

    client.add_reply(gen_usize_reply(old_len - cur_len));
    if cur_len == 0 {
        db.delete_key(&client.argv[1]);
    }

    server.dirty += 1;
}

pub fn smembers_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(EMPTY_MULTI_BULK));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    client.add_str_reply(&format!("*{}\r\n", set_obj.borrow().set_len()));

    for o in set_obj.borrow().set_iter() {
        add_single_reply(client, o);
    }
}

pub fn smove_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let src_set = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(CZERO));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    let mut dst_set = match db.look_up_key_read(&client.argv[2]) {
        None => {
            None
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            Some(o)
        }
    };

    let r = src_set.borrow_mut().set_delete(&client.argv[3]);

    match r {
        Ok(_) => {
            let dst_set = match dst_set {
                Some(s) => s,
                None => {
                    let set = Robj::create_int_set_object();
                    db.dict.add(
                        Rc::clone(&client.argv[2]),
                        Rc::clone(&set),
                    );
                    set
                }
            };
            dst_set.borrow_mut().set_add(Rc::clone(&client.argv[3]));
            client.add_reply(shared_object!(CONE));
        }
        Err(_) => {
            client.add_reply(shared_object!(CZERO));
        }
    }

    if src_set.borrow().set_len() == 0 {
        db.delete_key(&client.argv[1]);
    }
    server.dirty += 1;
}

pub fn sismember_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(CZERO));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    let r = set_obj.borrow().set_exists(&client.argv[2]);

    match r {
        true => client.add_reply(shared_object!(CONE)),
        false => client.add_reply(shared_object!(CZERO)),
    }
}

pub fn scard_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(CZERO));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    client.add_reply(gen_usize_reply(
        set_obj.borrow().set_len()
    ));
}

pub fn spop_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let db = &mut server.db[client.db_idx];
    let old_len: usize;
    let deleted: usize;

    let set_obj = match db.look_up_key_read(&client.argv[1]) {
        None => {
            client.add_reply(shared_object!(EMPTY_MULTI_BULK));
            return;
        }
        Some(o) => {
            if !o.borrow().is_set() {
                client.add_reply(shared_object!(WRONG_TYPE));
                return;
            }
            o
        }
    };

    old_len = set_obj.borrow().set_len();
    deleted = rand::thread_rng().gen_range(0, old_len + 1);

    client.add_str_reply(&format!("*{}\r\n", deleted));
    for _ in 0..deleted {
        add_single_reply(
            client,
            set_obj.borrow_mut().set_pop_random(),
        );
    }

    if deleted == old_len {
        db.delete_key(&client.argv[1]);
    }
    server.dirty += 1;
}

pub fn sinter_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    sinter_general_command(client, server, None);
}

pub fn sinterstore_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let mut new_set = Robj::create_int_set_object();
    let new_key = client.argv.drain(1..2).next().unwrap();
    sinter_general_command(client, server, Some(Rc::clone(&new_set)));
    if new_set.borrow().set_len() > 0 {
        server.db[client.db_idx].dict.replace(new_key, new_set);
    }
}

fn sinter_general_command(
    client: &mut Client,
    server: &mut Server,
    mut dst: Option<RobjPtr>,
) {
    let db = &mut server.db[client.db_idx];
    let mut sets: Vec<RobjPtr> = Vec::with_capacity(client.argc());

    for key in client.argv.iter().skip(1) {
        let set_obj = match db.look_up_key_read(key) {
            None => {
                client.add_reply(shared_object!(EMPTY_MULTI_BULK));
                return;
            }
            Some(o) => {
                if !o.borrow().is_set() {
                    client.add_reply(shared_object!(WRONG_TYPE));
                    return;
                }
                o
            }
        };
        sets.push(set_obj);
    }

    sets.sort_by(|l, r| {
        l.borrow().set_len().cmp(&r.borrow().set_len())
    });

    let obj_ref = sets[0].borrow();
    let iter = obj_ref.set_inter_iter(&sets[1..]);
    let mut cnt: usize = 0;
    let num = Robj::create_string_object("");
    client.add_reply(Rc::clone(&num));

    for r in iter {
        match dst.as_mut() {
            None => add_single_reply(client, r),
            Some(set) => { set.borrow_mut().set_add(r); }
        }
        cnt += 1;
    }

    match dst.as_ref() {
        None => num.borrow_mut().change_to_str(&format!("*{}\r\n", cnt)),
        Some(r) =>
            num.borrow_mut().change_to_str(&format!(":{}\r\n", r.borrow().set_len())),
    }
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
    let o = to_int_if_needed(&client.argv[2]);
    get_command(client, server, el);
    let db = &mut server.db[client.db_idx];
    db.dict.replace(Rc::clone(&client.argv[1]),
                    o);
    let _ = db.remove_expire(&client.argv[1]);
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

pub fn object_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    let sub = client.argv[1].borrow().string().to_ascii_lowercase();
    match &sub[..] {
        b"encoding" => object_encoding_command(client, server, el),
        _ => {
            client.add_str_reply("-Error unknown command\r\n");
        }
    }
}

pub fn object_encoding_command(
    client: &mut Client,
    server: &mut Server,
    el: &mut AeEventLoop,
) {
    if client.argc() != 3 {
        client.add_str_reply("-Error wrong number of arguments\r\n");
        return;
    }

    let db = &mut server.db[client.db_idx];
    let o = db.look_up_key_read(&client.argv[2]);

    let o = match o {
        None => {
            client.add_reply(shared_object!(NULL_BULK));
            return;
        }
        Some(obj) => obj,
    };

    let s = match o.borrow().encoding() {
        RobjEncoding::LinkedList => "linkedlist",
        RobjEncoding::Raw => "raw",
        RobjEncoding::Int => "int",
        RobjEncoding::Ht => "hashtable",
        RobjEncoding::ZipMap => "ziplist",
        RobjEncoding::ZipList => "ziplist",
        RobjEncoding::IntSet => "intset",
        RobjEncoding::SkipList => "skiplist",
        RobjEncoding::EmbStr => "embstr",
    };

    client.add_str_reply(&format!("${}\r\n", s.len()));
    client.add_str_reply(s);
    client.add_reply(shared_object!(CRLF));
}

fn gen_usize_reply(i: usize) -> RobjPtr {
    match i {
        0 => shared_object!(CZERO),
        1 => shared_object!(CONE),
        k => Robj::create_string_object(&format!(":{}\r\n", k)),
    }
}

fn add_single_reply(c: &mut Client, o: RobjPtr) {
    c.add_str_reply(&format!("${}\r\n", o.borrow().string().len()));
    c.add_reply(o);
    c.add_reply(shared_object!(CRLF));
}

fn real_list_index(idx: i64, len: usize) -> i64 {
    if idx >= 0 {
        idx
    } else {
        len as i64 + idx
    }
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
    Command { name: "llen", proc: llen_command, arity: 2, flags: CMD_INLINE },
    Command { name: "lindex", proc: lindex_command, arity: 3, flags: CMD_INLINE },
    Command { name: "lset", proc: lset_command, arity: 4, flags: CMD_INLINE },
    Command { name: "lrange", proc: lrange_command, arity: 4, flags: CMD_INLINE },
    Command { name: "ltrim", proc: ltrim_command, arity: 4, flags: CMD_INLINE },
    Command { name: "lrem", proc: lrem_command, arity: 4, flags: CMD_INLINE },
    Command { name: "sadd", proc: sadd_command, arity: -3, flags: CMD_INLINE },
    Command { name: "srem", proc: srem_command, arity: -3, flags: CMD_INLINE },
    Command { name: "smove", proc: smove_command, arity: 4, flags: CMD_INLINE },
    Command { name: "sismember", proc: sismember_command, arity: 3, flags: CMD_INLINE },
    Command { name: "scard", proc: scard_command, arity: 2, flags: CMD_INLINE },
    Command { name: "spop", proc: spop_command, arity: 2, flags: CMD_INLINE },
    Command { name: "sinter", proc: sinter_command, arity: -2, flags: CMD_INLINE },
    Command { name: "sinterstore", proc: sinterstore_command, arity: -3, flags: CMD_INLINE },
    Command { name: "smembers", proc: smembers_command, arity: 2, flags: CMD_INLINE },
    // TODO
    Command { name: "incrby", proc: incr_by_command, arity: 3, flags: CMD_INLINE },
    Command { name: "decrby", proc: decr_by_command, arity: 3, flags: CMD_INLINE },
    Command { name: "getset", proc: get_set_command, arity: 3, flags: CMD_INLINE },
    // TODO
    Command { name: "select", proc: select_command, arity: 2, flags: CMD_INLINE },
    // TODO
    Command { name: "ping", proc: ping_command, arity: 1, flags: CMD_INLINE },
    // TODO
    Command { name: "object", proc: object_command, arity: -2, flags: CMD_INLINE },
    Command { name: "command", proc: command_command, arity: 1, flags: CMD_INLINE },
];

pub fn lookup_command(name: &[u8]) -> Option<&'static Command> {
    CMD_TABLE.iter()
        .find(|x| case_eq(x.name.as_bytes(), name))
}
