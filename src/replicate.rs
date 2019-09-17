use crate::client::Client;
use crate::object::linked_list::LinkedList;
use std::rc::Rc;
use std::cell::RefCell;
use crate::object::{RobjPtr, Robj};
use crate::shared::CRLF;

pub fn feed_slaves(
    this_client: &mut Client,
    slaves: &LinkedList<Rc<RefCell<Client>>>,
    db_idx: usize,
) {
    let mut outv: Vec<RobjPtr> = Vec::with_capacity(3 * this_client.argv.len() + 1);
    outv.push(
        Robj::from_bytes(format!("*{}\r\n", this_client.argv.len()).into_bytes())
    );
    for obj in this_client.argv.iter() {
        outv.push(
            Robj::from_bytes(
                format!("${}\r\n", obj.borrow().string_len()).into_bytes()
            )
        );
        outv.push(Rc::clone(obj));
        outv.push(shared_object!(CRLF));
    }

    for slave in slaves.iter() {
        if slave.as_ptr() == this_client as *mut Client {
            feed_one_slave(this_client, db_idx, &outv);
        } else {
            let mut slave = slave.borrow_mut();
            feed_one_slave(&mut slave, db_idx, &outv);
        }
    }
}

fn feed_one_slave(
    slave: &mut Client,
    db_idx: usize,
    outv: &Vec<RobjPtr>,
) {
    if slave.slave_select_db != db_idx {
        slave.slave_select_db = db_idx;
        feed_select_db_command(slave);
    }
    slave.reply.reserve(outv.len());
    slave.reply.extend_from_slice(&outv[..]);
}

fn feed_select_db_command(slave: &mut Client) {
    let idx = slave.slave_select_db;
    slave.add_reply(
        Robj::from_bytes(format!("*{}\r\n", 2).into_bytes())
    );
    slave.add_reply(Robj::create_string_object("$6\r\nselect\r\n"));
    if idx < 10 {
        slave.add_reply(
            Robj::from_bytes(format!("${}\r\n{}\r\n", 1, idx).into_bytes())
        )
    } else if idx < 100 {
        slave.add_reply(
            Robj::from_bytes(format!("${}\r\n{}\r\n", 2, idx).into_bytes())
        )
    } else {
        unreachable!()
    }
}

