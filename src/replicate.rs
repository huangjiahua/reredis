use crate::ae::{AeEventLoop, AE_READABLE, AE_WRITABLE};
use crate::client::{Client, ClientData, ReplyState};
use crate::env::{send_bulk_to_slave, send_reply_to_client};
use crate::object::linked_list::LinkedList;
use crate::object::{Robj, RobjPtr};
use crate::rdb::rdb_save_in_background;
use crate::server::Server;
use crate::shared::CRLF;
use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;

pub fn feed_slaves(
    el: &mut AeEventLoop,
    this_client: &mut Client,
    slaves: &LinkedList<Rc<RefCell<Client>>>,
    db_idx: usize,
) {
    let mut outv: Vec<RobjPtr> = Vec::with_capacity(3 * this_client.argv.len() + 1);
    outv.push(Robj::from_bytes(
        format!("*{}\r\n", this_client.argv.len()).into_bytes(),
    ));
    for obj in this_client.argv.iter() {
        outv.push(Robj::from_bytes(
            format!("${}\r\n", obj.borrow().string_len()).into_bytes(),
        ));
        outv.push(Rc::clone(obj));
        outv.push(shared_object!(CRLF));
    }

    for slave in slaves.iter() {
        if slave.as_ptr() == this_client as *mut Client {
            feed_one_slave(this_client, db_idx, &outv);
        } else {
            let mut slave_ref = slave.borrow_mut();
            feed_one_slave(&mut slave_ref, db_idx, &outv);
            let _ = el.create_file_event(
                Rc::clone(&slave_ref.fd),
                AE_WRITABLE,
                send_reply_to_client,
                ClientData::Client(Rc::clone(slave)),
            );
        }
    }
}

fn feed_one_slave(slave: &mut Client, db_idx: usize, outv: &Vec<RobjPtr>) {
    if slave.slave_select_db != db_idx {
        slave.slave_select_db = db_idx;
        feed_select_db_command(slave);
    }
    slave.reply.reserve(outv.len());
    slave.reply.extend_from_slice(&outv[..]);
}

fn feed_select_db_command(slave: &mut Client) {
    let idx = slave.slave_select_db;
    slave.add_reply(Robj::from_bytes(format!("*{}\r\n", 2).into_bytes()));
    slave.add_reply(Robj::create_string_object("$6\r\nselect\r\n"));
    if idx < 10 {
        slave.add_reply(Robj::from_bytes(
            format!("${}\r\n{}\r\n", 1, idx).into_bytes(),
        ))
    } else if idx < 100 {
        slave.add_reply(Robj::from_bytes(
            format!("${}\r\n{}\r\n", 2, idx).into_bytes(),
        ))
    } else {
        unreachable!()
    }
}

pub fn update_slaves_waiting_bgsave(server: &mut Server, el: &mut AeEventLoop, ok: bool) {
    let mut start_bgsave: bool = false;
    let mut freed_clients: Vec<Rc<RefCell<Client>>> = vec![];
    for slave_ptr in server.slaves.iter() {
        let mut slave = slave_ptr.borrow_mut();
        match slave.reply_state {
            ReplyState::WaitBgSaveStart => {
                start_bgsave = true;
                slave.reply_state = ReplyState::WaitBgSaveEnd;
            }
            ReplyState::WaitBgSaveEnd => {
                if !ok {
                    freed_clients.push(Rc::clone(slave_ptr));
                    warn!("SYNC failed. BGSAVE child returned an error");
                    continue;
                }
                let file = match File::open(&server.db_filename) {
                    Ok(f) => f,
                    Err(err) => {
                        warn!("SYNC failed. Can't open/stat DB after BGSAVE: {}", err);
                        continue;
                    }
                };
                let file_size = match file.metadata() {
                    Ok(s) => s.len(),
                    Err(err) => {
                        warn!("SYNC failed. Can't open/stat DB after BGSAVE: {}", err);
                        continue;
                    }
                };
                slave.reply_db_off = 0;
                slave.reply_db_size = file_size;
                slave.reply_db_file = Some(file);
                slave.reply_state = ReplyState::SendBulk;
                el.delete_file_event(&slave.fd, AE_WRITABLE);
                //                el.deregister_stream(slave.fd.borrow().unwrap_stream());
                if let Err(_) = el.create_file_event(
                    Rc::clone(&slave.fd),
                    AE_WRITABLE,
                    send_bulk_to_slave,
                    ClientData::Client(Rc::clone(slave_ptr)),
                ) {
                    freed_clients.push(Rc::clone(slave_ptr));
                    continue;
                }
            }
            _ => {}
        }
    }
    if start_bgsave {
        if let Err(_) = rdb_save_in_background(server) {
            warn!("SYNC failed. BGSAVE failed");
            for slave_ptr in server.slaves.iter() {
                freed_clients.push(Rc::clone(slave_ptr));
            }
        }
    }
    for slave_ptr in freed_clients.iter() {
        server.free_client(slave_ptr);
        el.delete_file_event(&slave_ptr.borrow().fd, AE_WRITABLE);
        el.delete_file_event(&slave_ptr.borrow().fd, AE_WRITABLE | AE_READABLE);
        el.deregister_stream(slave_ptr.borrow().fd.borrow().unwrap_stream());
    }
}
