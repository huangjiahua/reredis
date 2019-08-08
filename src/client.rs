use crate::ae::{Fd, AeEventLoop, AE_READABLE, default_ae_event_finalizer_proc, ClientData};
use std::time::SystemTime;
use std::rc::Rc;
use crate::env::read_query_from_client;
use std::cell::RefCell;

pub struct Client {
    pub fd: Fd,
    pub dict_id: usize,
    pub query_buf: Vec<u8>,
    pub last_interaction: SystemTime,
}

impl Client {
    pub fn with_fd(fd: Fd, el: &mut AeEventLoop) -> Result<Rc<RefCell<Client>>, ()> {
        let client = Rc::new(RefCell::new(Client {
            fd,
            dict_id: 0,
            query_buf: vec![],
            last_interaction: SystemTime::now(),
        }));
        el.create_file_event(
            Rc::clone(&client.borrow().fd),
            AE_READABLE,
            read_query_from_client,
            ClientData::Client(Rc::clone(&client)),
            default_ae_event_finalizer_proc,
        )?;

        Ok(client)
    }
}


