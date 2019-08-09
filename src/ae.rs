use mio::*;
use std::borrow::Borrow;
use std::time::{SystemTime, Duration};
use std::ops::Add;
use std::error::Error;
use std::alloc::System;
use std::rc::Rc;
use mio::net::{TcpListener, TcpStream};
use crate::server::Server;
use std::cell::RefCell;
use crate::client::*;
use std::net::Shutdown::Read;

type AeTimeProc = fn(server: &mut Server, el: &mut AeEventLoop, id: i64, data: &ClientData) -> i32;
type AeFileProc = fn(server: &mut Server, el: &mut AeEventLoop, fd: &Fd, data: &ClientData, mask: i32);
type AeEventFinalizerProc = fn(el: &mut AeEventLoop, data: &ClientData);
pub type Fd = Rc<RefCell<Fdp>>;

pub enum Fdp {
    Listener(TcpListener),
    Stream(TcpStream),
}

impl Fdp {
    pub fn is_listener(&self) -> bool {
        match self {
            Fdp::Listener(_) => true,
            _ => false,
        }
    }

    pub fn is_stream(&self) -> bool {
        match self {
            Fdp::Stream(_) => true,
            _ => false,
        }
    }

    pub fn to_evented(&self) -> &dyn Evented {
        match self {
            Fdp::Stream(s) => s,
            Fdp::Listener(l) => l,
        }
    }

    pub fn unwrap_listener(&self) -> &TcpListener {
        match self {
            Fdp::Listener(l) => l,
            _ => panic!("not a listener"),
        }
    }

    pub fn unwrap_stream(&self) -> &TcpStream {
        match self {
            Fdp::Stream(s) => s,
            _ => panic!("not a stream"),
        }
    }

    pub fn unwrap_stream_mut(&mut self) -> &mut TcpStream {
        match self {
            Fdp::Stream(s) => s,
            _ => panic!("not a stream"),
        }
    }
}

fn default_ae_time_proc(server: &mut Server, el: &mut AeEventLoop, id: i64, data: &ClientData) -> i32 { 1 }

fn default_ae_file_proc(server: &mut Server, el: &mut AeEventLoop, token: Token, data: &ClientData, mask: i32) {}

pub fn default_ae_event_finalizer_proc(el: &mut AeEventLoop, data: &ClientData) {}

pub struct AeEventLoop {
    time_event_next_id: i64,
    file_events: Vec<Option<AeFileEvent>>,
    file_events_num: usize,
    occupied: Option<usize>,
    time_event_head: Option<Box<AeTimeEvent>>,
    poll: Poll,
    stop: bool,
}

impl AeEventLoop {
    pub fn new(n: usize) -> AeEventLoop {
        let mut el = AeEventLoop {
            time_event_next_id: 0,
            file_events: Vec::with_capacity(n),
            file_events_num: 0,
            occupied: None,
            time_event_head: None,
            poll: Poll::new().unwrap(),
            stop: false,
        };
        for i in 0..n {
            el.file_events.push(None);
        }
        el
    }

    pub fn deregister_stream(&mut self, stream: &TcpStream) {
        self.poll.deregister(stream).unwrap();
    }

    pub fn stop(&mut self) {
        self.stop = true;
    }

    pub fn readiness(mask: i32) -> Ready {
        let mut ready = Ready::empty();
        if mask & AE_READABLE != 0 {
            ready |= Ready::readable();
        }
        if mask & AE_WRITABLE != 0 {
            ready |= Ready::writable();
        }
        ready
    }

    pub fn create_file_event(
        &mut self,
        fd: Fd,
        mask: i32,
        file_proc: AeFileProc,
        client_data: ClientData,
        finalizer_proc: AeEventFinalizerProc,
    ) -> Result<(), ()> {
        let mut fe = AeFileEvent {
            fd,
            mask,
            file_proc,
            finalizer_proc,
            client_data,
        };

        for i in 0..self.file_events.len() {
            if self.file_events[i].is_none() &&
                self.occupied.map(|x| x != i).unwrap_or(true) {
                self.poll.register(
                    fe.fd.as_ref().borrow().to_evented(),
                    Token(i),
                    Self::readiness(mask),
                    PollOpt::edge(),
                ).unwrap();
                self.file_events[i] = Some(fe);
                self.file_events_num += 1;
                return Ok(());
            }
        }

        Err(())
    }

    fn occupy_file_event(&mut self, i: usize) -> AeFileEvent {
        self.occupied = Some(i);
        self.file_events[i].take().unwrap()
    }

    fn un_occupy_file_event(&mut self, i: usize, fe: AeFileEvent) {
        if let Some(n) = self.occupied {
            assert_eq!(n, i);
            self.file_events[i] = Some(fe);
        } else {
            self.file_events_num -= 1;
        }
    }

    pub fn try_delete_occupied(&mut self) {
        assert!(self.occupied.is_some());
        self.occupied = None;
    }

    pub fn delete_file_event(&mut self, fd: &Fd, mask: i32) {
        let mut del: Option<usize> = None;
        for i in 0..self.file_events.len() {
            let p = self.file_events[i]
                .as_ref()
                .map(|x| (&x.fd, x.mask));
            if let Some(p) = p {
                if Rc::ptr_eq(p.0, fd) && mask == p.1 {
                    del = Some(i);
                    break;
                }
            }
        }
        if let Some(i) = del {
            self.file_events[i] = None;
            self.file_events_num -= 1;
        }
    }

    fn create_time_event(
        &mut self,
        duration: Duration,
        time_proc: AeTimeProc,
        client_data: ClientData,
        finalizer_proc: AeEventFinalizerProc,
    ) -> i64 {
        let id = self.time_event_next_id;
        self.time_event_next_id += 1;
        let mut te = AeTimeEvent {
            id,
            when: SystemTime::now().add(duration),
            time_proc,
            client_data,
            finalizer_proc,
            next: None,
        };
        te.next = self.time_event_head.take();
        self.time_event_head = Some(Box::new(te));
        id
    }

    fn delete_time_event(&mut self, id: i64) {
        let mut prev: Option<&mut Box<AeTimeEvent>> = None;
        let mut fe = self.time_event_head.take();

        while let Some(mut e) = fe {
            if e.id == id {
                match prev {
                    None => self.time_event_head = e.next.take(),
                    Some(k) => k.next = e.next.take(),
                }
                e.finalizer_proc.borrow()(self, &e.client_data);
                break;
            }
            fe = e.next.take();
            match prev {
                None => {
                    self.time_event_head = Some(e);
                    prev = self.time_event_head.as_mut();
                }
                Some(k) => {
                    k.next = Some(e);
                    prev = k.next.as_mut();
                }
            }
        }
    }

    fn search_nearest_timer(&self) -> Option<&Box<AeTimeEvent>> {
        let mut te = self.time_event_head.as_ref();
        let mut nearest: Option<&Box<AeTimeEvent>> = None;

        while let Some(e) = te {
            if nearest.is_none() || e.when < nearest.unwrap().when {
                nearest = Some(e);
            }
            te = e.next.as_ref();
        }

        nearest
    }

    fn process_events(&mut self, flags: i32, server: &mut Server) -> Result<usize, Box<dyn Error>> {
        let mut processed: usize = 0;
        // nothing to do, return ASAP
        if (flags & AE_TIME_EVENTS == 0) && (flags & AE_FILE_EVENTS) == 0 {
            return Ok(0);
        }

        let poll = &self.poll;

        if self.file_events.len() > 0 ||
            ((flags & AE_TIME_EVENTS != 0) && (flags & AE_DONT_WAIT == 0)) {
            let mut wait: Option<Duration> = None;
            let mut shortest: Option<SystemTime> = None;
            if (flags & AE_TIME_EVENTS != 0) && (flags & AE_DONT_WAIT == 0) {
                let te = self.search_nearest_timer();
                if let Some(te) = te {
                    shortest = Some(te.when.clone());
                }
            }

            if let Some(shortest) = shortest {
                let curr = SystemTime::now();
                if curr > shortest {
                    wait = Some(Duration::from_secs(0));
                } else {
                    wait = Some(shortest.duration_since(curr).unwrap());
                }
            }

            let mut events = Events::with_capacity(self.file_events_num + 1);
            let event_num = poll.poll(&mut events, wait)?;
            for event in &events {
                let t = event.token();

                let fe = self.occupy_file_event(t.0);
                fe.file_proc.borrow()(server, self, &fe.fd, &fe.client_data, fe.mask);
                self.un_occupy_file_event(t.0, fe);

                processed += 1;
            }
        }

        if flags & AE_TIME_EVENTS != 0 {
            processed += self.process_time_events(server);
        }

        Ok(processed)
    }

    fn process_time_events(&mut self, server: &mut Server) -> usize {
//        unimplemented!()
        0
    }

    pub fn main(&mut self, server: &mut Server) {
        self.stop = false;
        while !self.stop {
            let r = self.process_events(AE_ALL_EVENTS, server);
            if let Err(e) = r {
                debug!("Processing events: {}", e.description());
            }
        }
    }
}

struct AeFileEvent {
    fd: Fd,
    mask: i32,
    file_proc: AeFileProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: ClientData,
}

struct AeTimeEvent {
    id: i64,
    when: SystemTime,
    time_proc: AeTimeProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: ClientData,
    next: Option<Box<AeTimeEvent>>,
}


fn ae_wait(fd: &Fd, mask: i32, duration: Duration) -> Result<i32, Box<dyn Error>> {
    let poll = Poll::new()?;
    let mut ready: Ready = Ready::empty();
    let ret_mask: i32 = 0;
    if mask & AE_READABLE != 0 {
        ready |= Ready::readable();
    }
    if mask & AE_WRITABLE != 0 {
        ready |= Ready::writable();
    }
    // TODO: exception?

    poll.register(fd.as_ref().borrow().to_evented(), Token(0), ready, PollOpt::edge())?;
    let mut events = Events::with_capacity(1);

    poll.poll(&mut events, Some(duration))?;

    for event in &events {
        if event.readiness().is_readable() {
            return Ok(AE_READABLE);
        } else if event.readiness().is_writable() {
            return Ok(AE_WRITABLE);
        }
    }
    unreachable!()
}


pub const AE_READABLE: i32 = 0b0001;
pub const AE_WRITABLE: i32 = 0b0010;
pub const AE_EXCEPTION: i32 = 0b0100;

pub const AE_FILE_EVENTS: i32 = 0b0001;
pub const AE_TIME_EVENTS: i32 = 0b0010;
pub const AE_ALL_EVENTS: i32 = AE_FILE_EVENTS | AE_TIME_EVENTS;
pub const AE_DONT_WAIT: i32 = 0b0100;


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_event_loop() {
        let el = AeEventLoop::new(1024);
        assert_eq!(el.file_events.len(), 1024);
        assert!(el.time_event_head.is_none());
    }

//    #[test]
//    fn create_file_events() {
//        let mut el = AeEventLoop::new();
//        for i in 0..100 {
//            el.create_file_event(
//                Token(i),
//                i as i32,
//                default_ae_file_proc,
//                Box::new(3i32),
//                default_ae_event_finalizer_proc,
//            );
//            assert_eq!(el.file_event_head.as_mut().unwrap().mask, i as i32);
//        }
//        for i in (1..100).rev() {
//            el.delete_file_event(Token(i), i as i32);
//            assert_eq!(el.file_event_head.as_mut().unwrap().mask, (i - 1) as i32);
//        }
//    }

    #[test]
    fn create_time_event() {
        let mut el = AeEventLoop::new(1204);
        for i in 0..100 {
            el.create_time_event(
                Duration::from_millis(500),
                default_ae_time_proc,
                ClientData::Nil(),
                default_ae_event_finalizer_proc,
            );
            assert_eq!(el.time_event_head.as_ref().unwrap().id, i);
        }
        for i in (1..100).rev() {
            el.delete_time_event(i);
            assert_eq!(el.time_event_head.as_ref().unwrap().id, i - 1);
        }
    }

    #[test]
    fn find_nearest() {
        let mut el = AeEventLoop::new(1204);
        for i in 0..100 {
            el.create_time_event(
                Duration::from_millis(500),
                default_ae_time_proc,
                ClientData::Nil(),
                default_ae_event_finalizer_proc,
            );
            assert_eq!(el.time_event_head.as_ref().unwrap().id, i);
        }
        let n = el.search_nearest_timer().unwrap();
        assert_eq!(n.id, 0);
        el.create_time_event(
            Duration::from_millis(500),
            default_ae_time_proc,
            ClientData::Nil(),
            default_ae_event_finalizer_proc,
        );
    }
}