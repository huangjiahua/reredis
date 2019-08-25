use mio::*;
use std::time::{SystemTime, Duration};
use std::ops::Add;
use std::error::Error;
use std::rc::Rc;
use mio::net::{TcpListener, TcpStream};
use crate::server::Server;
use std::cell::RefCell;
use crate::client::*;
use std::collections::VecDeque;

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

fn _default_ae_time_proc(_server: &mut Server, _el: &mut AeEventLoop, _id: i64,
                         _data: &ClientData) -> i32 { 1 }

pub fn default_ae_file_proc(_server: &mut Server, _el: &mut AeEventLoop,
                            _fd: &Fd, _data: &ClientData, _mask: i32) {}

pub fn default_ae_event_finalizer_proc(_el: &mut AeEventLoop, _data: &ClientData) {}

pub struct AeEventLoop {
    time_event_next_id: i64,
    file_events: Vec<Option<AeFileEvent>>,
    file_events_num: usize,
    occupied: Option<usize>,
    time_events: VecDeque<AeTimeEvent>,
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
            time_events: VecDeque::new(),
            poll: Poll::new().unwrap(),
            stop: false,
        };
        for _ in 0..n {
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
        w_file_proc: AeFileProc,
        client_data: ClientData,
        finalizer_proc: AeEventFinalizerProc,
    ) -> Result<(), ()> {
        let fe = AeFileEvent {
            fd,
            mask,
            r_file_proc: file_proc,
            w_file_proc,
            finalizer_proc,
            client_data,
        };

        for i in 0..self.file_events.len() {
            if self.file_events[i].is_none() &&
                self.occupied.map(|x| x != i).unwrap_or(true) {
                self.poll.register(
                    fe.fd.borrow().to_evented(),
                    Token(i),
                    Self::readiness(mask),
                    PollOpt::level(),
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

    pub fn create_time_event(
        &mut self,
        duration: Duration,
        time_proc: AeTimeProc,
        client_data: ClientData,
        finalizer_proc: AeEventFinalizerProc,
    ) -> i64 {
        let id = self.time_event_next_id;
        self.time_event_next_id += 1;
        let te = AeTimeEvent {
            id,
            when: SystemTime::now().add(duration),
            time_proc,
            client_data,
            finalizer_proc,
        };
        self.time_events.push_back(te);
        id
    }

    fn delete_time_event(&mut self, id: i64) {
        for i in 0..self.time_events.len() {
            if self.time_events[i].id == id {
                self.time_events.remove(i);
            }
        }
    }

    fn search_nearest_timer(&self) -> Option<&AeTimeEvent> {
        let mut nearest: Option<&AeTimeEvent> = None;

        for te in &self.time_events {
            nearest = match nearest {
                None => Some(te),
                Some(e) => {
                    if te.when < e.when {
                        Some(te)
                    } else {
                        Some(e)
                    }
                }
            }
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
            let _ = poll.poll(&mut events, wait)?;
            for event in &events {
                let t = event.token();

                let fe = self.occupy_file_event(t.0);
                let mut r_fired = false;

                if (fe.mask & AE_READABLE != 0) && event.readiness().is_readable() {
                    r_fired = true;
                    (&fe.r_file_proc)(server, self, &fe.fd, &fe.client_data, fe.mask);
                }
                if (fe.mask & AE_WRITABLE != 0) && event.readiness().is_writable() {
                    if !r_fired {
                        (&fe.w_file_proc)(server, self, &fe.fd, &fe.client_data, fe.mask);
                    }
                }

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
        let num = self.time_events.len();
        for _ in 0..num {
            assert!(!self.time_events.is_empty());
            let curr = SystemTime::now();
            let mut te = self.time_events.pop_front().unwrap();
            if curr > te.when {
                let id = te.id;
                let retval
                    = (&te.time_proc)(server, self, id, &te.client_data);
                if retval != -1 {
                    te.when = te.when.add(Duration::from_millis(retval as u64));
                }
            }
            self.time_events.push_back(te);
        }
        1
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
    r_file_proc: AeFileProc,
    w_file_proc: AeFileProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: ClientData,
}

struct AeTimeEvent {
    id: i64,
    when: SystemTime,
    time_proc: AeTimeProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: ClientData,
}


fn _ae_wait(fd: &Fd, mask: i32, duration: Duration) -> Result<i32, Box<dyn Error>> {
    let poll = Poll::new()?;
    let mut ready: Ready = Ready::empty();
    if mask & AE_READABLE != 0 {
        ready |= Ready::readable();
    }
    if mask & AE_WRITABLE != 0 {
        ready |= Ready::writable();
    }
    // TODO: exception?

    poll.register(fd.borrow().to_evented(), Token(0), ready, PollOpt::edge())?;
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
pub const AE_ALREADY_REGISTER: i32 = 0b1000;

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
        assert_eq!(el.time_events.len(), 0);
    }
}