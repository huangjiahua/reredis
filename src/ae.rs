use mio::*;
use std::borrow::Borrow;
use std::time::{SystemTime, Duration};
use std::ops::Add;
use std::error::Error;
use std::net::Shutdown::Read;

type AeTimeProc = fn(el: &mut AeEventLoop, id: i64, data: &Box<dyn ClientData>) -> i32;
type AeFileProc = fn(el: &mut AeEventLoop, token: Token, data: &Box<dyn ClientData>, mask: i32);
type AeEventFinalizerProc = fn(el: &mut AeEventLoop, data: &Box<dyn ClientData>);
type Fd = Box<dyn Evented>;

fn default_ae_time_proc(el: &mut AeEventLoop, id: i64, data: &Box<dyn ClientData>) -> i32 { 1 }

fn default_ae_file_proc(el: &mut AeEventLoop, token: Token, data: &Box<dyn ClientData>, mask: i32) {}

fn default_ae_event_finalizer_proc(el: &mut AeEventLoop, data: &Box<dyn ClientData>) {}

struct AeEventLoop {
    time_event_next_id: i64,
    file_event_head: Option<Box<AeFileEvent>>,
    time_event_head: Option<Box<AeTimeEvent>>,
    stop: bool,
}

impl AeEventLoop {
    fn new() -> AeEventLoop {
        AeEventLoop {
            time_event_next_id: 0,
            file_event_head: None,
            time_event_head: None,
            stop: false,
        }
    }

    fn stop(&mut self) {
        self.stop = true;
    }

    fn create_file_event(
        &mut self,
        fd: Fd,
        mask: i32,
        file_proc: AeFileProc,
        client_data: Box<dyn ClientData>,
        finalizer_proc: AeEventFinalizerProc,
    ) {
        let mut fe = AeFileEvent {
            fd,
            mask,
            file_proc,
            finalizer_proc,
            client_data,
            next: None,
        };

        fe.next = self.file_event_head.take();
        self.file_event_head = Some(Box::new(fe));
    }

    fn delete_file_event(&mut self, fd: &Fd, mask: i32) {
        let mut prev: Option<&mut Box<AeFileEvent>> = None;
        let mut fe = self.file_event_head.take();

        while let Some(mut e) = fe {
            if e.mask == mask && eq(&e.fd, fd) {
                match prev {
                    None => self.file_event_head = e.next.take(),
                    Some(k) => k.next = e.next.take(),
                }
                e.finalizer_proc.borrow()(self, &e.client_data);
                break;
            }
            fe = e.next.take();
            match prev {
                None => {
                    self.file_event_head = Some(e);
                    prev = self.file_event_head.as_mut();
                }
                Some(k) => {
                    k.next = Some(e);
                    prev = k.next.as_mut();
                }
            }
        }
    }

    fn create_time_event(
        &mut self,
        duration: Duration,
        time_proc: AeTimeProc,
        client_data: Box<dyn ClientData>,
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

    fn process_events(&mut self, flags: i32) -> Result<i32, Box<dyn Error>> {
        let mut token: usize = 0;
        // nothing to do, return ASAP
        if (flags & AE_TIME_EVENTS == 0) && (flags & AE_FILE_EVENTS) == 0 {
            return Ok(0);
        }

        let poll = Poll::new()?;

        let mut fe = self.file_event_head.as_ref();
        if flags & AE_ALL_EVENTS != 0 {
            while let Some(e) = fe {
                let mut ready = Ready::empty();
                if e.mask & AE_READABLE != 0 {
                    ready |= Ready::readable();
                }
                if e.mask & AE_WRITABLE != 0 {
                    ready |= Ready::writable();
                }
                poll.register(
                    &e.fd,
                    Token(token),
                    ready,
                    PollOpt::edge(),
                )?;
                token += 1;
            }
        }

        if token != 0 || ((flags & AE_TIME_EVENTS != 0) && (flags & AE_DONT_WAIT == 0)) {
            let mut shortest: Option<&Box<AeTimeEvent>> = None;
            let mut duration: Option<Duration> = None;

            if (flags & AE_TIME_EVENTS != 0) && (flags & AE_DONT_WAIT == 0) {
                shortest = self.search_nearest_timer();
            }

            if let Some(shortest) = shortest {
                let curr = SystemTime::now();
                duration = Some(shortest.when.duration_since(curr)?);
            }  // else the duration is None and poll won't wait

            let mut events = Events::with_capacity(token + 1);
            let n: usize = poll.poll(&mut events, duration)?;

            // TODO
        }
        Ok(1)
    }
}

struct AeFileEvent {
    fd: Fd,
    mask: i32,
    file_proc: AeFileProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: Box<dyn ClientData>,
    next: Option<Box<AeFileEvent>>,
}

struct AeTimeEvent {
    id: i64,
    when: SystemTime,
    time_proc: AeTimeProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: Box<dyn ClientData>,
    next: Option<Box<AeTimeEvent>>,
}

fn eq<T: ?Sized>(left: &Box<T>, right: &Box<T>) -> bool {
    let left: *const T = left.as_ref();
    let right: *const T = right.as_ref();
    left == right
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

    poll.register(fd, Token(0), ready, PollOpt::edge())?;
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

trait ClientData {}

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

    impl ClientData for i32 {}

    #[test]
    fn new_event_loop() {
        let el = AeEventLoop::new();
        assert!(el.file_event_head.is_none());
        assert!(el.time_event_head.is_none());
    }

    #[test]
    fn create_file_events() {
        let mut el = AeEventLoop::new();
        for i in 0..100 {
            el.create_file_event(
                Token(i),
                i as i32,
                default_ae_file_proc,
                Box::new(3i32),
                default_ae_event_finalizer_proc,
            );
            assert_eq!(el.file_event_head.as_mut().unwrap().mask, i as i32);
        }
        for i in (1..100).rev() {
            el.delete_file_event(Token(i), i as i32);
            assert_eq!(el.file_event_head.as_mut().unwrap().mask, (i - 1) as i32);
        }
    }

    #[test]
    fn create_time_event() {
        let mut el = AeEventLoop::new();
        for i in 0..100 {
            el.create_time_event(
                Duration::from_millis(500),
                default_ae_time_proc,
                Box::new(1),
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
        let mut el = AeEventLoop::new();
        for i in 0..100 {
            el.create_time_event(
                Duration::from_millis(500),
                default_ae_time_proc,
                Box::new(1),
                default_ae_event_finalizer_proc,
            );
            assert_eq!(el.time_event_head.as_ref().unwrap().id, i);
        }
        let n = el.search_nearest_timer().unwrap();
        assert_eq!(n.id, 0);
        el.create_time_event(
            Duration::from_millis(500),
            default_ae_time_proc,
            Box::new(1),
            default_ae_event_finalizer_proc,
        );
    }
}