use mio::*;
use std::borrow::Borrow;

type AeTimeProc = fn(el: &mut AeEventLoop, id: i64, data: Box<dyn ClientData>) -> i32;
type AeFileProc = fn(el: &mut AeEventLoop, token: Token, data: Box<dyn ClientData>, mask: i32);
type AeEventFinalizerProc = fn(el: &mut AeEventLoop, data: Box<dyn ClientData>);


fn default_ae_time_proc(el: &mut AeEventLoop, id: i64, data: Box<dyn ClientData>) -> i32 { 1 }

fn default_ae_file_proc(el: &mut AeEventLoop, token: Token, data: Box<dyn ClientData>, mask: i32) {}

fn default_ae_event_finalizer_proc(el: &mut AeEventLoop, data: Box<dyn ClientData>) {}

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
        token: Token,
        mask: i32,
        file_proc: AeFileProc,
        client_data: Box<dyn ClientData>,
        finalizer_proc: AeEventFinalizerProc,
    ) {
        let mut fe = AeFileEvent {
            token,
            mask,
            file_proc,
            finalizer_proc,
            client_data,
            next: None,
        };

        fe.next = self.file_event_head.take();
        self.file_event_head = Some(Box::new(fe));
    }

    fn delete_file_event(&mut self, token: Token, mask: i32) {
        let mut prev: Option<&mut Box<AeFileEvent>> = None;
        let mut fe = self.file_event_head.take();

        while let Some(mut e) = fe {
            if e.mask == mask && e.token == token {
                match prev {
                    None => self.file_event_head = e.next.take(),
                    Some(k) => k.next = e.next.take(),
                }
                e.finalizer_proc.borrow()(self, e.client_data);
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
}

struct AeFileEvent {
    token: Token,
    mask: i32,
    file_proc: AeFileProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: Box<dyn ClientData>,
    next: Option<Box<AeFileEvent>>,
}

struct AeTimeEvent {
    id: i64,
    when_sec: i64,
    when_ms: i64,
    time_proc: AeTimeProc,
    finalizer_proc: AeEventFinalizerProc,
    client_data: Box<dyn ClientData>,
    next: Option<Box<AeTimeEvent>>,
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
}