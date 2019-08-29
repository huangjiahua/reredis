#[allow(dead_code)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate redis;

use reredis::env::*;
use reredis::oom::oom;
use reredis::zalloc::Zalloc;
use threadpool::ThreadPool;
use std::thread::sleep;
use std::time::Duration;
use std::io::Write;
use std::sync::mpsc::{Sender, channel};
use redis::Commands;

type S = Sender<Option<String>>;

#[global_allocator]
static A: Zalloc = Zalloc;

fn level_to_character(level: log::Level) -> &'static str {
    match level {
        log::Level::Error => "CLIENT",
        _ => "SERVER",
    }
}

fn init_logger() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Debug);
    builder.format(
        |buf, record|
            writeln!(
                buf,
                "{}: {}",
                level_to_character(record.level()),
                record.args(),
            )
    );
    builder.init();
}

#[test]
#[ignore]
fn test_main() {
    init_logger();
    let pool = ThreadPool::new(4);
    pool.execute(|| {
        let config = Config::new();
        let mut env = Env::new(&config);
        env.init_server();
        if let Err(_) = env.create_first_file_event() {
            oom("creating file event");
        }
        env.ae_main();
    });

    sleep(Duration::from_secs(2));

    let (s, _r) = channel::<Option<String>>();

    for f in TEST_FUNCTION {
        let sender = s.clone();
        pool.execute(move || {
            (&f)(sender);
        });
    }

    sleep(Duration::from_secs(3));
    pool.execute(shutdown);
    info!("All test passed!");
    pool.join();
}

type TestFunc = fn(sender: S);

const TEST_FUNCTION: &[TestFunc] = &[
    ping_test,
    set_test,
];

fn ping_test(sender: S) {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_connection().unwrap();
    let r: String = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(r, "PONG");
    error!("PING OK");
    sender.send(None).unwrap();
}

fn set_test(sender: S) {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    error!("Simple set command test");
    let mut con = client.get_connection().unwrap();
    let r: String = con.set("a", "b").unwrap();
    assert_eq!(r, "OK");
    sender.send(None).unwrap();
}

fn shutdown() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_connection().unwrap();
    error!("Send shutdown command");
    let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").query(&mut con);
}


