#![allow(unused)]
#![allow(dead_code)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate redis;

#[global_allocator]
static A: Zalloc = Zalloc;

mod common;

use reredis::env::*;
use reredis::oom::oom;
use reredis::zalloc::Zalloc;
use threadpool::ThreadPool;
use std::thread::sleep;
use std::time::Duration;
use std::sync::mpsc;
use std::thread;
use std::fmt;
use std::io::Write;
use common::*;
use std::error::Error;
use redis::Commands;

const ADDR: &str = "redis://127.0.0.1/";

trait TestInputData {}

impl TestInputData for () {}

type TestResult = Result<(), Box<dyn Error>>;

type TestFunction = fn(input: Box<dyn TestInputData>) -> TestResult;

#[derive(Copy, Clone)]
struct TestCase {
    name: &'static str,
    func: TestFunction,
}

#[derive(Debug)]
struct ReturnError {
    expected: String,
    real: String,
}

impl fmt::Display for ReturnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Expected: {}\n\
                   Real:     {}", self.expected, self.real)
    }
}

impl Error for ReturnError {}

#[test]
#[ignore]
fn test_main() {
    test_init_logger();
    let pool = ThreadPool::new(4);

    // start running reredis server in a separated thread
    let handle: thread::JoinHandle<()> = thread::spawn(|| {
        let config = Config::new();
        let mut env = Env::new(&config);
        env.init_server();
        if let Err(_) = env.create_first_file_event() {
            oom("creating file event");
        }
        env.ae_main();
    });

    // waiting for the server to start
    sleep(Duration::from_secs(2));
    let (sender, receiver) = mpsc::channel::<String>();

    // start running tests concurrently
    for test in TEST_CASES.iter().cloned() {
        let s = sender.clone();

        pool.execute(move || {
            let r =
                (test.func)(Box::new(()));

            if let Err(e) = r {
                // send error to main thread
                s.send(format!("\n{}: \n{}\n", test.name, e.to_string()));
            } else {
                error!("{} .. ok", test.name);
            }
        });

        if let Ok(err) = receiver.try_recv() {
            panic!("{}", err);
        }
    }


    pool.join();
    eprintln!("ADMIN: wait for error for 3 seconds");
    if let Ok(err) = receiver.recv_timeout(Duration::from_secs(3)) {
        panic!("{}", err);
    }
    eprintln!("ADMIN: no error detected, all tests are passed");

    pool.execute(shutdown);
    let _ = handle.join();
}

const TEST_CASES: &'static [TestCase] = &[
    TestCase { name: "ping server", func: test_ping },
    TestCase { name: "simple set and get", func: test_simple_set_and_get },
];

fn test_ping(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to ping");
    let mut con = establish()?;
    let ret: String = redis::cmd("PING").query(&mut con)?;
    compare("PONG".to_string(), ret)
}

fn test_simple_set_and_get(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to set");
    let mut con = establish()?;
    let ret: String = con.set("_key", "_value")?;
    compare("OK".to_string(), ret)?;

    let ret: String = redis::cmd("GET").arg("_key").query(&mut con)?;
    compare("_value".to_string(), ret)?;
    Ok(())
}

fn shutdown() {
    let mut con = establish().unwrap();
    error!("Send shutdown command");
    let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").query(&mut con);
}

fn establish() -> Result<redis::Connection, Box<dyn Error>> {
    let client = redis::Client::open(ADDR)?;
    let con = client.get_connection()?;
    Ok(con)
}

fn compare(expected: String, real: String) -> TestResult {
    if expected == real {
        Ok(())
    } else {
        Err(Box::new(ReturnError {
            expected,
            real,
        }))
    }
}


