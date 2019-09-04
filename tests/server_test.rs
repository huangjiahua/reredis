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
    TestCase { name: "simple del", func: test_simple_del },
    TestCase { name: "simple incr and decr", func: test_simple_incr_decr },
    TestCase { name: "simple mget", func: test_simple_mget },
];

// simple tests

fn test_ping(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to ping");
    let mut con = establish()?;
    let ret: String = redis::cmd("PING").query(&mut con)?;
    compare("PONG".to_string(), ret)
}

fn test_simple_set_and_get(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to set and get");
    let mut con = establish()?;
    let ret: String = con.set("_key", "_value")?;
    compare("OK".to_string(), ret)?;

    let ret: String = con.get("_key")?;
    compare("_value".to_string(), ret)?;

    let ret: i64 = con.set_nx("_key", "_value")?;
    compare_i64(0, ret)?;

    let ret: Option<String> = con.get("_not_exist")?;
    is_nil(ret)?;


    Ok(())
}

fn test_simple_del(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to set and del");
    let mut con = establish()?;
    let ret: String = con.set("_to_be_deleted", "0")?;
    compare("OK".to_string(), ret)?;

    let ret: i64 = con.del("_to_be_deleted")?;
    compare_i64(1, ret)?;

    let ret: i64 = con.del("_to_not_be_deleted")?;
    compare_i64(0, ret)?;

    Ok(())
}

fn test_simple_incr_decr(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to incr and decr");
    let mut con = establish()?;
    let ret: i64 = con.incr("_counter1", 1)?;
    compare_i64(1, ret)?;

    let ret: i64 = con.incr("_counter2", -1)?;
    compare_i64(-1, ret)?;

    let ret: i64 = con.incr("_counter1", 100)?;
    compare_i64(101, ret)?;

    let ret: i64 = con.incr("_counter1", -1000)?;
    compare_i64(-899, ret)?;

    Ok(())
}

fn test_simple_mget(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to mget");
    let mut con = establish()?;
    con.set_read_timeout(Some(Duration::from_secs(10)));
    for j in 0..3 {
        let _: () = con.set(&format!("key{}", j), &j.to_string())?;
    }

    let ret: Vec<Option<String>> = con.get(&["key0", "key1", "key2"])?;
    for j in 0..3 {
        // TODO: change this
        let s = ret[j].as_ref().unwrap();
        let _ = compare(j.to_string(), s.clone())?;
    }
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

fn establish_other(addr: &str) -> Result<redis::Connection, Box<dyn Error>> {
    let client = redis::Client::open(addr)?;
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

fn compare_i64(expected: i64, real: i64) -> TestResult {
    if expected == real {
        Ok(())
    } else {
        Err(Box::new(ReturnError {
            expected: expected.to_string(),
            real: real.to_string(),
        }))
    }
}

fn is_nil(real: Option<String>) -> TestResult {
    if real.is_none() {
        Ok(())
    } else {
        Err(Box::new(ReturnError {
            expected: "nil".to_string(),
            real: real.unwrap(),
        }))
    }
}


