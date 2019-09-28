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
use rand::Rng;

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
        let mut config = Config::new();
        config.db_filename = "__temp_reredis_test_rdb_file.trdb".to_string();
        let mut env = Env::new(&config);
        env.server.clean_rdb = true;
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
    TestCase { name: "simple list push and pop", func: test_simple_list_push_pop },
    TestCase { name: "simple sort", func: test_simple_sort },
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

fn test_simple_list_push_pop(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to list push");
    let mut con = establish()?;

    // test lpush
    let ret: i64 = con.lpush("_list_simple_lpush", &["1", "2", "3", "4", "5"])?;
    let _ = compare_i64(5, ret)?;

    let ret: Vec<String> = con.lrange("_list_simple_lpush", 0, -1)?;
    let _ = compare_vec(vec!["5", "4", "3", "2", "1"], ret)?;

    // test rpush
    let ret: i64 = con.rpush("_list_simple_rpush", &["1", "2", "3", "4", "5"])?;
    let _ = compare_i64(5, ret)?;

    let ret: Vec<String> = con.lrange("_list_simple_rpush", 0, -1)?;
    let _ = compare_vec(vec!["1", "2", "3", "4", "5"], ret)?;

    // test lpop
    let ret: String = con.lpop("_list_simple_rpush")?;
    let _ = compare("1", ret)?;
    let ret: String = con.lpop("_list_simple_rpush")?;
    let _ = compare("2", ret)?;

    // test rpop
    let ret: String = con.rpop("_list_simple_rpush")?;
    let _ = compare("5", ret)?;
    let ret: String = con.rpop("_list_simple_rpush")?;
    let _ = compare("4", ret)?;

    let ret: i64 = con.llen("_list_simple_rpush")?;
    let _ = compare_i64(1, ret)?;

    // test pop empty
    let ret: Option<String> = con.rpop("_no_such_list")?;
    let _ = is_nil(ret)?;
    let ret: Option<String> = con.lpop("_no_such_list")?;
    let _ = is_nil(ret)?;

    // test lindex
    let ret: String = con.lindex("_list_simple_lpush", 0)?;
    let _ = compare("5", ret)?;

    let ret: Option<String> = con.lindex("_list_simple_lpush", 100)?;
    let _ = is_nil(ret)?;
    Ok(())
}

fn test_simple_lset(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to lset");
    let mut con = establish()?;

    let ret: i64 = con.rpush("_list_simple_lset", &["1", "2", "3", "4"])?;
    let _ = compare_i64(4, ret)?;

    let ret: String = con.lset("_list_simple_lset", 3, "5")?;
    let _ = compare("OK".to_string(), ret)?;

    let ret: String = con.lindex("_list_simple_lset", 3)?;
    let _ = compare("5", ret)?;

    Ok(())
}

fn test_simple_sort(_input: Box<dyn TestInputData>) -> TestResult {
    error!("ready to sort");
    let mut con = establish()?;
    let mut rng = rand::thread_rng();

    for _ in 0..20 {
        let k: i64 = rng.gen();
        let _ = con.lpush("_simple_sort_1", k.to_string())?;
    }

    let ret: Vec<i64> = redis::cmd("SORT").arg("_simple_sort_1").query(&mut con)?;
    is_sorted(&ret);
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

fn compare<S>(expected: S, real: String) -> TestResult
    where S: PartialEq<String> + std::string::ToString {
    if expected == real {
        Ok(())
    } else {
        Err(Box::new(ReturnError {
            expected: expected.to_string(),
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

fn compare_vec<S>(expected: Vec<S>, real: Vec<String>) -> TestResult
    where S: PartialEq<String> + std::fmt::Debug {
    if expected == real {
        Ok(())
    } else {
        Err(Box::new(ReturnError {
            expected: format!("{:?}", expected),
            real: format!("{:?}", real),
        }))
    }
}

fn is_nil<T>(real: Option<T>) -> TestResult
    where T: std::fmt::Debug {
    if real.is_none() {
        Ok(())
    } else {
        let none: Option<T> = None;
        Err(Box::new(ReturnError {
            expected: format!("{:?}", none),
            real: format!("{:?}", real),
        }))
    }
}

fn is_sorted<T>(v: &Vec<T>) -> TestResult
    where T: std::cmp::Ord + Clone {
    let mut v2 = v.to_vec();
    v2.sort();
    match *v == v2 {
        true => Ok(()),
        false => Err(Box::new(ReturnError {
            expected: "sorted vec".to_string(),
            real: "unsorted vec".to_string(),
        }))
    }
}


