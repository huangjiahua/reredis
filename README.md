![reredis-pic](https://storage.googleapis.com/gawa-storage-x/reredis.jpg)
# reredis

reredis is a reimplementation of [Redis](https://redis.io/) (server) in Rust programming language. The current equivalent version of Redis is 1.x - 2.x. It supports Linux and MacOS(it depends on Unix API like fork, so Windows version is not available now).

[![Build Status](https://dev.azure.com/jiahuah0077/jiahuah/_apis/build/status/huangjiahua.reredis?branchName=master)](https://dev.azure.com/jiahuah0077/jiahuah/_build/latest?definitionId=1&branchName=master)
[![Crates.io](https://img.shields.io/crates/v/reredis.svg)](https://crates.io/crates/reredis)

Licensed under BSD 3-Clause.

## HEADLINES!

Async version of reredis is coming in async branch!

## Building reredis

reredis can be compiled on all *nix systems that supports Rust toolchain(but tested only on Linux and MacOS).

It requires Rust(>= 1.37.0) to compile. To install Rust, [see this](https://www.rust-lang.org/tools/install).

The build command is

```shell
%cargo build --release
```

and the executable is located at  `./target/release/reredis`

After building Redis, it is a good idea to test it using:

```shell
%cargo test  # This is unit tests
```

and

```shell
%cargo test --test server_test -- --ignored --nocapture # This is integration tests
```

Alternatively, you can use the Makefile, which is just a wrapper of the former commands.

```shell
%make
```

to build.

```shell
%make test
```

to do all tests.

## How to use?

The command is identical to Redis. Like

```shell
%reredis  # start on 127.0.0.1:6379
```

and

```shell
%reredis --bind 0.0.0.0 --port 9090 # binds on all ip address and port 9090
```

and

```shell
%reredis example.conf # configured by example.conf
```

Other supported configuration are listed [here](./example.conf)

### Supported Commands

The usage of the commands can be looked up [here](https://redis.io/commands).

- get
- set
- setnx
- del
- exists
- incr
- decr
- mget
- rpush
- lpush
- lpop
- rpop
- llen
- lindex
- lset
- lrange
- ltrim
- lrem
- sadd
- srem
- smove
- sismember
- scard
- spop
- sinter
- sinterstore
- sunion
- sunionstore
- sdiff
- sdiffstore
- smembers
- incrby
- decrby
- getset
- randomkey
- select
- move
- rename
- renamenx
- expire
- keys
- dbsize
- auth
- ping
- echo
- save
- bgsave
- shutdown
- lastsave
- type
- sync
- flushdb
- flushall
- sort
- info
- monitor
- ttl
- slaveof
- object encoding

## Relation with Redis

reredis is a reimplementation of Redis, and its protocol is compatible with Redis Protocol([RESP](https://redis.io/topics/protocol)). But the current version of `.rdb` file is not compatible with Redis, because the format of `ziplist` and `intset` is slightly different. I'm still working on it. 

## Clients

Since the protocal is compatible with Redis. All clients of Redis can be used with reredis, like [redis-rs](https://github.com/mitsuhiko/redis-rs) in Rust. There is currently on implementation of `redis-cli` in my project, but I'm working on it. 
