# reredis

Rewrite [redis by Salvatore Sanfilippo](https://github.com/antirez/redis) in [Rust](https://www.rust-lang.org).

*huangjiahua*

[![Build Status](https://dev.azure.com/jiahuah0077/jiahuah/_apis/build/status/huangjiahua.reredis?branchName=master)](https://dev.azure.com/jiahuah0077/jiahuah/_build/latest?definitionId=1&branchName=master)

It is now under developing. **Hoping someone can join me!**

## Try it?

1. Persistant storage is not supported yet. 

2. The supported commands are listed in the bottom of the file `src/command.rs`, you can refer to [Redis Commands](https://redis.io/commands/) to get the information of the commands.

3. You can use all sorts of clients(cli tool or SDK) to use reredis.

4. How to build? 
  1. Install rust toolchain ([guide](https://www.rust-lang.org/tools/install)). In short, just run `curl https://sh.rustup.rs -sSf | sh` in the terminal and add `$HOME/.cargo/bin/` to you PATH. The least version of Rust should be 1.37.0 stable. In fact, I build reredis using this version.
  2. Change directory(cd) to the reredis project directory and run `cargo build`(you can run `cargo build --release` if you'd like the release version). If you just want to run reredis directly, run `cargo run`. 
  3. After building, you can find the binaries at `target/debug/reredis` or `target/release/reredis` depending on your build type.
  4. `./reredis` or `reredis` on Windows.
  5. If you really want to use it to do some jobs(looks like you really like me or this project), change the address and port in the `src/server.rs` use it as you like.
  
