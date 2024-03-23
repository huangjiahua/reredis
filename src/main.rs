#[macro_use]
extern crate log;
extern crate env_logger;

use reredis::env::{Env, REREDIS_VERSION, init_logger, Config};
use reredis::oom::oom;
use reredis::zalloc::Zalloc;

#[global_allocator]
static A: Zalloc = Zalloc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::new();

    if args.len() == 2 {
        config.reset_server_save_params();
        config.load_server_config(&args[1]);
    } else if args.len() > 2 {
        config.config_from_args(&args[..]);
    } else {
        println!("no config file specified, using the default config. \
            In order to specify a config file use 'reredis-server \
            /path/to/reredis.conf'");
    }

    let mut env = Env::new(&config);
    env.init_server();
    init_logger(env.server.verbosity);

    if env.server.daemonize {
        env.daemonize();
    }

    info!("Server started, Reredis version {}", REREDIS_VERSION);

    if env.rdb_load().is_ok() {
        info!("DB loaded from disk");
    }

    if env.create_first_file_event().is_err() {
        oom("creating file event");
    }

    info!("The server is now ready to accept connections on port {}", env.server.port);

    env.ae_main();
}