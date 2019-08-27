#[macro_use]
extern crate log;
extern crate env_logger;

use reredis::env::{Env, REREDIS_VERSION, init_logger, Config};
use reredis::oom::oom;

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

    if let Ok(_) = env.rdb_load() {
        info!("DB loaded from disk");
    }

    if let Err(_) = env.create_first_file_event() {
        oom("creating file event");
    }

    info!("The server is now ready to accept connections on port {}", env.server.port);

    env.ae_main();
}