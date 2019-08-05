use reredis::log::LogLevel;
use reredis::env::{Env, REREDIS_VERSION};
use reredis::oom::oom;
use std::process::exit;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut env = Env::new();

    if args.len() == 2 {
        env.reset_server_save_params();
        env.load_server_config(&args[1]);
    } else if args.len() > 2 {
        eprintln!("Usage: reredis-server [/path/to/reredis.conf]");
        exit(1);
    } else {
        env.log(
            LogLevel::Warning,
            "no config file specified, using the default config. \
            In order to specify a config file use 'reredis-server \
            /path/to/reredis.conf'",
        );
    }

    env.init_server();

    if env.server.daemonize {
        env.daemonize();
    }

    env.log(
        LogLevel::Notice,
        &format!("Server started, Reredis version {}", REREDIS_VERSION),
    );

    if let Ok(_) = env.rdb_load() {
        env.log(LogLevel::Notice, "DB loaded from disk");
    }

    if let Err(_) = env.create_first_file_event() {
        oom("creating file event");
    }

    env.log(
        LogLevel::Notice,
        &format!("The server is now ready to accept connections on port {}", env.server.port),
    );

    env.ae_main();
}