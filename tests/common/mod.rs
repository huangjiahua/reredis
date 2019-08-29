use std::io::Write;

pub fn level_to_character(level: log::Level) -> &'static str {
    match level {
        log::Level::Error => "CLIENT",
        _ => "SERVER",
    }
}

pub fn test_init_logger() {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(log::LevelFilter::Info);
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
