use log4rs::{encode::pattern::PatternEncoder, append::console::{ConsoleAppender, Target}, Config, config::{Appender, Root}};

pub fn init_logging(level: log::LevelFilter) {
    // Eventually it might make sense to log to a file as well, so that
    // ALL of the issues that happened during post processing are captured.
    let stderr = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{h({d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M})} - {m}{n}")))
        .target(Target::Stderr)
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stderr", Box::new(stderr)))
        .build(
            Root::builder()
                .appender("stderr")
                .build(level)
        ).expect("Failed to configure logger");

    log4rs::init_config(config).expect("Failed to initialize logger");
}