use std::path::Path;

use log::LevelFilter;
use log4rs::{encode::pattern::PatternEncoder, append::{console::{ConsoleAppender, Target}, file::FileAppender}, Config, config::{Appender, Root}};


/// Set up logging to both stderr and "write_netcdf.log" in the given run directory.
/// 
/// Note that any previous write_netcdf.log is overwritten. Panics if setting up the logger
/// fails, usually because it cannot write to the log file.
pub(crate) fn init_logging(run_dir: &Path, level: LevelFilter) {
    let stderr = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{h({d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M})} - {m}{n}")))
        .target(Target::Stderr)
        .build();
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M} - {m}{n}")))
        .append(false)
        .build(run_dir.join("write_netcdf.log"))
        .expect("Could not create write_netcdf.log file");

    let config = Config::builder()
        .appender(Appender::builder().build("stderr", Box::new(stderr)))
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(
            Root::builder()
                .appender("stderr")
                .appender("logfile")
                .build(level)
        ).expect("Failed to configure logger");

    log4rs::init_config(config).expect("Failed to initialize logger");
}