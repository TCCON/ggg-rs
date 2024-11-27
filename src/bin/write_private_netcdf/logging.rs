use std::{path::Path, sync::{Arc, Mutex}};

use indicatif::MultiProgress;
use tracing::Level;
use tracing_subscriber::{Registry,fmt::writer::MakeWriterExt,prelude::*};


/// Set up logging to both stderr and "write_netcdf.log" in the given run directory.
/// 
/// Note that any previous write_netcdf.log is overwritten. Panics if setting up the logger
/// fails, usually because it cannot write to the log file.
pub(crate) fn init_logging(run_dir: &Path, level: Level, mpbar: Arc<MultiProgress>) {
    // TODO: Possibly integrate with indicatif to use its
    // println (https://docs.rs/indicatif/latest/indicatif/struct.ProgressBar.html#method.println)
    // or suspend functions to provide a way to log messages and have a progress bar running.
    // This will probably require creating a custom struct that implements Write and handles suspending
    // a weak progress bar to print the messages.

    // Log to the screen with the user-requested verbosity
    // The Mutex is required by tracing_subscriber to make something that implements
    // std::io::Write implement tracing_subscriber::writer::MakeWriter.
    let stderr = Mutex::new(ConsoleLogger::new(mpbar)).with_max_level(level);
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(stderr);

    // Log the same things to a write_netcdf.log file, but enforce that this file is always recording
    // detailed logs (at least DEBUG)
    let file = std::fs::File::create(run_dir.join("write_netcdf.log"))
        .expect("Could not create log file")
        .with_max_level(Level::DEBUG);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file);

    // I would kind of like to give each warning an ID that is easy to search for, but let's see
    // how the basic JSON format works for now.
    let json_file = std::fs::File::create(run_dir.join("write_netcdf.log.json"))
        .expect("Could not create log JSON file")
        .with_max_level(Level::WARN);
    let json_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(json_file)
        .json();

    let subscriber = Registry::default()
        .with(stderr_layer)
        .with(file_layer)
        .with(json_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing/logging subscriber");
}

struct ConsoleLogger {
    stderr: std::io::Stderr,
    mpbar: Arc<MultiProgress>,
}

impl ConsoleLogger {
    fn new(mpbar: Arc<MultiProgress>) -> Self {
        let stderr = std::io::stderr();
        Self { stderr, mpbar }
    }
}

impl std::io::Write for ConsoleLogger {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut res = Ok(0);
        self.mpbar.suspend(|| {
            res = self.stderr.write(buf);
        });
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut res = Ok(());
        self.mpbar.suspend(|| {
            res = self.stderr.flush();
        });
        res
    }
}