use std::path::Path;

use tracing_subscriber::filter::LevelFilter;


/// Set up logging to both stderr and "write_netcdf.log" in the given run directory.
/// 
/// Note that any previous write_netcdf.log is overwritten. Panics if setting up the logger
/// fails, usually because it cannot write to the log file.
pub(crate) fn init_logging(run_dir: &Path, level: LevelFilter) {
    // TODO: write to log file, possibly integrate with indicatif to use its
    // println (https://docs.rs/indicatif/latest/indicatif/struct.ProgressBar.html#method.println)
    // or suspend functions to provide a way to log messages and have a progress bar running.
    // Would also be nice to have an extra layer that writes warnings to a structured JSON file.
    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_max_level(level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing/logging subscriber");
}