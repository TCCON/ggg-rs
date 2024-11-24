use std::{path::PathBuf, process::ExitCode};

use error_stack::ResultExt;
use errors::CliError;
use log::info;

use crate::errors::IntoCliReport;

mod logging;
mod errors;
mod interface;
mod setup;
mod dimensions;
mod providers;

fn main() -> ExitCode {
    let run_dir = PathBuf::from(".");
    logging::init_logging(&run_dir, log::LevelFilter::Debug);
    info!("Logging initialized");

    match driver(run_dir) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("{e:?}");
            eprintln!("{e:?}");
            ExitCode::FAILURE
        },
    }
}

fn driver(run_dir: PathBuf) -> error_stack::Result<(), CliError> {
    let file_paths = setup::InputFiles::from_run_dir(&run_dir)?;

    let providers = [
        providers::RunlogProvider::new(file_paths.runlog).change_context_lazy(|| CliError::input_error("error occurred while reading the runlog"))?,
    ];

    Ok(())
}