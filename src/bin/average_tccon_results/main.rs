use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use ggg_rs::{
    averaging::{self},
    readers::ProgramVersion,
    utils::GggError,
};

mod grouping;

fn main() -> ExitCode {
    let clargs = AverageCli::parse();
    if let Err(e) = driver(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[derive(Debug, Parser)]
struct AverageCli {
    /// Path the to post processing file containing per-window
    /// amounts to average. This will be a file with ".?sw" as an
    /// extension.
    upstream_file: PathBuf,

    /// Directory in which to save the output file. If omitted, the output
    /// file will be saved to the same directory as the upstream file.
    #[clap(short = 'o', long)]
    output_dir: Option<PathBuf>,
}

fn driver(clargs: AverageCli) -> error_stack::Result<(), GggError> {
    let in_file = clargs.upstream_file.as_path();
    let grouper = grouping::default_tccon_grouper();
    averaging::average_results(
        in_file,
        &grouper,
        program_version(),
        clargs.output_dir.as_deref(),
    )?;
    Ok(())
}

fn program_version() -> ProgramVersion {
    ProgramVersion {
        program: "average_tccon_results".to_string(),
        version: "Version 1.0".to_string(),
        date: "2026-06-25".to_string(),
        authors: "JLL".to_string(),
    }
}
