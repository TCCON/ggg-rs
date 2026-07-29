use std::{path::PathBuf, process::ExitCode};

use clap::Parser;

use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::{logging::init_logging, opus::IgramHeader};

fn main() -> ExitCode {
    let clargs = Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    if let Err(e) = driver(clargs) {
        eprintln!("{e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Print quantities from an Opus interferogram or spectrum header.
///
/// This is similar to the OpusHdr Perl program, but the block labels
/// will match the [`ggg_rs::opus::constants::bruker::BrukerBlockType`]
/// enum.
#[derive(Debug, Parser)]
struct Cli {
    /// Path to the spectrum or interferogram.
    spec_or_igm: PathBuf,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("{0}")]
    Custom(String),
}

impl CliError {
    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    let header = IgramHeader::read_full_igram_header(&clargs.spec_or_igm)
        .change_context_lazy(|| CliError::custom("Error reading file header"))?;
    for (block_type, block) in header.parameter_blocks {
        println!("{block_type}");
        for (param_key, param_val) in block {
            println!("  {param_key}: {param_val:?}")
        }
    }
    Ok(())
}
