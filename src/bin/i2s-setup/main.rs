use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand, Args};
use ggg_rs::i2s::I2SVersion;
use merge_inputs::ParamWhitespaceEq;

mod merge_inputs;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    let res = match clargs.command {
        Commands::MergeInputs(args) => merge_inputs::driver(
            &args.input_files,
            &args.output_file,
            args.i2s_version,
            args.whitespace_eq,
            &args.skip_param
        ),
    };

    if let Err(e) = res {
        eprintln!("Error: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    MergeInputs(MergeInputsCli)
}

#[derive(Debug, Args)]
struct MergeInputsCli {
    /// The I2S inputs files to merge.
    input_files: Vec<PathBuf>,

    /// Path to write to (required).
    #[clap(short, long, required = true)]
    output_file: PathBuf,

    /// Which I2S version these input files are for (needed to
    /// determine the number of header parameters). Options are
    /// '2014' or '2020'; '2020' is the default.
    #[clap(short, long)]
    i2s_version: I2SVersion,

    /// How to consider whitespace when checking parameters for equality.
    /// The default will require and exact match (including whitespace) for
    /// parameters where whitespace matters. 'matchall' will require exact
    /// equality for all parameters, and 'ignoreall' will ignore whitespace
    /// for all parameters.
    #[clap(short, long, default_value_t=ParamWhitespaceEq::default())]
    whitespace_eq: ParamWhitespaceEq,

    /// Parameter numbers (1-based) to ignore when checking that they
    /// agree before merging the headers.
    #[clap(short = 's', long, action=clap::ArgAction::Append)]
    skip_param: Vec<usize>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Could not read file {}", .0.display())]
    ReadError(PathBuf),
    #[error("Could not write to file {}", .0.display())]
    WriteError(PathBuf),
    #[error("Parameter #{param} differs between {} and {}; ('{v1}' vs. '{v2}'). Note that later files/parameters may also differ.", f1.display(), f2.display())]
    ParamMismatch{f1: PathBuf, v1: String, f2: PathBuf, v2: String, param: usize},
    #[error("Error in arguments: {0}")]
    BadInput(String),
}