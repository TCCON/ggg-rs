use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand, Args};
use ggg_rs::{i2s, i2s::I2SVersion, utils};
use merge_inputs::ParamWhitespaceEq;

mod merge_inputs;
mod modify_input;
mod copy_inputs;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    let res = match clargs.command {
        Commands::MergeInputs(args) => merge_inputs::driver(
            &args.input_files,
            &args.output_file,
            args.i2s_version,
            args.whitespace_eq,
            args.skip_param_check,
            args.edits_json.as_deref()
        ),

        Commands::ModifyInput(args) => {
            modify_input::driver(
                args.input_file,
                args.edits_json,
                args.outputs,
                args.i2s_version
            )
        },

        Commands::CopyInputs(args) => {
            if args.top_param.is_empty() {
                eprintln!("Warning: --top-param never specified. Output will be an unchanged DEST_FILE.");
            }

            copy_inputs::driver(
                &args.src_file,
                &args.dest_file, 
                args.outputs, 
                &args.top_param, 
                args.src_i2s_version,
                args.dest_i2s_version,
                args.copy_catalog
            )
        },

        Commands::EditJsonExample => {
            println!("Here is an example of an I2S edit JSON:\n");
            println!("{}", i2s::I2SInputModifcations::example_json_string(true));
            Ok(())
        }
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
    MergeInputs(MergeInputsCli),
    ModifyInput(ModifyInputCli),
    CopyInputs(CopyInputsCli),
    /// Print an example of the JSON format used by modify-input
    EditJsonExample,
}

/// Merge multiple I2S input files.
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
    #[clap(short, long, default_value_t=I2SVersion::default())]
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
    skip_param_check: Vec<usize>,

    /// A path to a JSON file specifying edits to make to the header
    /// parameters in the output file. Use - to tell this program to
    /// read from stdin.
    #[clap(long)]
    edits_json: Option<PathBuf>,
}

/// Modify parameters in an I2S input file.
#[derive(Debug, Args)]
struct ModifyInputCli {
    input_file: PathBuf,
    edits_json: PathBuf,
    /// Which I2S version these input files are for (needed to
    /// determine the number of header parameters). Options are
    /// '2014' or '2020'; '2020' is the default.
    #[clap(short, long, default_value_t=I2SVersion::default())]
    i2s_version: I2SVersion,
    #[clap(flatten)]
    outputs: utils::OutputOptCli,
}

/// Copy parameters from one I2S input file to another
#[derive(Debug, Args)]
struct CopyInputsCli {
    /// File to copy parameters from
    src_file: PathBuf,

    /// File to copy parameters to. Note that if you
    /// want to modify this directly, you must specify
    /// --in-place.
    dest_file: PathBuf,

    /// I2S version of SRC_FILE
    #[clap(short, long, default_value_t=I2SVersion::default())]
    src_i2s_version: I2SVersion,

    /// I2S version of DEST_FILE
    #[clap(short, long, default_value_t=I2SVersion::default())]
    dest_i2s_version: I2SVersion,

    #[clap(flatten)]
    outputs: utils::OutputOptCli,

    /// An argument specifying a header (i.e. top) parameter to
    /// copy from SRC_FILE to DEST_FILE. This has the format
    /// FROM,TO where FROM is the parameter number in SRC_FILE
    /// to copy and TO is the parameter number in DEST_FILE to
    /// replace. For example, "1,1" will copy parameter #1 from
    /// SRC_FILE to parameter #1 in DEST_FILE, while "27,28"
    /// would copy parameter #27 to #28. This argument can be
    /// repeated for each top parameter to copy.
    #[clap(short, long, action=clap::ArgAction::Append)]
    top_param: Vec<copy_inputs::ParamMap>,

    /// Set this flag to copy the whole catalog from SRC_FILE to DEST_FILE
    #[clap(short, long)]
    copy_catalog: bool,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Could not read file {}", .0.display())]
    ReadError(PathBuf),
    #[error("Could not write to file {}", .0.display())]
    WriteError(PathBuf),
    #[error("Could not set up I/O")]
    IoError,
    #[error("Parameter #{param} differs between {} and {}; ('{v1}' vs. '{v2}'). Note that later files/parameters may also differ.", f1.display(), f2.display())]
    ParamMismatch{f1: PathBuf, v1: String, f2: PathBuf, v2: String, param: usize},
    #[error("Error in arguments: {0}")]
    BadInput(String),
}