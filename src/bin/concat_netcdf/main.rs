use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::logging::init_logging;

use crate::error::GggConcatError;

mod error;
mod setup;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    if let Err(e) = main_inner(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn main_inner(clargs: Cli) -> error_stack::Result<(), GggConcatError> {
    let mut concatenator = setup::setup_concat();
    let output_file = get_out_name(&clargs)?;
    let input_files = clargs.cmd.get_files().change_context_lazy(|| {
        GggConcatError::context("Error occurred while inferring the output file name")
    })?;
    concatenator
        .concat(&input_files, &output_file)
        .map_err(|e| e.to_error_stack())
        .change_context_lazy(|| GggConcatError::context("Error occurred during concatenation"))?;
    Ok(())
}

fn get_out_name(clargs: &Cli) -> Result<PathBuf, GggConcatError> {
    if clargs.explicit_out_file && clargs.out.is_dir() {
        return Err(GggConcatError::use_error(
            "--out must not be a directory if --explicit-out-file is set",
        ));
    }

    if clargs.explicit_out_file {
        return Ok(clargs.out.to_path_buf());
    }

    todo!("infer output name from input files")
}

#[derive(Debug, clap::Parser)]
struct Cli {
    /// Output location, see --explicit-out-file for more information
    #[clap(long, default_value = ".")]
    out: PathBuf,

    /// Use this flag to change the meaning of --out. By default,
    /// --out specifies a directory in which to place the concatenated
    /// file. With this flag, --out must include the desired output
    /// file name.
    #[clap(short = 'e', long)]
    explicit_out_file: bool,

    #[clap(subcommand)]
    cmd: InputChoice,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, clap::Subcommand)]
enum InputChoice {
    /// List specific files to concatenate on the command line
    Files(FilesCli),

    /// Specify a directory containing netCDF files to concatenate.
    Site(SiteCli),
}

impl InputChoice {
    fn get_files(self) -> std::io::Result<Vec<PathBuf>> {
        match self {
            InputChoice::Files(files_cli) => Ok(files_cli.files),
            InputChoice::Site(site_cli) => site_cli.get_files(),
        }
    }
}

/// Concatenate specific files.
#[derive(Debug, clap::Args)]
struct FilesCli {
    /// The files to concatenate, as individual arguments.
    files: Vec<PathBuf>,
}

/// Concatenate all files in a directory, optionally with a given
/// prefix.
#[derive(Debug, clap::Args)]
struct SiteCli {
    /// If given, only concatenate input files starting with this string
    /// in their name.
    #[clap(long, default_value = "")]
    prefix: String,

    /// Set the suffix/file extension to search for.
    #[clap(long, default_value = ".nc")]
    suffix: String,

    /// The directory from which to take the input files.
    path: PathBuf,
}

impl SiteCli {
    fn get_files(&self) -> std::io::Result<Vec<PathBuf>> {
        if !self.path.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("Given input path is not a directory"),
            ));
        }

        let matches = glob::glob(&format!(
            "{}/{}*{}",
            self.path.display(),
            self.prefix,
            self.suffix
        ))
        .map_err(|e| std::io::Error::other(format!("Error globbing for input files: {e}")))?;

        let mut files = vec![];
        for entry in matches {
            let entry = entry.map_err(|e| {
                std::io::Error::other(format!("Error globbing for input files: {e}"))
            })?;
            files.push(entry);
        }

        Ok(files)
    }
}
