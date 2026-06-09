use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use error_stack::ResultExt;
use ggg_rs::{
    averaging::{self, WindowGrouper},
    readers::postproc_files::open_and_read_postproc_file,
};

mod grouping;

static SW_EXT_REGEX: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

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

impl AverageCli {
    fn get_output_file(&self) -> Result<PathBuf, CliError> {
        let re = SW_EXT_REGEX.get_or_init(|| {
            regex::Regex::new(r"\.[a-z]sw").expect("Could not compile regex for .?sw extension")
        });
        let old_name = self
            .upstream_file
            .file_name()
            .ok_or_else(|| CliError::custom("could not get name base name of the upstream file"))?
            .to_str()
            .ok_or_else(|| {
                CliError::custom("upstream file name should be interpretable in UTF-8")
            })?;
        let new_name = re.replace(old_name, ".${1}av");

        let out_dir = self.output_dir.as_deref().unwrap_or_else(|| {
            self.upstream_file
                .parent()
                .expect("upstream file should have a parent directory")
        });
        let out_file = out_dir.join(new_name.as_ref());

        Ok(out_file)
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Error reading {}", .0.display())]
    ReadError(PathBuf),
    #[error("Error reading line {line} of {}", .file.display())]
    ReadErrorAtLine { file: PathBuf, line: usize },
    #[error("Error writing output {}, {cause}", .path.display())]
    WriteError { path: PathBuf, cause: String },
    #[error("{0}")]
    Custom(String),
    #[error("{0}")]
    Context(String),
}

impl CliError {
    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }

    fn context<S: ToString>(msg: S) -> Self {
        Self::Context(msg.to_string())
    }
}

fn driver(clargs: AverageCli) -> error_stack::Result<(), CliError> {
    let in_file = clargs.upstream_file.as_path();
    let out_file = clargs.get_output_file()?;

    let (mut header, data) = open_and_read_postproc_file(in_file)
        .change_context_lazy(|| CliError::ReadError(clargs.upstream_file.to_path_buf()))?;

    let grouper = grouping::default_tccon_grouper();
    let tmp_groups = grouper
        .group_windows(&header)
        .change_context_lazy(|| CliError::context("Error occurred while grouping the windows"))?;
    dbg!(tmp_groups);
    let predefined_scale_factors = averaging::extract_scale_factors(&header)
        .change_context_lazy(|| CliError::context("An error occurred while checking for predefined scale factors in the input file header"))?;
    dbg!(predefined_scale_factors);

    Ok(())
}
