use std::path::{Path, PathBuf};

use error_stack::ResultExt;
use ggg_rs::output_files::{get_col_files, get_file_from_col_header};

use crate::errors::CliError;

pub(crate) struct InputFiles {
    pub(crate) runlog: PathBuf,
    pub(crate) col_files: Vec<PathBuf>,
}

impl InputFiles {
    pub(crate) fn from_run_dir(run_dir: &Path) -> error_stack::Result<Self, CliError> {
        let multiggg_file = run_dir.join("multiggg.sh");
        if !multiggg_file.exists() {
            return Err(CliError::input_error(format!(
                "No multiggg.sh file found in {}", run_dir.display()
            )).into())
        }
        let col_files = get_col_files(&multiggg_file, run_dir)
            .change_context_lazy(|| CliError::input_error("failed to get the list of .col file by parsing the multiggg.sh file"))?;

        let runlog = get_file_from_col_header(&col_files, run_dir, |h| h.runlog_file.path)
            .change_context_lazy(|| CliError::runtime_error("failed to get the runlog from the .col file headers; may indicate a file system problem or inconsistent runlogs listed"))?;

        Ok(Self { runlog, col_files })
    }
}