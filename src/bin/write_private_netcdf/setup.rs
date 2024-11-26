use std::path::{Path, PathBuf};

use error_stack::ResultExt;
use ggg_rs::output_files::{get_col_files, get_file_from_col_header};

use crate::errors::CliError;

pub(crate) struct InputFiles {
    pub(crate) runlog: PathBuf,
    pub(crate) col_files: Vec<PathBuf>,
    pub(crate) aia_file: PathBuf,
    pub(crate) qc_file: PathBuf,
}

impl InputFiles {
    pub(crate) fn from_run_dir(run_dir: &Path) -> error_stack::Result<Self, CliError> {
        let ggg_path = ggg_rs::utils::get_ggg_path()
            .map_err(|e| CliError::runtime_error(e.to_string()))?;
        if !ggg_path.exists() {
            return Err(CliError::input_error(format!(
                "GGGPATH directory ({}) does not exist", ggg_path.display()
            )).into())
        }

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


        let runlog_name = runlog.file_stem()
            .ok_or_else(|| CliError::input_error("failed to get the runlog name from the path to the runlog"))?;
        let site_id: String = runlog_name.to_string_lossy().chars().take(2).collect();

        let mut aia_name = runlog_name.to_os_string();
        aia_name.push(".vav.ada.aia");
        let aia_file = run_dir.join(aia_name);
        if !aia_file.exists() {
            return Err(CliError::input_error(format!("expected .aia file ({}) does not exist", aia_file.display())).into());
        }

        let qc_file = ggg_path.join("tccon").join(format!("{site_id}_qc.dat"));
        if !qc_file.exists() {
            return Err(CliError::input_error(format!("expected qc.dat file ({}) does not exist", qc_file.display())).into());
        }
        

        Ok(Self { runlog, col_files, aia_file, qc_file })
    }
}