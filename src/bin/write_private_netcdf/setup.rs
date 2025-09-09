use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use error_stack::ResultExt;
use ggg_rs::readers::col_files::{get_all_col_files, get_col_files, get_file_from_col_header};

use crate::errors::CliError;

pub(crate) struct InputFiles {
    pub(crate) runlog: PathBuf,
    pub(crate) window_prefix_file: PathBuf,
    pub(crate) mav_file: PathBuf,
    /// Col files listed in the multiggg.sh file and not commented out
    pub(crate) selected_col_files: Vec<PathBuf>,
    /// All .col files present in the directory, whether listed in the multiggg.sh file or not
    pub(crate) all_col_files: Vec<PathBuf>,
    pub(crate) vsw_file: PathBuf,
    pub(crate) tsw_file: PathBuf,
    pub(crate) vav_file: PathBuf,
    pub(crate) tav_file: PathBuf,
    pub(crate) vsw_ada_file: Option<PathBuf>,
    pub(crate) vav_ada_file: PathBuf,
    pub(crate) aia_file: PathBuf,
    pub(crate) qc_file: PathBuf,
}

impl InputFiles {
    pub(crate) fn from_run_dir(run_dir: &Path) -> error_stack::Result<Self, CliError> {
        let ggg_path =
            ggg_rs::utils::get_ggg_path().map_err(|e| CliError::runtime_error(e.to_string()))?;
        if !ggg_path.exists() {
            return Err(CliError::input_error(format!(
                "GGGPATH directory ({}) does not exist",
                ggg_path.display()
            ))
            .into());
        }

        let window_prefix_file = ggg_path.join("tccon").join("secondary_prefixes.dat");
        if !window_prefix_file.exists() {
            return Err(CliError::input_error(format!(
                "Window prefix file not found at {}",
                window_prefix_file.display()
            ))
            .into());
        }

        let multiggg_file = run_dir.join("multiggg.sh");
        if !multiggg_file.exists() {
            return Err(CliError::input_error(format!(
                "No multiggg.sh file found in {}",
                run_dir.display()
            ))
            .into());
        }
        let selected_col_files =
            get_col_files(&multiggg_file, run_dir).change_context_lazy(|| {
                CliError::input_error(
                    "failed to get the list of .col file by parsing the multiggg.sh file",
                )
            })?;

        let all_col_files = get_all_col_files(run_dir).change_context_lazy(|| {
            CliError::runtime_error(format!(
                "failed to get the list of all .col files present in {}",
                run_dir.display()
            ))
        })?;

        let runlog = get_file_from_col_header(&selected_col_files, run_dir, |h| h.runlog_file.path)
            .change_context_lazy(|| CliError::runtime_error("failed to get the runlog from the .col file headers; may indicate a file system problem or inconsistent runlogs listed"))?;

        let runlog_name = runlog.file_stem().ok_or_else(|| {
            CliError::input_error("failed to get the runlog name from the path to the runlog")
        })?;
        let site_id: String = runlog_name.to_string_lossy().chars().take(2).collect();

        // Gsetup files
        let mav_file = find_req_output_file(run_dir, runlog_name, ".mav")?;

        // All the postprocessing files
        let vsw_file = find_req_output_file(run_dir, runlog_name, ".vsw")?;
        let tsw_file = find_req_output_file(run_dir, runlog_name, ".tsw")?;
        let vav_file = find_req_output_file(run_dir, runlog_name, ".vav")?;
        let tav_file = find_req_output_file(run_dir, runlog_name, ".tav")?;
        // Allow the .vsw.ada file to be missing; the EM27s don't generate this
        let vsw_ada_file = find_req_output_file(run_dir, runlog_name, ".vsw.ada").ok();
        let vav_ada_file = find_req_output_file(run_dir, runlog_name, ".vav.ada")?;
        let aia_file = find_req_output_file(run_dir, runlog_name, ".vav.ada.aia")?;

        let qc_file = ggg_path.join("tccon").join(format!("{site_id}_qc.dat"));
        if !qc_file.exists() {
            return Err(CliError::input_error(format!(
                "expected qc.dat file ({}) does not exist",
                qc_file.display()
            ))
            .into());
        }

        Ok(Self {
            runlog,
            window_prefix_file,
            mav_file,
            selected_col_files,
            all_col_files,
            aia_file,
            vsw_file,
            tsw_file,
            vav_file,
            tav_file,
            vsw_ada_file,
            vav_ada_file,
            qc_file,
        })
    }
}

fn find_req_output_file(
    run_dir: &Path,
    runlog_name: &OsStr,
    ext: &str,
) -> Result<PathBuf, CliError> {
    let mut name = runlog_name.to_os_string();
    name.push(ext);
    let file = run_dir.join(name);
    if file.exists() {
        Ok(file)
    } else {
        Err(CliError::input_error(format!(
            "expected {ext} file ({}) does not exist",
            file.display()
        )))
    }
}
