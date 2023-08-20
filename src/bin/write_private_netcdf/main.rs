use std::{io::{BufRead, BufReader}, path::PathBuf};

use error::SetupError;
use error_stack::{IntoReport, ResultExt};
use ggg_rs::{utils::{GggError, FileBuf}, error::{HeaderError, FileLocation}};
use ggg_rs::output_files::read_col_file_header;
use itertools::Itertools;
use sources::{TcconRunlog, DataSourceList};

mod error;
mod interface;
mod sources;

fn main() {
    // Basic workflow:
    //  1. Generate the list of `DataSource` instances; this will need to be semi-dynamic (i.e. read from the multiggg file)
    //  2. Get the list of available dimensions from these instances, and ensure there are no duplicates
    //  3. Get the list of required dimensions from these instances, write the dimensions required to the netCDF file
    //     (with their position determined by the `write_at_start` property)
    //  4. Get the unique groups required by all the data sources, if writing a hierarchical file, create those groups
    //  5. For each data source, loop through the groups it requires and pass it the `GroupMut` handle for that group
    //     (flat files will always get the root group, and append the required suffix to variable names).
    let all_sources = setup_data_sources().expect("An error occurred while building the list of data sources");
}

fn setup_data_sources() -> error_stack::Result<DataSourceList, SetupError> {
    let mut all_sources = DataSourceList::default();

    let windows = read_multiggg()?;
    let col_file_paths = windows.iter()
        .map(|w| PathBuf::from(format!("{w}.col")))
        .collect_vec();

    let runlog_path = get_runlog(&col_file_paths)?;
    let runlog = TcconRunlog::new(runlog_path.clone())
        .into_report()
        .change_context_lazy(|| SetupError::FileReadError { description: runlog_path.to_string_lossy().to_string() })?;
    all_sources.add_source(runlog);

    Ok(all_sources)
}

fn get_runlog(col_files: &[PathBuf]) -> error_stack::Result<PathBuf, SetupError> {
    let first_col_file = col_files.get(0)
        .ok_or_else(|| SetupError::ParsingError { 
            location: FileLocation::new(Some("."), None, None::<&str>), 
            cause: "no .col files inferred from multiggg.sh file".to_string() 
        })?;

    let mut file = FileBuf::open(first_col_file)
        .into_report()
        .change_context_lazy(|| SetupError::FileReadError { description: first_col_file.to_string_lossy().to_string() })?;
    let col_header = read_col_file_header(&mut file)
        .change_context_lazy(|| SetupError::FileReadError { description: first_col_file.to_string_lossy().to_string() })?;

    Ok(col_header.runlog_file.path)
}

fn read_multiggg() -> error_stack::Result<Vec<String>, SetupError> {
    let multiggg_file = PathBuf::from("multiggg.sh");

    let f = ggg_rs::utils::FileBuf::open(&multiggg_file)
        .into_report()
        .change_context_lazy(|| SetupError::FileReadError { description: "multiggg.sh".to_string() })?;

    let mut windows = vec![];
    for (idx, line) in f.into_reader().lines().enumerate() {
        let line = line
            .into_report()
            .change_context_lazy(|| SetupError::FileReadError { description: "multiggg.sh".to_string() })?
            .trim()
            .to_string();

        // GGG traditionally uses : for comments, since this is a shell script, also check for shell comments
        if line.starts_with(':') || line.starts_with('#') {
            continue;
        }

        // Assume a line format like: /mnt/data/josh/Research/ggg-devel/bin/gfit luft_6146.pa_ggg_benchmark.ggg>/dev/null
        let end_part = line.split("gfit ").nth(1)
            .ok_or_else(|| SetupError::ParsingError { 
                location: FileLocation::new(Some("multiggg.sh"), Some(idx+1), Some(&line)), 
                cause: "could not find 'gfit' to split on when identifying window name".to_string()
            })?;

        let this_window = end_part.split(".ggg").next()
            .ok_or_else(|| SetupError::ParsingError { 
                location: FileLocation::new(Some("multiggg.sh"), Some(idx+1), Some(&line)), 
                cause: "could not find '.ggg' to split on when identifying window name".to_string()
            })?;

        windows.push(this_window.trim().to_string());
    }
    
    Ok(windows)
}