use std::{io::{BufRead, BufReader}, path::PathBuf};

use error_stack::{IntoReport, ResultExt};
use ggg_rs::utils::GggError;

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
    setup_data_sources().expect("An error occurred while building the list of data sources");
}

fn setup_data_sources() -> error_stack::Result<(), GggError> {
    let windows = read_multiggg()?;
    dbg!(windows);
    Ok(())
}

fn read_multiggg() -> error_stack::Result<Vec<String>, GggError> {
    let multiggg_file = PathBuf::from("multiggg.sh");

    let f = ggg_rs::utils::FileBuf::open(&multiggg_file)
        .into_report()
        .attach_printable("Could not read multiggg.sh file")?;        ;

    let mut windows = vec![];
    for (idx, line) in f.into_reader().lines().enumerate() {
        let line = line
            .map_err(|e| GggError::CouldNotRead { path: multiggg_file.clone(), reason: format!("At line {}: {e}", idx+1) })
            .into_report()?
            .trim()
            .to_string();

        // GGG traditionally uses : for comments, since this is a shell script, also check for shell comments
        if line.starts_with(':') || line.starts_with('#') {
            continue;
        }

        // Assume a line format like: /mnt/data/josh/Research/ggg-devel/bin/gfit luft_6146.pa_ggg_benchmark.ggg>/dev/null
        let end_part = line.split("gfit ").nth(1)
            .ok_or_else(|| GggError::DataError { path: PathBuf::from("multiggg.sh"), cause: format!("Could not find 'gfit' in line {} of multiggg.sh file to split on", idx+1) })
            .into_report()?;

        let this_window = end_part.split(".ggg").next()
            .ok_or_else(|| GggError::DataError { path: PathBuf::from("multiggg.sh"), cause: format!("Could not find '.ggg' in line {} of multiggg.sh file to split on", idx+1) })
            .into_report()?;

        windows.push(this_window.trim().to_string());
    }
    
    Ok(windows)
}