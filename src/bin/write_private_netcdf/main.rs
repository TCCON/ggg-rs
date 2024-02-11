mod error;
mod logging;
mod interface;
mod sources;
mod setup;
mod dimensions;

use std::{path::{PathBuf, Path}, fmt::Debug, process::ExitCode, ffi::OsString};

use clap::Parser;
use error_stack::ResultExt;
use log::{error, info};

use error::CliError;

fn main() -> ExitCode {
    let run_dir = PathBuf::from(".");
    let args = WriteNcCli::parse();
    
    logging::init_logging(&run_dir, log::LevelFilter::Debug);
    info!("Logging initialized");

    // Basic workflow:
    //  1. Generate the list of `DataSource` instances; this will need to be semi-dynamic (i.e. read from the multiggg file)
    //  2. Get the list of available dimensions from these instances, and ensure there are no duplicates
    //  3. Get the list of required dimensions from these instances, write the dimensions required to the netCDF file
    //     (with their position determined by the `write_at_start` property)
    //  4. Get the unique groups required by all the data sources, if writing a hierarchical file, create those groups
    //  5. For each data source, loop through the groups it requires and pass it the `GroupMut` handle for that group
    //     (flat files will always get the root group, and append the required suffix to variable names).
    let finalize_result = match driver(&run_dir, args) {
        Ok(nc_stem) => finalize(&temporary_nc_path(&run_dir), nc_stem),
        Err(e) => return cleanup(e)
    };

    if let Err(e) = finalize_result {
        error!("An error occurred while renaming the netCDF file to its final name. The file itself should still be complete. The error was: {e}");
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

fn driver(run_dir: &Path, args: WriteNcCli) -> error_stack::Result<OsString, CliError> {
    let mut nc = init_nc_file(&run_dir)
        .change_context_lazy(|| CliError::Setup)?;

    let all_sources = setup::setup_data_sources()
        .change_context_lazy(|| CliError::Setup)?;

    let runlog_name = all_sources.get_runlog_path()
        .ok_or_else(|| CliError::Unexpected("A runlog was not included in the sources"))?
        .file_stem()
        .ok_or_else(|| CliError::Unexpected("Could not get the file stem of the runlog path"))?;

    let mut nc_root = nc.root_mut()
        .ok_or_else(|| CliError::Unexpected("unable to get the root group in the output netCDF file"))?;

    dimensions::write_required_dimensions(&mut nc_root, &all_sources)
        .change_context_lazy(|| CliError::Dimension)?;


    if args.keep_runlog_name {
        Ok(runlog_name.to_os_string())
    } else {
        todo!() // get the date range of the file for the name, e.g. "pa20040701_20041231"
    }
}


#[derive(Debug, Parser)]
struct WriteNcCli {
    /// For the output file name, use the runlog name instead of deriving the
    /// name from the site ID and date range of the data. NOTE: this option
    /// may not be used to submit standard TCCON data to the Caltech repository.
    #[clap(short = 'k', long)]
    keep_runlog_name: bool,

    /// Set this flag to generate a netCDF4 file with experimental/non-standard 
    /// TCCON products placed in subgroups, rather than the root group with
    /// suffixes appended to the variable names
    #[clap(short = 'g', long)]
    hierachical_file: bool,
}

/// Finalize the netCDF file; move it from the temporary path to the final path.
fn finalize(nc_path: &Path, mut final_name_stem: OsString) -> Result<(), std::io::Error> {
    final_name_stem.push(".private.nc");
    let out_path = nc_path.with_file_name(final_name_stem);
    std::fs::rename(nc_path, out_path)
}

fn cleanup<E: Debug>(err: E) -> ExitCode {
    error!("ERROR: {err:?}");
    ExitCode::FAILURE
}

/// Create the netCDF file at the temporary location
fn init_nc_file(run_dir: &Path) -> error_stack::Result<netcdf::FileMut, netcdf::Error> {
    let nc_file = temporary_nc_path(run_dir);
    let file = netcdf::create(nc_file)?;
    Ok(file)
}

fn temporary_nc_path(run_dir: &Path) -> PathBuf {
    run_dir.join("temporary.private.nc")
}