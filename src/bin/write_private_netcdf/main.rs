mod error;
mod logging;
mod interface;
mod sources;
mod setup;
mod dimensions;

use std::{path::{PathBuf, Path}, fmt::Debug, process::ExitCode};

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
    if let Err(e) = driver(&run_dir, args) {
        cleanup(e)
    } else {
        ExitCode::SUCCESS
    }
}

fn driver(run_dir: &Path, args: WriteNcCli) -> error_stack::Result<(), CliError> {
    let mut nc = init_nc_file(&run_dir)
        .change_context_lazy(|| CliError::Setup)?;

    let all_sources = setup::setup_data_sources()
        .change_context_lazy(|| CliError::Setup)?;

    let mut nc_root = nc.root_mut()
        .ok_or_else(|| CliError::Unexpected("unable to get the root group in the output netCDF file"))?;

    dimensions::write_required_dimensions(&mut nc_root, &all_sources)
        .change_context_lazy(|| CliError::Dimension)?;

    Ok(())
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

fn cleanup<E: Debug>(err: E) -> ExitCode {
    error!("ERROR: {err:?}");
    ExitCode::FAILURE
}

fn init_nc_file(run_dir: &Path) -> error_stack::Result<netcdf::MutableFile, netcdf::error::Error> {
    let nc_file = run_dir.join("temporary.private.nc");
    let file = netcdf::create(nc_file)?;
    Ok(file)
}