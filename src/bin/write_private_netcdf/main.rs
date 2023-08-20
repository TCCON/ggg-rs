mod error;
mod logging;
mod interface;
mod sources;
mod setup;
mod dimensions;

use std::{path::{PathBuf, Path}, fmt::Debug};

use clap::Args;
use log::{error, info};

fn main() {
    let run_dir = PathBuf::from(".");

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
    let mut nc = match init_nc_file(&run_dir) {
        Ok(f) => f,
        Err(e) => return cleanup(e)
    };

    let all_sources = match setup::setup_data_sources() {
        Ok(sources) => sources,
        Err(e) => return cleanup(e)
    };

    if let Err(e) = dimensions::write_required_dimensions(&mut nc.root_mut().unwrap(), &all_sources) {
        return cleanup(e);
    }
}


#[derive(Debug, Args)]
struct WritNcCli {
    /// For the output file name, use the runlog name instead of deriving the
    /// name from the site ID and date range of the data. NOTE: this option
    /// may not be used to submit standard TCCON data to the Caltech repository.
    keep_runlog_name: bool
}

fn cleanup<E: Debug>(err: E) {
    error!("{err:?}");
    // TODO: return exit code
}

fn init_nc_file(run_dir: &Path) -> error_stack::Result<netcdf::MutableFile, netcdf::error::Error> {
    let nc_file = run_dir.join("temporary.private.nc");
    let file = netcdf::create(nc_file)?;
    Ok(file)
}