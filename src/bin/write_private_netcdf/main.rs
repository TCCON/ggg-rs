mod error;
mod logging;
mod interface;
mod sources;
mod setup;

use std::path::PathBuf;

use clap::Args;
use log::info;

fn main() {
    logging::init_logging(&PathBuf::from("."), log::LevelFilter::Debug);
    info!("Logging initialized");

    // Basic workflow:
    //  1. Generate the list of `DataSource` instances; this will need to be semi-dynamic (i.e. read from the multiggg file)
    //  2. Get the list of available dimensions from these instances, and ensure there are no duplicates
    //  3. Get the list of required dimensions from these instances, write the dimensions required to the netCDF file
    //     (with their position determined by the `write_at_start` property)
    //  4. Get the unique groups required by all the data sources, if writing a hierarchical file, create those groups
    //  5. For each data source, loop through the groups it requires and pass it the `GroupMut` handle for that group
    //     (flat files will always get the root group, and append the required suffix to variable names).
    let all_sources = setup::setup_data_sources().expect("An error occurred while building the list of data sources");
}