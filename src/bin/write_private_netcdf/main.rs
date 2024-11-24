use std::{collections::HashMap, ffi::OsString, path::{Path, PathBuf}, process::ExitCode, sync::Arc};

use error_stack::ResultExt;
use errors::CliError;
use interface::{DataProvider, StdGroupWriter};
use log::info;
use providers::RunlogProvider;
use rayon::iter::{IntoParallelIterator, ParallelIterator};


mod logging;
mod errors;
mod interface;
mod setup;
mod dimensions;
mod providers;

fn main() -> ExitCode {
    let run_dir = PathBuf::from(".");
    logging::init_logging(&run_dir, log::LevelFilter::Debug);
    info!("Logging initialized");

    match driver(run_dir) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            log::error!("{e:?}");
            eprintln!("\nThe netCDF writer failed:\n\n{e:?}\n");
            eprintln!("{}", e.current_context().user_message());
            ExitCode::FAILURE
        },
    }
}

fn driver(run_dir: PathBuf) -> error_stack::Result<(), CliError> {
    let file_paths = setup::InputFiles::from_run_dir(&run_dir)?;
    let runlog_name = file_paths.runlog.file_stem()
        .ok_or_else(|| CliError::input_error(format!(
            "runlog path ({}) does not include a file name", file_paths.runlog.display()
        )))?
        .to_os_string();

    let (runlog, spec_indexer) = RunlogProvider::new(file_paths.runlog)
        .change_context_lazy(|| CliError::input_error("error occurred while reading the runlog"))?;
    let spec_indexer = Arc::new(spec_indexer);
    let providers: Vec<Box<dyn DataProvider>> = vec![
        Box::new(runlog)
    ];

    // Initialize the temporary netCDF file with a name that clearly indicates it is not complete.
    let mut nc_dset = init_nc_file(&run_dir)
        .change_context_lazy(|| CliError::runtime_error("error occurred while initializing netCDF file"))?;

    // Create all dimensions first
    let mut known_dims = vec![];
    for provider in providers.iter() {
        let provided_dimensions = provider.dimension_lengths();
        for (dimname, dimlength) in provided_dimensions.iter() {
            nc_dset.add_dimension(&dimname, *dimlength)
                .change_context_lazy(|| CliError::runtime_error(
                    format!("error occurred while creating the '{dimname}' dimension")
                ))?;
            known_dims.push(dimname.to_string());
        }
    }

    // Check that all the dimensions we need were written
    report_missing_dimensions(&known_dims, &providers)?;

    // Actually write the variables to the netCDF file.
    // Do so in an inner scope so that `writer` is dropped and our netCDF file is closed. 
    // TODO: allow users to limit the number of processes used.
    let res = {
        let writer = StdGroupWriter::new(nc_dset, false);
        providers.into_par_iter().try_for_each(|provider| {
            let local_writer = writer.clone();
            let local_indexer = Arc::clone(&spec_indexer);
            provider.write_data_to_nc(&local_indexer, &local_writer)
        })
    };

    if let Err(e) = &res {
        let new_context = match e.current_context() {
            errors::WriteError::Netcdf(_) => CliError::runtime_error("a netCDF error occurred"),
            errors::WriteError::VarCreation(_) => CliError::internal_error("the netCDF writer tried to construct a variable incorrectly"),
        };
        return res.change_context(new_context);
    }

    let curr_nc_path = temporary_nc_path(&run_dir);
    // TODO: compute the file name from the times by default.
    finalize_nc_file(&curr_nc_path, runlog_name)?;

    Ok(())
}

/// Create the netCDF file at the temporary location
fn init_nc_file(run_dir: &Path) -> error_stack::Result<netcdf::FileMut, netcdf::Error> {
    let nc_file = temporary_nc_path(run_dir);
    let mut file = netcdf::create(nc_file)?;
    file.add_attribute("writing_was_completed", 0)?;
    Ok(file)
}

fn finalize_nc_file(nc_path: &Path, mut final_name_stem: OsString) -> error_stack::Result<(), CliError> {
    // Does this work? If not, I don't see a way to edit attributes, which is weird.
    // In that case, we'll have to just not add this attribute until writing is completed,
    // it's absence will indicate failure.
    let mut nc_dset = netcdf::append(nc_path)
        .change_context_lazy(|| CliError::runtime_error("failed to reopen netCDF file for finalization"))?;
    nc_dset.add_attribute("writing_was_completed", 1)
        .change_context_lazy(|| CliError::runtime_error("failed to update 'writing_was_completed' attribute during file finalization"))?;
    nc_dset.close()
        .change_context_lazy(|| CliError::runtime_error("failed to close completed netCDF file"))?;
    final_name_stem.push(".private.nc");
    let out_path = nc_path.with_file_name(final_name_stem);
    std::fs::rename(nc_path, out_path)
        .change_context_lazy(|| CliError::runtime_error("failed to rename netCDF file during finalization"))

}

fn temporary_nc_path(run_dir: &Path) -> PathBuf {
    run_dir.join("temporary.private.nc")
}

fn report_missing_dimensions(known_dimensions: &[String], providers: &[Box<dyn DataProvider>]) -> Result<(), CliError> {
    let mut missing_dims: HashMap<&str, String> = HashMap::new();
    for provider in providers.iter() {
        for &req_dim in provider.dimensions_required().iter() {
            if !known_dimensions.iter().any(|known_dim| known_dim == req_dim) {
                if let Some(needed_by) = missing_dims.get_mut(req_dim) {
                    needed_by.push_str(", ");
                    needed_by.push_str(&provider.to_string());
                } else {
                    missing_dims.insert(req_dim, provider.to_string());
                }
            }
        }
    }

    if missing_dims.is_empty() {
        Ok(())
    } else {
        let mut msg = "The following dimension(s) were not created in the netCDF file:".to_string();
        for (dimname, req_providers) in missing_dims.into_iter() {
            msg.push_str(&format!(
                "\n- {dimname} (needed by the {req_providers})"
            ));
        }
        Err(CliError::internal_error(msg))
    }
}