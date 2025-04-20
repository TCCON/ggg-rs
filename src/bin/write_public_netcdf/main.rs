use std::{path::{Path, PathBuf}, process::ExitCode};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use constants::TIME_DIM_NAME;
use copying::{AuxVarCopy, CopySet, Subsetter, XgasCopy};
use error_stack::ResultExt;
use ggg_rs::{logging::init_logging, utils::nctime_to_datetime};
use itertools::Itertools;
use ndarray::Ix1;
use netcdf::{AttributeValue, Extents};

mod constants;
mod template_strings;
mod config;
mod copying;


fn main() -> ExitCode {
    let clargs = Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    
    match driver(clargs) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("write_public_netcdf did not complete successfully:\n{e:?}");
            ExitCode::FAILURE
        },
    }
}

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    let private_ds = netcdf::open(&clargs.private_nc_file)
        .change_context(CliError::OpeningPrivateFile)?;

    let time_subsetter = make_time_subsetter(&private_ds)?;
    let private_file_name = &clargs.private_nc_file;
    let public_file_name = make_public_name_from_dates(private_file_name, &time_subsetter)?;
    let mut public_ds = netcdf::create(&public_file_name)
        .change_context(CliError::OpeningPublicFile)?;

    add_time_dim(&mut public_ds, &time_subsetter)?;
    add_aux_vars(&private_ds, &mut public_ds, &time_subsetter)?;
    add_xgas_vars(&private_ds, &mut public_ds, &time_subsetter)?;
    Ok(())
}

#[derive(Debug, clap::Parser)]
struct Cli {
    private_nc_file: PathBuf,
    // config_file: Option<PathBuf>,
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("An error occurred while opening the private file")]
    OpeningPrivateFile,
    #[error("An error occurred while opening the public file for writing")]
    OpeningPublicFile,
    #[error("An error occurred while finding flag == 0 data")]
    FindingFlag0,
    #[error("An error occurred while determining what to name the public file")]
    MakePubName,
    #[error("An error occurred while writing dimensions to the public file")]
    WritingDim,
    #[error("An error occurred while writing the auxiliary variables to the public file")]
    WritingAux,
    #[error("An error occurred while writing the Xgas and related variables to the public file")]
    WritingXgas,
    #[error("{0}")]
    Custom(&'static str),
}

fn make_time_subsetter(private_ds: &netcdf::File) -> error_stack::Result<Subsetter, CliError> {
    let flags = private_ds.variable("flag")
        .ok_or_else(|| netcdf::Error::NotFound("variable 'flag'".to_string()))
        .change_context(CliError::FindingFlag0)?
        .get::<i32, _>(Extents::All)
        .change_context(CliError::FindingFlag0)?
        .into_dimensionality::<Ix1>()
        .change_context(CliError::FindingFlag0)?;
    let subsetter = Subsetter::from_flag(flags.view());
    Ok(subsetter)
}

fn make_public_name_from_dates(private_filename: &Path, time_subsetter: &Subsetter) -> error_stack::Result<PathBuf, CliError> {
    // Load the times, subset them, and find the first and last times.
    // Yeah, this is painfully long for what should be simple...
    let ds = netcdf::open(private_filename)
        .change_context(CliError::MakePubName)?;
    let time_var = ds.variable("time")
        .ok_or_else(|| netcdf::Error::NotFound("variable 'time'".to_string()))
        .change_context(CliError::MakePubName)?;
    let times = time_var
        .get::<f64, _>(Extents::All)
        .change_context(CliError::MakePubName)?;
    
    let time_units = time_var.attribute("units")
        .ok_or_else(|| netcdf::Error::NotFound("attribute 'units' on variable 'time'".to_string()))
        .change_context(CliError::MakePubName)?
        .value()
        .change_context(CliError::MakePubName)?;
    let time_units = if let AttributeValue::Str(u) = time_units {
        u
    } else {
        return Err(CliError::Custom("'units' attribute on 'time' variable is not a string").into())
    };

    let times = time_subsetter.subset_nd_array(times.view(), 0)
        .change_context(CliError::MakePubName)?;
    let (first_time, last_time) = match times.iter().minmax() {
        itertools::MinMaxResult::NoElements => {
            let error_msg = "Could not determine times for file name, no times left after subsetting for flag == 0";
            return Err(CliError::Custom(error_msg).into())
        },
        itertools::MinMaxResult::OneElement(&t) => (t, t),
        itertools::MinMaxResult::MinMax(&ta, &tb) => (ta, tb),
    };
    let first_time = nctime_to_datetime(first_time, &time_units)
        .change_context(CliError::MakePubName)?;
    let last_time = nctime_to_datetime(last_time, &time_units)
        .change_context(CliError::MakePubName)?;

    // Get the site ID, current file extension, and parent directory
    let private_stem = private_filename.file_name()
        .ok_or_else(|| CliError::Custom("private file name does not have a basename!"))?
        .to_string_lossy();

    let site_id: String = private_stem.chars().take(2).collect();
    let public_ext = private_stem.split_once('.')
        .map(|(_, ext)| ext)
        .unwrap_or("public.nc");
    let parent_dir = private_filename.parent()
        .ok_or_else(|| CliError::Custom("could not get parent directory of the private file"))?;

    // Finally, construct the dang name
    let public_file_name = format!("{site_id}{}_{}.{public_ext}", first_time.format("%Y%m%d"), last_time.format("%Y%m%d"));
    Ok(parent_dir.join(public_file_name))
}

fn add_time_dim(public_ds: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CliError> {
    let ntime = time_subsetter.len();
    public_ds.add_dimension(TIME_DIM_NAME, ntime)
        .change_context(CliError::WritingDim)?;
    Ok(())
}

fn add_aux_vars(private_ds: &netcdf::File, public_ds: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CliError> {
    let aux_vars= config::default_aux_vars();

    for var in aux_vars {
        var.copy(private_ds, public_ds, time_subsetter)
            .change_context(CliError::WritingAux)?;
    }

    Ok(())
}

fn add_xgas_vars(private_ds: &netcdf::File, public_ds: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CliError> {
    // TODO: discover the Xgas variables. This is just a quick verification
    let xgas_vars: Vec<XgasCopy<f32>> = vec![
        XgasCopy::new("xch4", "ch4", "methane"),
        XgasCopy::new("xco", "co", "carbon monoxide"),
        XgasCopy::new("xn2o", "n2o", "nitrous oxide"),
    ];

    for var in xgas_vars {
        var.copy(private_ds, public_ds, time_subsetter)
            .change_context(CliError::WritingXgas)?;
    }

    Ok(())
}
