use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use chrono::NaiveDate;
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use config::{Config, ConfigError, EXTENDED_TCCON_TOML, STANDARD_TCCON_TOML};
use constants::TIME_DIM_NAME;
use copying::{AuxVarCopy, ComputedVariable, CopySet, Subsetter, XgasCopy};
use discovery::discover_xgas_vars;
use error_stack::ResultExt;
use ggg_rs::{logging::init_logging, nc_utils, utils::nctime_to_datetime};
use itertools::Itertools;
use ndarray::Ix1;
use netcdf::{AttributeValue, Extents};

mod config;
mod constants;
mod copying;
mod discovery;
mod template_strings;

// Todos:
//   1. Traceability scale [x]
//   2. GEOS source summary [x]
//   3. Xgas discovery [x]
//   4. Standard [x] and experimental [ ] configs
//      4a. Create unit tests that parse the book TOML examples and try deserializing them [x]
//      4b. Add ability to specify Xgas and Xgas error public name [x - tentative]
//      4c. Add rename option to Xgas discovery [x - tentative]
//      4d. Make a subset of the ancillary variable specs allowed for Xgas discovery, and From<THAT> for the regular ancillary spec [x - tentative]
//      4e. Use https://docs.rs/figment/latest/figment/ to handle merging configurations.
//      4f. Make the inferred AK names include suffixes or however we choose to distinguish the mid-IR gases' AKs
//   5. Data latency
//   6. Global attributes

fn main() -> ExitCode {
    let clargs = Cli::parse();

    init_logging(clargs.verbosity.log_level_filter());

    match driver(clargs) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("write_public_netcdf did not complete successfully:\n{e:?}");
            ExitCode::FAILURE
        }
    }
}

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    let config = load_config(clargs.extended, clargs.config.clone())
        .change_context(CliError::ReadingConfig)?;

    if clargs.check_config_only {
        println!("Loaded configuration:\n{config:#?}");
        return Ok(());
    }

    // TODO: time subsetter needs to account for data latency
    let opt_end_date = clargs.get_release_lag_date()?;

    let private_nc_file = clargs
        .private_nc_file
        .expect("If --check-config-only not given, a private netCDF file must be given");
    let private_ds = netcdf::open(&private_nc_file).change_context(CliError::OpeningPrivateFile)?;

    let time_subsetter = make_time_subsetter(&private_ds, opt_end_date)?;
    let private_file_name = &private_nc_file;
    let public_file_name = if clargs.no_rename_by_dates {
        make_public_name_from_stem(private_file_name)?
    } else {
        make_public_name_from_dates(private_file_name, &time_subsetter)?
    };
    log::info!("Will write to {}", public_file_name.display());
    let mut public_ds =
        netcdf::create(&public_file_name).change_context(CliError::OpeningPublicFile)?;

    add_time_dim(&mut public_ds, &time_subsetter)?;
    add_aux_vars(&config, &private_ds, &mut public_ds, &time_subsetter)?;
    add_computed_vars(&config, &private_ds, &mut public_ds, &time_subsetter)?;
    add_xgas_vars(&config, &private_ds, &mut public_ds, &time_subsetter)?;
    Ok(())
}

#[derive(Debug, clap::Parser)]
struct Cli {
    /// The private netCDF file to copy.
    #[clap(required_unless_present("check_config_only"))]
    private_nc_file: Option<PathBuf>,

    /// Run using the default configuration for the extended
    /// TCCON public files, which include Xgas values from the
    /// secondary detector if available.
    #[clap(long, group = "configuration")]
    extended: bool,

    /// Run using a custom configuration.
    #[clap(long, group = "configuration")]
    config: Option<PathBuf>,

    /// Do not rename the output file to match the time span of the
    /// data retained after flagging and data latency.
    #[clap(long)]
    no_rename_by_dates: bool,

    /// Will attempt to parse the selected configuration and print
    /// a debugging representation to stdout, then stop without
    /// creating a netCDF file.
    #[clap(long)]
    check_config_only: bool,

    /// Specify a number of days back in time from today to withhold
    /// data from the public files. For example, if run with
    /// --data-latency-days=30 on 31 Jan 2025, then no data after midnight,
    /// 1 Jan 2025 will be included. Mutually exclusive with --data-latency-date
    /// and --data-latency-file.
    #[clap(long, group = "data_latency")]
    data_latency_days: Option<u32>,

    /// Specify a date after which data will not be included. Argument must be in
    /// YYYY-MM-DD format. Note that this will use midnight UTC as the cutoff time.
    /// Mutually exclusive with --data-latency-days and --data-latency-file.
    #[clap(long, group = "data_latency")]
    data_latency_date: Option<NaiveDate>,

    /// Specify a TOML file which includes metadata for each site from which to
    /// take the data latency. The data latency will be based on the "release_lag"
    /// key for the entry in the file that corresponds to the first two letters of
    /// the private netCDF file name.
    #[clap(long, group = "data_latency")]
    data_latency_file: Option<PathBuf>,

    // config_file: Option<PathBuf>,
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

impl Cli {
    fn get_release_lag_date(&self) -> error_stack::Result<Option<NaiveDate>, CliError> {
        // First, double check that we didn't get multiple arguments for this.
        // This will be a panic, rather than an error, since clap treats incorrect
        // CLI configuration as a panic - this way we are consistent. This shouldn't
        // happen anyway, as these arguments should all be in a mutually exclusive
        // group, so clap should not let more than one be specified.
        let mut nargs = 0;
        if self.data_latency_date.is_some() {
            nargs += 1;
        }
        if self.data_latency_days.is_some() {
            nargs += 1;
        }
        if self.data_latency_file.is_some() {
            nargs += 1;
        }

        if nargs > 1 {
            panic!("Multiple mutually exclusive --data-latency-* arguments were given")
        }

        // Now that we know at most one argument is present, it doesn't
        // matter what order we handle them in.
        if let Some(date) = self.data_latency_date {
            return Ok(Some(date));
        }

        // Either of the last two options need today's date, so go ahead and get that now.
        let today = chrono::Utc::now().date_naive();
        let ndays = if let Some(days) = self.data_latency_days {
            days
        } else if self.data_latency_file.is_some() {
            self.get_latency_from_metadata()?
        } else {
            return Ok(None);
        };

        let date = today - chrono::Days::new(ndays as u64);
        log::debug!("Calculated data end date {date} from latency number of days {ndays}");
        return Ok(Some(date));
    }

    fn get_latency_from_metadata(&self) -> error_stack::Result<u32, CliError> {
        let dlf = self.data_latency_file.as_deref().expect(
            "get_site_metadata should not be called if --data-latency-file was not provided",
        );

        let metadata =
            nc_utils::read_nc_site_metadata(dlf).change_context(CliError::SiteMetadata)?;

        let private_nc_filename = self.private_nc_file
            .as_deref()
            .expect("get_site_metadata should not be called if no private netCDF file is given as an argument")
            .file_name()
            .ok_or_else(|| CliError::custom("Could not get the file base name of the private netCDF file"))?
            .to_string_lossy();

        let site_id: String = private_nc_filename.chars().take(2).collect();

        let site_meta = metadata.get(&site_id).ok_or_else(|| {
            CliError::custom(format!(
                "No site metadata found for site '{site_id}' in site metadata file {}",
                dlf.display()
            ))
        })?;

        Ok(site_meta.release_lag)
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("An error occurred while reading the configuration")]
    ReadingConfig,
    #[error("An error occurred while getting the site metadata")]
    SiteMetadata,
    #[error("An error occurred while opening the private file")]
    OpeningPrivateFile,
    #[error("An error occurred while opening the public file for writing")]
    OpeningPublicFile,
    #[error("An error occurred while subsetting data")]
    Subsetting,
    #[error("An error occurred while determining what to name the public file")]
    MakePubName,
    #[error("An error occurred while writing dimensions to the public file")]
    WritingDim,
    #[error("An error occurred while writing the auxiliary variables to the public file")]
    WritingAux,
    #[error("An error occurred while writing the Xgas and related variables to the public file")]
    WritingXgas,
    #[error("An error occurred while writing the computed variables to the public file")]
    WritingComputed,
    #[error("{0}")]
    Custom(String),
}

impl CliError {
    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

fn load_config(extended: bool, custom_file: Option<PathBuf>) -> Result<Config, ConfigError> {
    match (extended, custom_file) {
        (true, None) => Config::from_toml_str(EXTENDED_TCCON_TOML),
        (true, Some(_)) => panic!(
            "invalid combination of arguments: --extended and --config cannot be used together"
        ),
        (false, None) => Config::from_toml_str(STANDARD_TCCON_TOML),
        (false, Some(p)) => Config::from_toml_file(p),
    }
}

fn make_time_subsetter(
    private_ds: &netcdf::File,
    opt_end_date: Option<NaiveDate>,
) -> error_stack::Result<Subsetter, CliError> {
    let flags = private_ds
        .variable("flag")
        .ok_or_else(|| netcdf::Error::NotFound("variable 'flag'".to_string()))
        .change_context(CliError::Subsetting)?
        .get::<i32, _>(Extents::All)
        .change_context(CliError::Subsetting)?
        .into_dimensionality::<Ix1>()
        .change_context(CliError::Subsetting)?;
    let mut subsetter = Subsetter::from_flag(flags.view());
    if let Some(end_date) = opt_end_date {
        let nc_times = private_ds
            .variable("time")
            .ok_or_else(|| netcdf::Error::NotFound("variable 'time'".to_string()))
            .change_context(CliError::Subsetting)?
            .get::<f64, _>(Extents::All)
            .change_context(CliError::Subsetting)?
            .into_dimensionality::<Ix1>()
            .change_context(CliError::Subsetting)?;
        log::debug!("Subsetting to observations before {end_date}");
        subsetter.add_cutoff_date(nc_times.view(), end_date);
    }
    Ok(subsetter)
}

fn make_public_name_from_dates(
    private_filename: &Path,
    time_subsetter: &Subsetter,
) -> error_stack::Result<PathBuf, CliError> {
    // Load the times, subset them, and find the first and last times.
    // Yeah, this is painfully long for what should be simple...
    let ds = netcdf::open(private_filename).change_context(CliError::MakePubName)?;
    let time_var = ds
        .variable("time")
        .ok_or_else(|| netcdf::Error::NotFound("variable 'time'".to_string()))
        .change_context(CliError::MakePubName)?;
    let times = time_var
        .get::<f64, _>(Extents::All)
        .change_context(CliError::MakePubName)?;

    let time_units = time_var
        .attribute("units")
        .ok_or_else(|| netcdf::Error::NotFound("attribute 'units' on variable 'time'".to_string()))
        .change_context(CliError::MakePubName)?
        .value()
        .change_context(CliError::MakePubName)?;
    let time_units = if let AttributeValue::Str(u) = time_units {
        u
    } else {
        return Err(
            CliError::custom("'units' attribute on 'time' variable is not a string").into(),
        );
    };

    let times = time_subsetter
        .subset_nd_array(times.view(), 0)
        .change_context(CliError::MakePubName)?;
    let (first_time, last_time) = match times.iter().minmax() {
        itertools::MinMaxResult::NoElements => {
            let error_msg = "Could not determine times for file name, no times left after subsetting for flag == 0";
            return Err(CliError::custom(error_msg).into());
        }
        itertools::MinMaxResult::OneElement(&t) => (t, t),
        itertools::MinMaxResult::MinMax(&ta, &tb) => (ta, tb),
    };
    let first_time =
        nctime_to_datetime(first_time, &time_units).change_context(CliError::MakePubName)?;
    let last_time =
        nctime_to_datetime(last_time, &time_units).change_context(CliError::MakePubName)?;

    // Get the site ID, current file extension, and parent directory
    let private_base_name = private_filename
        .file_name()
        .ok_or_else(|| CliError::custom("private file name does not have a basename!"))?
        .to_string_lossy();

    let site_id: String = private_base_name.chars().take(2).collect();
    let public_ext = private_base_name
        .split_once('.')
        .map(|(_, ext)| ext.replace("private", "public"))
        .unwrap_or_else(|| "public.nc".to_string());
    let parent_dir = private_filename
        .parent()
        .ok_or_else(|| CliError::custom("could not get parent directory of the private file"))?;

    // Finally, construct the dang name
    let public_filename = format!(
        "{site_id}{}_{}.{public_ext}",
        first_time.format("%Y%m%d"),
        last_time.format("%Y%m%d")
    );
    Ok(parent_dir.join(public_filename))
}

fn make_public_name_from_stem(private_filename: &Path) -> error_stack::Result<PathBuf, CliError> {
    let base_name = private_filename
        .file_name()
        .ok_or_else(|| CliError::custom("private file name does not have a basename!"))?
        .to_string_lossy();

    let public_filename = if let Some((stem, ext)) = base_name.split_once('.') {
        let public_ext = ext.replace("private", "public");
        format!("{stem}.{public_ext}")
    } else {
        format!("{base_name}.public.nc")
    };

    let parent_dir = private_filename
        .parent()
        .ok_or_else(|| CliError::custom("could not get parent directory of the private file"))?;
    Ok(parent_dir.join(public_filename))
}

fn add_time_dim(
    public_ds: &mut netcdf::FileMut,
    time_subsetter: &Subsetter,
) -> error_stack::Result<(), CliError> {
    let ntime = time_subsetter.len();
    public_ds
        .add_dimension(TIME_DIM_NAME, ntime)
        .change_context(CliError::WritingDim)?;
    Ok(())
}

fn add_aux_vars(
    config: &Config,
    private_ds: &netcdf::File,
    public_ds: &mut netcdf::FileMut,
    time_subsetter: &Subsetter,
) -> error_stack::Result<(), CliError> {
    for var in config.aux.iter() {
        var.copy(private_ds, public_ds, time_subsetter)
            .change_context(CliError::WritingAux)?;
    }

    Ok(())
}

fn add_xgas_vars(
    config: &Config,
    private_ds: &netcdf::File,
    public_ds: &mut netcdf::FileMut,
    time_subsetter: &Subsetter,
) -> error_stack::Result<(), CliError> {
    let defined_xgases = &config.xgas;
    let discovered_xgases = discover_xgas_vars(
        &defined_xgases,
        &config.discovery.rule,
        &config.discovery.excluded_gases,
        &config.discovery.excluded_xgas_variables,
        &config.gas_long_names,
        private_ds,
    )
    .change_context(CliError::WritingXgas)?;

    let it = defined_xgases.iter().chain(discovered_xgases.iter());

    for var in it {
        log::trace!("Xgas variable: {var:?}");
        var.copy(private_ds, public_ds, time_subsetter)
            .change_context(CliError::WritingXgas)?;
    }

    Ok(())
}

fn add_computed_vars(
    config: &Config,
    private_ds: &netcdf::File,
    public_ds: &mut netcdf::FileMut,
    time_subsetter: &Subsetter,
) -> error_stack::Result<(), CliError> {
    for var in config.computed.iter() {
        var.copy(private_ds, public_ds, time_subsetter)
            .change_context(CliError::WritingComputed)?;
    }

    Ok(())
}
